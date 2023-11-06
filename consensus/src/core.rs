use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::filter::FilterInput;
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, RandomCoin, RandomnessShare, SPBProof, SPBValue, SPBVote, Vote, QC};
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use store::Store;
use threshold_crypto::PublicKeySet;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};
#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type SeqNumber = u64; // For both round and view

const OPT: u8 = 0;
const PES: u8 = 1;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    HsPropose(Block),
    HSVote(Vote),
    RandomnessShare(RandomnessShare),
    RandomCoin(RandomCoin),
    LoopBack(Block),
    SyncRequest(Digest, PublicKey),
    SyncReply(Block),
    SPBPropose(SPBValue, SPBProof),
    SPBVote(SPBVote),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    pk_set: PublicKeySet,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    core_channel: Receiver<ConsensusMessage>,
    network_filter: Sender<FilterInput>,
    commit_channel: Sender<Block>,
    round: SeqNumber, // current round
    epoch: SeqNumber, // current round
    last_voted_round: SeqNumber,
    last_committed_round: SeqNumber,
    high_qc: QC,
    aggregator: Aggregator,
    opt_path: bool,
    pes_path: bool,
    smvba_pending_blocks: HashMap<SeqNumber, HashMap<PublicKey, Block>>, // buffering smvba's propose
    spb_vote_sender: HashMap<SeqNumber, [HashSet<PublicKey>; 2]>, // set of nodes that send spb vote
    spb_vote_weight: HashMap<SeqNumber, [Stake; 2]>,              // weight of the above nodes
    spb_votes: HashMap<SeqNumber, [Vec<SPBVote>; 2]>,             // set of vote
    randomness_share_sender: HashMap<SeqNumber, HashSet<PublicKey>>, // set of nodes that send randomness share
    randomness_share_weight: HashMap<SeqNumber, Stake>,              // weight of the above nodes
    randomness_shares: HashMap<SeqNumber, Vec<RandomnessShare>>,     // set of randomness share
    random_coin: HashMap<SeqNumber, RandomCoin>,                     // random coin of each fallback
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        pk_set: PublicKeySet,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        core_channel: Receiver<ConsensusMessage>,
        network_filter: Sender<FilterInput>,
        commit_channel: Sender<Block>,
        opt_path: bool,
        pes_path: bool,
    ) -> Self {
        let aggregator = Aggregator::new(committee.clone());
        Self {
            name,
            committee,
            parameters,
            signature_service,
            store,
            pk_set,
            leader_elector,
            mempool_driver,
            synchronizer,
            network_filter,
            commit_channel,
            core_channel,
            round: 1,
            epoch: 0,
            last_voted_round: 0,
            last_committed_round: 0,
            high_qc: QC::genesis(),
            aggregator,
            opt_path,
            pes_path,
            smvba_pending_blocks: HashMap::new(),
            spb_vote_sender: HashMap::new(),
            spb_vote_weight: HashMap::new(),
            spb_votes: HashMap::new(),
            randomness_share_sender: HashMap::new(),
            randomness_share_weight: HashMap::new(),
            randomness_shares: HashMap::new(),
            random_coin: HashMap::new(),
        }
    }

    //初试化一个 epoch
    fn epoch_init(&mut self, epoch: u64) {
        self.round = 1;
        self.epoch = epoch;
        self.high_qc = QC::genesis();
        self.last_voted_round = 0;
        self.last_committed_round = 0;
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    // -- Start Safety Module --
    fn increase_last_voted_round(&mut self, target: SeqNumber) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.round > self.last_voted_round;
        let safety_rule_2 = block.qc.round + 1 == block.round;

        // if let Some(ref tc) = block.tc {
        //     let mut can_extend = tc.seq + 1 == block.round;
        //     can_extend &= block.qc.round >= *tc.high_qc_rounds().iter().max().expect("Empty TC");
        //     safety_rule_2 |= can_extend;
        // }

        if !(safety_rule_1 && safety_rule_2) {
            return None;
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.round);
        // TODO [issue #15]: Write to storage preferred_round and last_voted_round.
        Some(Vote::new(&block, self.name, self.signature_service.clone()).await)
    }

    async fn commit(&mut self, block: Block) -> ConsensusResult<()> {
        if self.last_committed_round >= block.round {
            return Ok(());
        }

        let mut to_commit = VecDeque::new();
        to_commit.push_back(block.clone());

        // Ensure we commit the entire chain. This is needed after view-change.
        let mut parent = block.clone();
        while self.last_committed_round + 1 < parent.round {
            let ancestor = self
                .synchronizer
                .get_parent_block(&parent)
                .await?
                .expect("We should have all the ancestors by now");
            to_commit.push_front(ancestor.clone());
            parent = ancestor;
        }

        // Save the last committed block.
        self.last_committed_round = block.round;

        // Send all the newly committed blocks to the node's application layer.
        while let Some(block) = to_commit.pop_back() {
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed B{}({})", block.round, base64::encode(x));
                }
            }
            debug!("Committed {:?}", block);
            if let Err(e) = self.commit_channel.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
        Ok(())
    }
    // -- End Safety Module --

    // -- Start Pacemaker --
    fn update_high_qc(&mut self, qc: &QC) {
        if qc.round > self.high_qc.round {
            self.high_qc = qc.clone();
        }
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_hs_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            // Process the QC.
            self.process_qc(&qc).await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round) {
                self.generate_proposal(OPT).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, round: SeqNumber) {
        if round < self.round {
            return;
        }
        // Reset the timer and advance round.
        self.round = round + 1;
        debug!("Moved to round {}", self.round);

        // Cleanup the vote aggregator.
        self.aggregator.cleanup_hs(&self.round);
    }
    // -- End Pacemaker --

    #[async_recursion]
    async fn generate_proposal(&mut self, path: u8) -> ConsensusResult<()> {
        // Make a new block.
        let payload = self
            .mempool_driver
            .get(self.parameters.max_payload_size)
            .await;
        let block = Block::new(
            self.high_qc.clone(),
            self.name,
            self.round,
            self.epoch,
            payload,
            self.signature_service.clone(),
            path,
        )
        .await;
        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created B{}({})", block.round, base64::encode(x));
            }
        }
        debug!("Created {:?}", block);

        if path == OPT {
            // Process our new block and broadcast it.
            let message = ConsensusMessage::HsPropose(block.clone());
            Synchronizer::transmit(
                message,
                &self.name,
                None,
                &self.network_filter,
                &self.committee,
            )
            .await?;
            self.process_block(&block).await?;

            // Wait for the minimum block delay.
            sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        } else if path == PES {
        }
        Ok(())
    }

    async fn process_qc(&mut self, qc: &QC) {
        self.advance_round(qc.round).await;
        self.update_high_qc(qc);
    }

    // #[async_recursion]
    // async fn print_chain(&mut self, block: &Block) -> ConsensusResult<()> {
    //     debug!("-------------------------------------------------------- printing chain start --------------------------------------------------------");
    //     let mut current_block = block.clone();
    //     while current_block.qc != QC::genesis() {
    //         let parent = match self.synchronizer.get_previous_block(&current_block).await? {
    //             Some(b) => b,
    //             None => {
    //                 debug!("Processing of {} suspended: missing parent", current_block.digest());
    //                 break;
    //             }
    //         };
    //         debug!("{:?}", current_block);
    //         current_block = parent;
    //     }
    //     debug!("{:?}", current_block);
    //     debug!("-------------------------------------------------------- printing chain end --------------------------------------------------------");
    //     Ok(())
    // }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process both ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        // Check if we can commit the head of the 2-chain.
        // Note that we commit blocks only if we have all its ancestors.
        if b0.round + 1 == b1.round {
            self.commit(b0.clone()).await?;
        }

        // Cleanup the mempool.
        self.mempool_driver.cleanup(&b0, &b1, &block).await;

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.round != self.round {
            return Ok(());
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_vote(block).await {
            debug!("Created {:?}", vote);
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                let message = ConsensusMessage::HSVote(vote);
                Synchronizer::transmit(
                    message,
                    &self.name,
                    Some(&next_leader),
                    &self.network_filter,
                    &self.committee,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone()).await? {
            debug!("Processing of {} suspended: missing payload", digest);
            return Ok(());
        }

        // All check pass, we can process this block.
        self.process_block(block).await
    }

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = ConsensusMessage::SyncReply(block);
            Synchronizer::transmit(
                message,
                &self.name,
                Some(&sender),
                &self.network_filter,
                &self.committee,
            )
            .await?;
        }
        Ok(())
    }

    async fn handle_rs(&mut self, _: RandomnessShare) -> ConsensusResult<()> {
        Ok(())
    }

    async fn handle_rc(&mut self, _: RandomCoin) -> ConsensusResult<()> {
        Ok(())
    }

    async fn handle_spb_proposal(
        &mut self,
        value: SPBValue,
        proof: SPBProof,
    ) -> ConsensusResult<()> {
        //是否同一阶段
        ensure!(
            value.phase() == proof.phase,
            ConsensusError::SPBPhaseWrong(value.phase(), proof.phase)
        );

        //验证Proof是否正确
        proof.verify(&self.committee, &self.pk_set)?;

        Ok(())
    }

    async fn handle_spb_vote(&mut self, spb_vote: SPBVote) -> ConsensusResult<()> {
        self.aggregator.add_spb_vote(spb_vote)?;
        Ok(())
    }

    pub async fn run_epoch(&mut self) {
        let mut epoch = 0u64;
        loop {
            self.epoch_init(epoch);
            self.run().await; //运行当前epoch
            epoch += 1;
        }
    }

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        if self.opt_path && self.name == self.leader_elector.get_leader(self.round) {
            //如果是leader就发送propose
            self.generate_proposal(OPT)
                .await
                .expect("Failed to send the first block");
        }
        //如果启动了悲观路劲
        if self.pes_path {}

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        ConsensusMessage::HsPropose(block) => self.handle_proposal(&block).await,
                        ConsensusMessage::HSVote(vote) => self.handle_vote(&vote).await,
                        ConsensusMessage::RandomnessShare(rs) => self.handle_rs(rs).await,
                        ConsensusMessage::RandomCoin(rc) => self.handle_rc(rc).await,
                        ConsensusMessage::LoopBack(block) => self.process_block(&block).await,
                        ConsensusMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await,
                        ConsensusMessage::SyncReply(block) => self.handle_proposal(&block).await,
                        ConsensusMessage::SPBPropose(value,proof)=> self.handle_spb_proposal(value,proof).await,
                        ConsensusMessage::SPBVote(vote)=> self.handle_spb_vote(vote).await,
                    }
                },
                else => break,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(ConsensusError::PESOutput) => return, //ABA输出1 直接退出当前epoch
                Err(e) => warn!("{}", e),
            }
        }
    }
}
