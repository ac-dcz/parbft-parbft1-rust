use crate::config::{Committee, Stake};
use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{HVote, RandomCoin, RandomnessShare, SPBProof, SPBVote, QC};
use crypto::{PublicKey, Signature};
use std::collections::{BTreeMap, HashMap, HashSet};
use threshold_crypto::PublicKeySet;
// use std::convert::TryInto;

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

// In HotStuff, votes/timeouts aggregated by round
// In VABA and async fallback, votes aggregated by round, timeouts/coin_share aggregated by view
pub struct Aggregator {
    committee: Committee,
    hs_votes_aggregators: HashMap<SeqNumber, Box<QCMaker>>,
    spb_votes_aggregators: HashMap<(SeqNumber, SeqNumber, u8), Box<ProofMaker>>,
    pre_votes_aggregators: HashMap<(SeqNumber, SeqNumber), Box<ProofMaker>>,
    smvba_randomcoin_aggregators: HashMap<(SeqNumber, SeqNumber), Box<SMVBARandomCoinMaker>>,
    aba_randomcoin_aggregators: HashMap<(SeqNumber, SeqNumber), Box<ABARandomCoinMaker>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            hs_votes_aggregators: HashMap::new(),
            spb_votes_aggregators: HashMap::new(),
            smvba_randomcoin_aggregators: HashMap::new(),
            aba_randomcoin_aggregators: HashMap::new(),
            pre_votes_aggregators: HashMap::new(),
        }
    }

    pub fn add_hs_vote(&mut self, vote: HVote) -> ConsensusResult<Option<QC>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        self.hs_votes_aggregators
            .entry(vote.height)
            .or_insert_with(|| Box::new(QCMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn add_spb_vote(&mut self, vote: SPBVote) -> ConsensusResult<Option<SPBProof>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        self.spb_votes_aggregators
            .entry((vote.height, vote.round, vote.phase))
            .or_insert_with(|| Box::new(ProofMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn add_pre_vote(&mut self, vote: SPBVote) -> ConsensusResult<Option<SPBProof>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        self.pre_votes_aggregators
            .entry((vote.height, vote.round))
            .or_insert_with(|| Box::new(ProofMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn add_smvba_random(
        &mut self,
        share: RandomnessShare,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<Option<RandomCoin>> {
        self.smvba_randomcoin_aggregators
            .entry((share.height, share.round))
            .or_insert_with(|| Box::new(SMVBARandomCoinMaker::new()))
            .append(share, &self.committee, pk_set)
    }

    pub fn add_aba_random(
        &mut self,
        share: RandomnessShare,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<Option<usize>> {
        self.aba_randomcoin_aggregators
            .entry((share.height, share.round))
            .or_insert_with(|| Box::new(ABARandomCoinMaker::new()))
            .append(share, &self.committee, pk_set)
    }

    // used in HotStuff
    pub fn cleanup_hs_vote(&mut self, height: &SeqNumber) {
        self.hs_votes_aggregators.retain(|k, _| k > height);
    }

    pub fn cleanup_spb_vote(&mut self, height: &SeqNumber) {
        self.spb_votes_aggregators
            .retain(|(h, _, ..), _| h > height);
        self.pre_votes_aggregators.retain(|(h, ..), _| h > height);
    }

    pub fn cleanup_mvba_random(&mut self, height: &SeqNumber) {
        self.smvba_randomcoin_aggregators
            .retain(|(h, _), _| h > height);
    }
}

struct QCMaker {
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl QCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: HVote, committee: &Committee) -> ConsensusResult<Option<QC>> {
        let author = vote.author;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinQC(author)
        );
        self.votes.push((author, vote.signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(QC {
                hash: vote.hash.clone(),
                height: vote.height,
                epoch: vote.epoch,
                proposer: vote.proposer,
                acceptor: vote.proposer,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

struct ProofMaker {
    weight: Stake,
    votes: Vec<SPBVote>,
    used: HashSet<PublicKey>,
}

impl ProofMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        vote: SPBVote,
        committee: &Committee,
    ) -> ConsensusResult<Option<SPBProof>> {
        let author = vote.author;
        let phase = vote.phase;
        let round = vote.round;
        let height = vote.height;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinProof(author, self.used.clone())
        );
        self.votes.push(vote);
        self.weight += committee.stake(&author);

        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(SPBProof {
                height,
                phase: phase + 1, //为下一个阶段产生proof
                round,
                shares: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

struct SMVBARandomCoinMaker {
    weight: Stake,
    shares: Vec<RandomnessShare>,
    used: HashSet<PublicKey>,
}

impl SMVBARandomCoinMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            shares: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        share: RandomnessShare,
        committee: &Committee,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<Option<RandomCoin>> {
        let author = share.author;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinCoin(author)
        );
        self.shares.push(share.clone());
        self.weight += committee.stake(&author);
        if self.weight == committee.random_coin_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            let mut sigs = BTreeMap::new();
            // Check the random shares.
            for share in self.shares.clone() {
                sigs.insert(
                    committee.id(share.author.clone()),
                    share.signature_share.clone(),
                );
            }
            if let Ok(sig) = pk_set.combine_signatures(sigs.iter()) {
                let id = usize::from_be_bytes((&sig.to_bytes()[0..8]).try_into().unwrap())
                    % committee.size();
                let mut keys: Vec<_> = committee.authorities.keys().cloned().collect();
                keys.sort();
                let leader = keys[id];

                let random_coin = RandomCoin {
                    height: share.height,
                    epoch: share.epoch,
                    round: share.round,
                    leader,
                    shares: self.shares.to_vec(),
                };
                return Ok(Some(random_coin));
            }
        }
        Ok(None)
    }
}

struct ABARandomCoinMaker {
    weight: Stake,
    shares: Vec<RandomnessShare>,
    used: HashSet<PublicKey>,
}

impl ABARandomCoinMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            shares: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        share: RandomnessShare,
        committee: &Committee,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<Option<usize>> {
        let author = share.author;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinCoin(author)
        );
        self.shares.push(share.clone());
        self.weight += committee.stake(&author);
        if self.weight == committee.random_coin_threshold() {
            // self.weight = 0; // Ensures QC is only made once.
            let mut sigs = BTreeMap::new();
            // Check the random shares.
            for share in self.shares.clone() {
                sigs.insert(
                    committee.id(share.author.clone()),
                    share.signature_share.clone(),
                );
            }
            if let Ok(sig) = pk_set.combine_signatures(sigs.iter()) {
                let id = usize::from_be_bytes((&sig.to_bytes()[0..8]).try_into().unwrap()) % 2;

                return Ok(Some(id));
            }
        }
        Ok(None)
    }
}
