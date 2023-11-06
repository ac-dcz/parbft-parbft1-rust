use crate::config::{Committee, Stake};
use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{SPBProof, SPBVote, Vote, QC};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
use std::collections::{HashMap, HashSet};
// use std::convert::TryInto;

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

// In HotStuff, votes/timeouts aggregated by round
// In VABA and async fallback, votes aggregated by round, timeouts/coin_share aggregated by view
pub struct Aggregator {
    committee: Committee,
    hs_votes_aggregators: HashMap<SeqNumber, HashMap<Digest, Box<QCMaker>>>,
    spb_votes_aggregators: HashMap<SeqNumber, HashMap<Digest, Box<ProofMaker>>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            hs_votes_aggregators: HashMap::new(),
            spb_votes_aggregators: HashMap::new(),
        }
    }

    pub fn add_hs_vote(&mut self, vote: Vote) -> ConsensusResult<Option<QC>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        self.hs_votes_aggregators
            .entry(vote.round)
            .or_insert_with(HashMap::new)
            .entry(vote.digest())
            .or_insert_with(|| Box::new(QCMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn add_spb_vote(&mut self, vote: SPBVote) -> ConsensusResult<Option<SPBProof>> {
        // TODO [issue #7]: A bad node may make us run out of memory by sending many votes
        // with different round numbers or different digests.

        // Add the new vote to our aggregator and see if we have a QC.
        self.spb_votes_aggregators
            .entry(vote.round)
            .or_insert_with(HashMap::new)
            .entry(vote.digest())
            .or_insert_with(|| Box::new(ProofMaker::new()))
            .append(vote, &self.committee)
    }

    // used in HotStuff
    pub fn cleanup_hs(&mut self, round: &SeqNumber) {
        self.hs_votes_aggregators.retain(|k, _| k >= round);
    }

    pub fn cleanup_mvba(&mut self, round: &SeqNumber) {
        self.spb_votes_aggregators.retain(|k, _| k >= round);
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
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<QC>> {
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
                round: vote.round,
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
        let hash = vote.hash.clone();
        let phase = vote.phase;
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuseinQC(author)
        );
        self.votes.push(vote);
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(SPBProof {
                hash,
                phase: phase + 1,
                shares: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}
