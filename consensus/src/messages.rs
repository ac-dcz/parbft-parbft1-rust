use crate::config::Committee;
use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::{fmt, hash, vec};
use threshold_crypto::{PublicKeySet, SecretKeyShare, SignatureShare};

#[cfg(test)]
#[path = "tests/messages_tests.rs"]
pub mod messages_tests;

// daniel: Add view, height, fallback in Block, Vote and QC
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub qc: QC, //前一个节点的highQC
    pub author: PublicKey,
    pub round: SeqNumber,
    pub epoch: SeqNumber,
    pub payload: Vec<Digest>,
    pub signature: Signature,
    pub path_tag: u8,
}

impl Block {
    pub async fn new(
        qc: QC,
        author: PublicKey,
        round: SeqNumber,
        epoch: SeqNumber,
        payload: Vec<Digest>,
        mut signature_service: SignatureService,
        path_tag: u8,
    ) -> Self {
        let block = Self {
            qc,
            author,
            round,
            epoch,
            payload,
            signature: Signature::default(),
            path_tag,
        };

        let signature = signature_service.request_signature(block.digest()).await;
        Self { signature, ..block }
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn parent(&self) -> &Digest {
        &self.qc.hash
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        // Check the embedded QC.
        if self.qc != QC::genesis() {
            self.qc.verify(committee)?;
        }

        Ok(())
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        for x in &self.payload {
            hasher.update(x);
        }
        hasher.update(&self.qc.hash);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B(author {}, round {}, epoch {}, qc {:?}, payload_len {})",
            self.digest(),
            self.author,
            self.round,
            self.epoch,
            self.qc,
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}", self.round)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub hash: Digest,
    pub round: SeqNumber,
    pub epoch: SeqNumber,
    pub proposer: PublicKey, // proposer of the block
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        block: &Block,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let vote = Self {
            hash: block.digest(),
            round: block.round,
            epoch: block.epoch,
            proposer: block.author,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.proposer.0);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Vote(blockhash {}, proposer {}, round {}, epoch {},  voter {})",
            self.hash, self.proposer, self.round, self.epoch, self.author
        )
    }
}

impl fmt::Display for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Vote{}", self.hash)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum SPBValue {
    B(Block),
    D(Digest, PublicKey, Signature),
}

impl SPBValue {
    pub fn phase(&self) -> u8 {
        match self {
            SPBValue::B(..) => 1u8,
            SPBValue::D(..) => 2u8,
        }
    }
    pub fn proposer(&self) -> PublicKey {
        match self {
            SPBValue::B(b) => b.author,
            SPBValue::D(_, p, _) => p.clone(),
        }
    }
    pub fn verify(
        &self,
        committee: &Committee,
        proof: &SPBProof,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<()> {
        match self {
            SPBValue::B(b) => b.verify(committee),
            SPBValue::D(_, name, sig) => {
                let voting_rights = committee.stake(name);
                ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));

                // Check the signature.
                sig.verify(&self.digest(), name)?;

                // Check the proof
                proof.verify(committee, pk_set)?;

                Ok(())
            }
        }
    }
}

impl Hash for SPBValue {
    fn digest(&self) -> Digest {
        match self {
            SPBValue::B(b) => b.digest(),
            SPBValue::D(d, name, _) => {
                let mut hasher = Sha512::new();
                hasher.update(d);
                hasher.update(name.0);
                hasher.update(vec![2u8]);
                Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
            }
        }
    }
}

impl fmt::Debug for SPBValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "SPBValue(proposer {}, pahse {})",
            self.proposer(),
            self.phase(),
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SPBVote {
    pub hash: Digest,
    pub phase: u8,
    pub round: SeqNumber,
    pub epoch: SeqNumber,
    pub proposer: PublicKey,
    pub author: PublicKey,
    pub signature_share: SignatureShare,
}

impl SPBVote {
    pub async fn new(
        value: SPBValue,
        round: SeqNumber,
        epoch: SeqNumber,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut hasher = Sha512::new();
        hasher.update(value.digest());
        hasher.update(round.to_le_bytes());
        hasher.update(epoch.to_le_bytes());
        let digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
        let signature_share = signature_service
            .request_tss_signature(digest)
            .await
            .unwrap();

        Self {
            hash: value.digest(),
            phase: value.phase(),
            round,
            epoch,
            proposer: value.proposer(),
            author,
            signature_share,
        }
    }

    //验证门限签名是否正确
    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        let tss_pk = pk_set.public_key_share(committee.id(self.author));
        // Check the signature.
        ensure!(
            tss_pk.verify(&self.signature_share, &self.digest()),
            ConsensusError::InvalidThresholdSignature(self.author)
        );

        Ok(())
    }
}

impl Hash for SPBVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for SPBVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "RandomnessShare (author {}, view {}, sig share {:?})",
            self.author, self.round, self.signature_share
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SPBProof {
    pub hash: Digest,
    pub phase: u8, //SPB的哪一个阶段
    pub shares: Vec<SPBVote>,
}

impl SPBProof {
    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        if self.phase == 1 {
            //第一阶段不做检查
            return Ok(());
        }

        //第二阶段检查签名是否正确
        let mut weight = 0;
        for share in self.shares.iter() {
            if share.hash == self.hash {
                let name = share.author;
                let voting_rights = committee.stake(&name);
                ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(name));
                weight += voting_rights;
            }
        }
        ensure!(
            weight >= committee.spb_vote_threshold(), //f+1
            ConsensusError::SPBRequiresQuorum
        );

        for share in &self.shares {
            share.verify(committee, pk_set)?;
        }

        Ok(())
    }
}

impl fmt::Debug for SPBProof {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "SPB(phase {})", self.phase)
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub hash: Digest,
    pub round: SeqNumber,
    pub epoch: SeqNumber,
    pub proposer: PublicKey, // proposer of the block
    pub acceptor: PublicKey, // Node that accepts the QC and builds its f-chain extending it
    pub votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis() -> Self {
        QC::default()
    }

    pub fn timeout(&self) -> bool {
        self.hash == Digest::default() && self.round != 0
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0; //票数
        let mut used = HashSet::new(); //防止重复统计
        for (name, _) in self.votes.iter() {
            ensure!(
                !used.contains(name),
                ConsensusError::AuthorityReuseinQC(*name)
            );
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::QCRequiresQuorum
        );

        // Check the signatures.
        Signature::verify_batch(&self.digest(), &self.votes).map_err(ConsensusError::from)
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.proposer.0);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "QC(hash {}, round {},  proposer {})",
            self.hash, self.round, self.proposer
        )
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.round == other.round
            && self.epoch == self.epoch
            && self.proposer == other.proposer
    }
}

// leader选举时 每个发送自己的randomshare
#[derive(Clone, Serialize, Deserialize)]
pub struct RandomnessShare {
    pub round: SeqNumber, // round
    pub epoch: SeqNumber,
    pub author: PublicKey,
    pub signature_share: SignatureShare,
    pub high_qc: Option<QC>, // attach its height-2 qc in the randomness share as an optimization
}

impl RandomnessShare {
    pub async fn new(
        round: SeqNumber,
        epoch: SeqNumber,
        author: PublicKey,
        mut signature_service: SignatureService,
        high_qc: Option<QC>,
    ) -> Self {
        let mut hasher = Sha512::new();
        hasher.update(round.to_le_bytes());
        hasher.update(epoch.to_le_bytes());
        let digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
        let signature_share = signature_service
            .request_tss_signature(digest)
            .await
            .unwrap();
        Self {
            round,
            epoch,
            author,
            signature_share,
            high_qc,
        }
    }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        let tss_pk = pk_set.public_key_share(committee.id(self.author));
        // Check the signature.
        ensure!(
            tss_pk.verify(&self.signature_share, &self.digest()),
            ConsensusError::InvalidThresholdSignature(self.author)
        );

        Ok(())
    }
}

impl Hash for RandomnessShare {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for RandomnessShare {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "RandomnessShare (author {}, view {}, sig share {:?})",
            self.author, self.round, self.signature_share
        )
    }
}

// f+1 个 RandomnessShare 合成的
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct RandomCoin {
    pub round: SeqNumber, // round
    pub epoch: SeqNumber,
    pub leader: PublicKey, // elected leader of the view
    pub shares: Vec<RandomnessShare>,
}

impl RandomCoin {
    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for share in self.shares.iter() {
            let name = share.author;
            ensure!(
                !used.contains(&name),
                ConsensusError::AuthorityReuseinCoin(name)
            );
            let voting_rights = committee.stake(&name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(name));
            used.insert(name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.random_coin_threshold(), //f+1
            ConsensusError::RandomCoinRequiresQuorum
        );

        let mut sigs = BTreeMap::new(); //构建BTree选择leader
                                        // Check the random shares.
        for share in &self.shares {
            share.verify(committee, pk_set)?;
            sigs.insert(committee.id(share.author), share.signature_share.clone());
        }
        if let Ok(sig) = pk_set.combine_signatures(sigs.iter()) {
            let id = usize::from_be_bytes((&sig.to_bytes()[0..8]).try_into().unwrap())
                % committee.size();
            let mut keys: Vec<_> = committee.authorities.keys().cloned().collect();
            keys.sort();
            let leader = keys[id];
            ensure!(
                leader == self.leader,
                ConsensusError::RandomCoinWithWrongLeader
            );
        } else {
            ensure!(true, ConsensusError::RandomCoinWithWrongShares);
        }

        Ok(())
    }
}

impl fmt::Debug for RandomCoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "RandomCoin(epoch {}, round {}, leader {})",
            self.epoch, self.round, self.leader
        )
    }
}
