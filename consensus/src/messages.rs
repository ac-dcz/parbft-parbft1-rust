use crate::config::Committee;
use crate::core::{SeqNumber, FIN_PHASE, INIT_PHASE, LOCK_PHASE, OPT, PES};
use crate::error::{ConsensusError, ConsensusResult};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use threshold_crypto::{PublicKeySet, SignatureShare};

#[cfg(test)]
#[path = "tests/messages_tests.rs"]
pub mod messages_tests;

// daniel: Add view, height, fallback in Block, Vote and QC
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub qc: QC, //前一个节点的highQC
    pub author: PublicKey,
    pub height: SeqNumber,
    pub epoch: SeqNumber,
    pub payload: Vec<Digest>,
    pub signature: Signature,
    pub tag: u8,
}

impl Block {
    pub async fn new(
        qc: QC,
        author: PublicKey,
        height: SeqNumber,
        epoch: SeqNumber,
        payload: Vec<Digest>,
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            qc,
            author,
            height,
            epoch,
            payload,
            signature: Signature::default(),
            tag: OPT,
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
        hasher.update(self.height.to_le_bytes());
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
            "{}: B(author {}, height {}, epoch {}, qc {:?}, payload_len {})",
            self.digest(),
            self.author,
            self.height,
            self.epoch,
            self.qc,
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}", self.height)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HVote {
    pub hash: Digest,
    pub height: SeqNumber,
    pub epoch: SeqNumber,
    pub proposer: PublicKey, // proposer of the block
    pub author: PublicKey,
    pub signature: Signature,
}

impl HVote {
    pub async fn new(
        block: &Block,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let vote = Self {
            hash: block.digest(),
            height: block.height,
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

impl Hash for HVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.proposer.0);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for HVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Vote(blockhash {}, proposer {}, height {}, epoch {},  voter {})",
            self.hash, self.proposer, self.height, self.epoch, self.author
        )
    }
}

impl fmt::Display for HVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Vote{}", self.hash)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SPBValue {
    pub block: Block,
    pub round: SeqNumber,
    pub phase: u8,
}

impl SPBValue {
    pub async fn new(block: Block, round: SeqNumber, phase: u8) -> Self {
        Self {
            block,
            round,
            phase,
        }
    }

    pub fn verify(
        &self,
        committee: &Committee,
        proof: &SPBProof,
        pk_set: &PublicKeySet,
    ) -> ConsensusResult<()> {
        self.block.verify(committee)?;
        proof.verify(committee, pk_set)?;
        Ok(())
    }
}

impl Hash for SPBValue {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.block.digest());
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.phase.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for SPBValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "SPBValue(proposer {}, round {}, pahse {})",
            self.block.author, self.round, self.phase
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SPBVote {
    pub hash: Digest,
    pub phase: u8,
    pub height: SeqNumber,
    pub epoch: SeqNumber,
    pub round: SeqNumber,
    pub proposer: PublicKey,
    pub author: PublicKey,
    pub signature: Signature,
}

impl SPBVote {
    pub async fn new(
        value: SPBValue,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut vote = Self {
            hash: value.digest(),
            phase: value.phase,
            height: value.block.height,
            epoch: value.block.epoch,
            round: value.round,
            proposer: value.block.author,
            author,
            signature: Signature::default(),
        };
        vote.signature = signature_service.request_signature(vote.digest()).await;
        return vote;
    }

    //验证门限签名是否正确
    pub fn verify(&self, committee: &Committee, _pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        // self.signature.verify(&self.digest(), &self.author)?;
        // let tss_pk = pk_set.public_key_share(committee.id(self.author));
        // // Check the signature.
        // ensure!(
        //     tss_pk.verify(&self.signature_share, &self.digest()),
        //     ConsensusError::InvalidThresholdSignature(self.author)
        // );

        Ok(())
    }
}

impl Hash for SPBVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.hash.clone());
        hasher.update(self.phase.to_be_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for SPBVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "spb vote (author {}, height {}, round {}, phase {}, proposer {})",
            self.author, self.height, self.round, self.phase, self.proposer,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SPBProof {
    pub phase: u8, //SPB的哪一个阶段
    pub round: SeqNumber,
    pub height: SeqNumber,
    pub shares: Vec<SPBVote>,
}

impl SPBProof {
    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        if self.phase <= INIT_PHASE {
            //第一阶段不做检查
            return Ok(());
        }

        if self.phase >= LOCK_PHASE {
            //检查门限签名是否正确
            let mut weight = 0;
            for share in self.shares.iter() {
                let name = share.author;
                let voting_rights = committee.stake(&name);
                ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(name));
                weight += voting_rights;
            }
            ensure!(
                weight >= committee.quorum_threshold(), //2f+1
                ConsensusError::SPBRequiresQuorum
            );

            for share in &self.shares {
                share.verify(committee, pk_set)?;
            }
        }

        Ok(())
    }
}

impl fmt::Debug for SPBProof {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "SPBProof(height {} ,round {}, phase {})",
            self.height, self.round, self.phase
        )
    }
}

impl fmt::Display for SPBProof {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "SPBProof(round {}, phase {})", self.round, self.phase)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MDone {
    pub author: PublicKey,
    pub signature: Signature,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub round: SeqNumber,
}

impl MDone {
    pub async fn new(
        author: PublicKey,
        mut signature_service: SignatureService,
        epoch: SeqNumber,
        height: SeqNumber,
        round: SeqNumber,
    ) -> Self {
        let mut done = Self {
            author,
            signature: Signature::default(),
            epoch,
            height,
            round,
        };
        done.signature = signature_service.request_signature(done.digest()).await;
        return done;
    }

    pub fn verify(&self) -> ConsensusResult<()> {
        // self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for MDone {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_be_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for MDone {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Done(author {},height {},epoch {},round {},)",
            self.author, self.height, self.epoch, self.round
        )
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub enum PreVoteTag {
    Yes(SPBValue, SPBProof),
    No(),
    // No(SignatureShare)
}

impl PreVoteTag {
    pub async fn new_yes(v: SPBValue, p: SPBProof) -> PreVoteTag {
        Self::Yes(v, p)
    }

    pub async fn new_no() -> PreVoteTag {
        Self::No()
    }

    // async fn new_no(
    //     leader: PublicKey,
    //     round: SeqNumber,
    //     height: SeqNumber,
    //     epoch: SeqNumber,
    //     mut signature_service: SignatureService,
    // ) -> SMVBAVoteTag {
    //     let share = signature_service
    //         .request_tss_signature(Self::digest_no(leader, round, height, epoch))
    //         .await
    //         .unwrap();
    //     Self::No(share)
    // }

    // fn digest_no(
    //     leader: PublicKey,
    //     round: SeqNumber,
    //     height: SeqNumber,
    //     epoch: SeqNumber,
    // ) -> Digest {
    //     let mut hasher = Sha512::new();
    //     hasher.update(leader.0);
    //     hasher.update("no-null");
    //     hasher.update(round.to_be_bytes());
    //     hasher.update(height.to_le_bytes());
    //     hasher.update(epoch.to_le_bytes());
    //     Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    // }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        match self {
            Self::Yes(v, p) => v.verify(committee, p, pk_set),
            Self::No() => {
                // let tss_pk = pk_set.public_key_share(committee.id(name));
                // // Check the signature.
                // ensure!(
                //     tss_pk.verify(share, d),
                //     ConsensusError::InvalidThresholdSignature(name)
                // );
                Ok(())
            }
        }
    }

    pub fn is_yes(&self) -> bool {
        match self {
            Self::Yes(..) => true,
            _ => false,
        }
    }
}

impl fmt::Debug for PreVoteTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreVoteTag::Yes(..) => write!(f, "PreVote-Yes"),
            PreVoteTag::No(..) => write!(f, "PreVote-No"),
        }
    }
}

impl fmt::Display for PreVoteTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreVoteTag::Yes(..) => write!(f, "PreVote-Yes"),
            PreVoteTag::No(..) => write!(f, "PreVote-No"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MPreVote {
    pub author: PublicKey,
    pub signature: Signature,
    pub leader: PublicKey,
    pub round: SeqNumber,
    pub height: SeqNumber,
    pub epoch: SeqNumber,
    pub tag: PreVoteTag,
}

impl MPreVote {
    pub async fn new(
        author: PublicKey,
        leader: PublicKey,
        mut signature_service: SignatureService,
        round: SeqNumber,
        height: SeqNumber,
        epoch: SeqNumber,
        tag: PreVoteTag,
    ) -> Self {
        let mut pvote = Self {
            author,
            signature: Signature::default(),
            leader,
            round,
            height,
            epoch,
            tag,
        };
        pvote.signature = signature_service.request_signature(pvote.digest()).await;
        return pvote;
    }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        //check signature
        self.signature.verify(&self.digest(), &self.author)?;

        //chekc tag
        self.tag.verify(committee, pk_set)?;

        Ok(())
    }

    pub fn is_yes(&self) -> bool {
        self.tag.is_yes()
    }
}

impl Hash for MPreVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.leader.0);
        hasher.update(self.round.to_be_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.tag.to_string());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for MPreVote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PreVote(author {}, leader {}, round {}, tag {},)",
            self.author, self.leader, self.round, self.tag
        )
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub enum MVoteTag {
    Yes(SPBValue, SPBProof, SPBVote),
    // No(Vec<MPreVote>, SignatureShare),
    No(),
}

impl MVoteTag {
    pub async fn new_yes(
        author: PublicKey,
        value: SPBValue,
        proof: SPBProof,
        signature_service: SignatureService,
    ) -> Self {
        let spbvote = SPBVote::new(value.clone(), author, signature_service).await;
        Self::Yes(value, proof, spbvote)
    }

    pub async fn new_no() -> Self {
        Self::No()
    }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        match self {
            Self::Yes(value, proof, prevote) => {
                value.verify(committee, proof, pk_set)?;
                prevote.verify(committee, pk_set)?;
                Ok(())
            }
            Self::No() => Ok(()),
        }
    }
}

impl fmt::Debug for MVoteTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Yes(..) => {
                write!(f, "Vote-Yes")
            }
            Self::No(..) => {
                write!(f, "Vote-No")
            }
        }
    }
}

impl fmt::Display for MVoteTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Yes(..) => {
                write!(f, "Vote-Yes")
            }
            Self::No(..) => {
                write!(f, "Vote-No")
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MVote {
    pub author: PublicKey,
    pub leader: PublicKey,
    pub round: SeqNumber,
    pub height: SeqNumber,
    pub epoch: SeqNumber,
    pub signature: Signature,
    pub tag: MVoteTag,
}

impl MVote {
    pub async fn new(
        author: PublicKey,
        leader: PublicKey,
        mut signature_service: SignatureService,
        round: SeqNumber,
        height: SeqNumber,
        epoch: SeqNumber,
        tag: MVoteTag,
    ) -> Self {
        let mut pvote = Self {
            author,
            signature: Signature::default(),
            leader,
            round,
            height,
            epoch,
            tag,
        };
        pvote.signature = signature_service.request_signature(pvote.digest()).await;
        return pvote;
    }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        //check signature
        self.signature.verify(&self.digest(), &self.author)?;

        //chekc tag
        self.tag.verify(committee, pk_set)?;

        Ok(())
    }
}

impl Hash for MVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_be_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.tag.to_string());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for MVote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MVote(author {}, leader {}, round {}, tag {},)",
            self.author, self.leader, self.round, self.tag
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MHalt {
    pub author: PublicKey,
    pub leader: PublicKey,
    pub value: SPBValue,
    pub proof: SPBProof,
    pub round: SeqNumber,
    pub height: SeqNumber,
    pub epoch: SeqNumber,
    pub signature: Signature,
}

impl MHalt {
    pub async fn new(
        author: PublicKey,
        leader: PublicKey,
        value: SPBValue,
        proof: SPBProof,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut halt = Self {
            round: proof.round,
            height: proof.height,
            epoch: value.block.epoch,
            author,
            value,
            leader,
            proof,
            signature: Signature::default(),
        };
        halt.signature = signature_service.request_signature(halt.digest()).await;
        return halt;
    }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        //check signature
        self.signature.verify(&self.digest(), &self.author)?;

        //check proof
        ensure!(
            self.proof.phase == FIN_PHASE, //是否为finsh phase 阶段
            ConsensusError::InvalidFinProof()
        );
        self.value.verify(committee, &self.proof, pk_set)?;

        Ok(())
    }
}

impl Hash for MHalt {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for MHalt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MHalt(author {}, round {}, leader {},)",
            self.author, self.round, self.leader
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum PrePareProof {
    OPT(QC),
    PES(SPBProof),
}

impl PrePareProof {
    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        match self {
            Self::OPT(qc) => qc.verify(committee),
            Self::PES(proof) => {
                ensure!(proof.phase == FIN_PHASE, ConsensusError::InvalidFinProof());
                proof.verify(committee, pk_set)
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrePare {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub tag: u8,
    pub block: Block,
    pub proof: PrePareProof,
    pub signature: Signature,
}

impl PrePare {
    pub async fn new(
        author: PublicKey,
        block: Block,
        proof: PrePareProof,
        tag: u8,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut prepare = Self {
            author,
            epoch: block.epoch,
            height: block.height,
            tag,
            block,
            proof,
            signature: Signature::default(),
        };
        prepare.signature = signature_service.request_signature(prepare.digest()).await;
        return prepare;
    }
    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        self.signature.verify(&self.digest(), &self.author)?;

        self.proof.verify(committee, pk_set)
    }
}

impl Hash for PrePare {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for PrePare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut tag = String::from("Unkonw");
        if self.tag == OPT {
            tag = "OPT".to_string();
        } else if self.tag == PES {
            tag = "PES".to_string();
        }
        write!(f, "PrePare(tag {}, block {:?})", tag, self.block)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ABAVal {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub round: SeqNumber,
    pub phase: u8,
    pub val: usize,
    pub signature: Signature,
}

impl ABAVal {
    pub async fn new(
        author: PublicKey,
        epoch: SeqNumber,
        height: SeqNumber,
        round: SeqNumber,
        val: usize,
        phase: u8,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut aba_val = Self {
            author,
            epoch,
            height,
            round,
            val,
            phase,
            signature: Signature::default(),
        };
        aba_val.signature = signature_service.request_signature(aba_val.digest()).await;
        return aba_val;
    }

    pub fn verify(&self) -> ConsensusResult<()> {
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for ABAVal {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for ABAVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ABAVal(round {},phase {},val {})",
            self.round, self.phase, self.val
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ABAOutput {
    pub author: PublicKey,
    pub epoch: SeqNumber,
    pub height: SeqNumber,
    pub round: SeqNumber,
    pub val: usize,
    pub signature: Signature,
}

impl ABAOutput {
    pub async fn new(
        author: PublicKey,
        epoch: SeqNumber,
        height: SeqNumber,
        round: SeqNumber,
        val: usize,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut out = Self {
            author,
            epoch,
            height,
            round,
            val,
            signature: Signature::default(),
        };
        out.signature = signature_service.request_signature(out.digest()).await;
        return out;
    }

    pub fn verify(&self) -> ConsensusResult<()> {
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for ABAOutput {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for ABAOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ABAOutput(author {},height {},round {},val {})",
            self.author, self.height, self.round, self.val
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub hash: Digest,
    pub height: SeqNumber,
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
        self.hash == Digest::default() && self.height != 0
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
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.proposer.0);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "QC(hash {}, height {},  proposer {})",
            self.hash, self.height, self.proposer
        )
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.height == other.height
            && self.epoch == self.epoch
            && self.proposer == other.proposer
    }
}

// leader选举时 每个发送自己的randomshare
#[derive(Clone, Serialize, Deserialize)]
pub struct RandomnessShare {
    pub height: SeqNumber,
    pub epoch: SeqNumber,
    pub round: SeqNumber,
    pub author: PublicKey,
    pub signature_share: SignatureShare,
    // pub high_qc: Option<QC>, // attach its height-2 qc in the randomness share as an optimization
}

impl RandomnessShare {
    pub async fn new(
        height: SeqNumber,
        epoch: SeqNumber,
        round: SeqNumber,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut hasher = Sha512::new();
        hasher.update(round.to_le_bytes());
        hasher.update(height.to_le_bytes());
        hasher.update(epoch.to_le_bytes());
        let digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
        let signature_share = signature_service
            .request_tss_signature(digest)
            .await
            .unwrap();
        Self {
            round,
            height,
            epoch,
            author,
            signature_share,
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
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.epoch.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for RandomnessShare {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "RandomnessShare (author {}, height {},round {})",
            self.author, self.height, self.round,
        )
    }
}

// f+1 个 RandomnessShare 合成的
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct RandomCoin {
    pub height: SeqNumber, //
    pub epoch: SeqNumber,
    pub round: SeqNumber,
    pub leader: PublicKey, // elected leader of the view
    pub shares: Vec<RandomnessShare>,
}

impl RandomCoin {
    // pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
    //     // Ensure the QC has a quorum.
    //     let mut weight = 0;
    //     let mut used = HashSet::new();
    //     for share in self.shares.iter() {
    //         let name = share.author;
    //         ensure!(
    //             !used.contains(&name),
    //             ConsensusError::AuthorityReuseinCoin(name)
    //         );
    //         let voting_rights = committee.stake(&name);
    //         ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(name));
    //         used.insert(name);
    //         weight += voting_rights;
    //     }
    //     ensure!(
    //         weight >= committee.random_coin_threshold(), //f+1
    //         ConsensusError::RandomCoinRequiresQuorum
    //     );

    //     let mut sigs = BTreeMap::new(); //构建BTree选择leader
    //                                     // Check the random shares.
    //     for share in &self.shares {
    //         share.verify(committee, pk_set)?;
    //         sigs.insert(committee.id(share.author), share.signature_share.clone());
    //     }
    //     if let Ok(sig) = pk_set.combine_signatures(sigs.iter()) {
    //         let id = usize::from_be_bytes((&sig.to_bytes()[0..8]).try_into().unwrap())
    //             % committee.size();
    //         let mut keys: Vec<_> = committee.authorities.keys().cloned().collect();
    //         keys.sort();
    //         let leader = keys[id];
    //         ensure!(
    //             leader == self.leader,
    //             ConsensusError::RandomCoinWithWrongLeader
    //         );
    //     } else {
    //         ensure!(true, ConsensusError::RandomCoinWithWrongShares);
    //     }

    //     Ok(())
    // }
}

impl fmt::Debug for RandomCoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "RandomCoin(epoch {}, height {},round {}, leader {})",
            self.epoch, self.height, self.round, self.leader
        )
    }
}
