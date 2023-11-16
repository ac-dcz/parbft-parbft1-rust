use crate::config::Committee;
use crate::core::SeqNumber;
use crate::mempool::{ConsensusMempoolMessage, PayloadStatus};
use crate::messages::{Block, HVote, QC};
use crate::OPT;
use crypto::Hash as _;
use crypto::{generate_keypair, Digest, PublicKey, SecretKey, Signature};
use rand::rngs::StdRng;
use rand::RngCore as _;
use rand::SeedableRng as _;
use tokio::sync::mpsc::Receiver;

// Fixture.
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture.
pub fn committee() -> Committee {
    Committee::new(
        keys()
            .into_iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let address = format!("127.0.0.1:{}", i).parse().unwrap();
                let stake = 1;
                (name, 0, stake, address)
            })
            .collect(),
        /* epoch */ 1,
    )
}

impl Committee {
    pub fn increment_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.address.port();
            authority.address.set_port(base_port + port);
        }
    }
}

impl Block {
    pub fn new_from_key(
        qc: QC,
        author: PublicKey,
        height: SeqNumber,
        payload: Vec<Digest>,
        secret: &SecretKey,
    ) -> Self {
        let block = Block {
            qc,
            author,
            height,
            epoch: 0,
            payload,
            signature: Signature::default(),
            tag: OPT,
        };
        let signature = Signature::new(&block.digest(), secret);
        Self { signature, ..block }
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl HVote {
    pub fn new_from_key(
        hash: Digest,
        height: SeqNumber,
        proposer: PublicKey,
        author: PublicKey,
        secret: &SecretKey,
    ) -> Self {
        let vote = Self {
            hash,
            height,
            epoch: 0,
            proposer,
            author,
            signature: Signature::default(),
        };
        let signature = Signature::new(&vote.digest(), &secret);
        Self { signature, ..vote }
    }
}

impl PartialEq for HVote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

// impl SPBVote {
//     pub fn new_from_key(value: SPBValue, author: PublicKey) -> Self {
//         Self {
//             hash: value.digest(),
//             phase: value.phase,
//             height: value.block.height,
//             epoch: value.block.epoch,
//             round: value.round,
//             proposer: value.block.author,
//             author,
//             signature_share,SignatureShare::,
//         }
//     }
// }

// Fixture.
pub fn block() -> Block {
    let (public_key, secret_key) = keys().pop().unwrap();
    Block::new_from_key(QC::genesis(), public_key, 1, Vec::new(), &secret_key)
}

// Fixture.
pub fn vote() -> HVote {
    let (public_key, secret_key) = keys().pop().unwrap();
    HVote::new_from_key(block().digest(), 1, block().author, public_key, &secret_key)
}

// Fixture.
pub fn qc() -> QC {
    let mut keys = keys();
    let (public_key, _) = keys.pop().unwrap();
    let qc = QC {
        hash: Digest::default(),
        height: 1,
        epoch: 0,
        proposer: public_key,
        acceptor: public_key,
        votes: Vec::new(),
    };
    let digest = qc.digest();
    let votes: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();
    QC { votes, ..qc }
}

// Fixture.
pub fn chain(keys: Vec<(PublicKey, SecretKey)>) -> Vec<Block> {
    let mut latest_qc = QC::genesis();
    keys.iter()
        .enumerate()
        .map(|(i, key)| {
            // Make a block.
            let (public_key, secret_key) = key;
            let block = Block::new_from_key(
                latest_qc.clone(),
                *public_key,
                1 + i as SeqNumber,
                Vec::new(),
                secret_key,
            );

            // Make a qc for that block (it will be used for the next block).
            let qc = QC {
                epoch: 0,
                hash: block.digest(),
                height: block.height,
                proposer: block.author,
                acceptor: block.author,
                votes: Vec::new(),
            };
            let digest = qc.digest();
            let votes: Vec<_> = keys
                .iter()
                .map(|(public_key, secret_key)| (*public_key, Signature::new(&digest, secret_key)))
                .collect();
            latest_qc = QC { votes, ..qc };

            // Return the block.
            block
        })
        .collect()
}

// Fixture
pub struct MockMempool;

impl MockMempool {
    pub fn run(mut consensus_mempool_channel: Receiver<ConsensusMempoolMessage>) {
        tokio::spawn(async move {
            while let Some(message) = consensus_mempool_channel.recv().await {
                match message {
                    ConsensusMempoolMessage::Get(_max, sender) => {
                        let mut rng = StdRng::from_seed([0; 32]);
                        let mut payload = [0u8; 32];
                        rng.fill_bytes(&mut payload);
                        sender.send(vec![Digest(payload)]).unwrap();
                    }
                    ConsensusMempoolMessage::Verify(_block, sender, _) => {
                        sender.send(PayloadStatus::Accept).unwrap()
                    }
                    ConsensusMempoolMessage::Cleanup(_digests, _round) => (),
                }
            }
        });
    }
}
