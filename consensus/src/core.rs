use crate::messages::{Block, Vote};
use serde::{Deserialize, Serialize};
pub type SeqNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    Propose(Block),
    Vote(Vote),
    LoopBack(Block),
}
