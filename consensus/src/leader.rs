use std::collections::HashMap;

use crate::config::Committee;
use crate::core::SeqNumber;
use crate::messages::RandomCoin;
use crypto::PublicKey;

pub type LeaderElector = RandomLeaderElector;

pub struct RandomLeaderElector {
    committee: Committee,
    random_coins: HashMap<(SeqNumber, SeqNumber), RandomCoin>,
}

impl RandomLeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            random_coins: HashMap::new(),
        }
    }

    pub fn get_leader(&self, round: SeqNumber) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[round as usize % self.committee.size()]
    }

    pub fn add_random_coin(&mut self, random_coin: RandomCoin) {
        self.random_coins
            .insert((random_coin.height, random_coin.round), random_coin);
    }

    // pub fn get_coin_leader(&self, height: SeqNumber, round: SeqNumber) -> Option<PublicKey> {
    //     if !self.random_coins.contains_key(&(height, round)) {
    //         return None;
    //     }
    //     Some(self.random_coins.get(&(height, round)).unwrap().leader)
    // }
}
