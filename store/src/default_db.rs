use super::*;
use std::collections::HashMap;
use std::hash::Hash;
// use std::sync::RwLock;
// 默认内存DB
pub struct DefaultDB<K: Clone, V: Clone> {
    pub data: HashMap<K, V>,
}

impl<K: Clone, V: Clone> DefaultDB<K, V> {
    pub fn new() -> Self {
        Self {
            data: HashMap::<K, V>::new(),
        }
    }
}

impl<K: Clone + PartialEq + Eq + Hash, V: Clone> BaseDB<K, V> for DefaultDB<K, V> {
    fn put(&mut self, k: &K, v: &V) -> Result<(), Error> {
        // let mut db = self.data.write().unwrap();
        // db.insert(k.clone(), v.clone());
        self.data.insert(k.clone(), v.clone());
        Ok(())
    }
    fn get(&self, k: &K) -> Result<Option<V>, Error> {
        // let db = self.data.read().unwrap();
        let v: Option<&V> = self.data.get(k);
        if let Some(d) = v {
            return Ok(Some(d.clone()));
        }
        Ok(None)
    }
}
