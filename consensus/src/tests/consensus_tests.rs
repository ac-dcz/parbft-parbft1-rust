use super::*;
use crate::common::{committee, keys, MockMempool};
use crate::config::Parameters;
use crypto::{SecretKey, SecretShare};
use futures::future::try_join_all;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

fn spawn_nodes(
    keys: Vec<(PublicKey, SecretKey)>,
    committee: Committee,
    store_path: &str,
    tss_keys: SecretShare,
) -> Vec<JoinHandle<Block>> {
    keys.into_iter()
        .enumerate()
        .map(|(i, (name, secret))| {
            let committee = committee.clone();
            let parameters = Parameters {
                timeout_delay: 100,
                ..Parameters::default()
            };
            let pk_set = tss_keys.pkset.clone();
            let signature_service =
                SignatureService::new(secret, Some(tss_keys.secret.clone().into_inner()));
            let store_path = format!("{}_{}", store_path, i);
            let _ = fs::remove_dir_all(&store_path);
            let store = Store::new(&store_path)?;
            // let signature_service = SignatureService::new(secret, None);
            let (tx_consensus, rx_consensus) = channel(10);
            let (tx_smvba, rx_smvba) = channel(10);
            let (tx_consensus_mempool, rx_consensus_mempool) = channel(1);
            MockMempool::run(rx_consensus_mempool);
            let (tx_commit, mut rx_commit) = channel(1);
            // let size = committee.size();
            // let threshold = (size - 1) / 3 + 1;
            // let mut rng = rand::thread_rng();
            // let sk_set = SecretKeySet::random(threshold, &mut rng);
            // let pk_set = sk_set.public_keys();
            tokio::spawn(async move {
                Consensus::run(
                    name,
                    committee,
                    parameters,
                    store,
                    signature_service,
                    pk_set,
                    tx_consensus,
                    rx_consensus,
                    tx_smvba,
                    rx_smvba,
                    tx_consensus_mempool,
                    tx_commit,
                    Protocol::HotStuffAndSMVBA,
                )
                .await
                .unwrap();

                rx_commit.recv().await.unwrap()
            })
        })
        .collect()
}

#[tokio::test]
async fn end_to_end() {
    let mut committee = committee();
    committee.increment_base_port(6000);
    let tss_key = SecretShare::default();
    // Run all nodes.
    let store_path = ".db_test_end_to_end";
    let handles = spawn_nodes(keys(), committee, store_path, tss_key);

    // Ensure all threads terminated correctly.
    let blocks = try_join_all(handles).await.unwrap();
    assert!(blocks.windows(2).all(|w| w[0] == w[1]));
}
