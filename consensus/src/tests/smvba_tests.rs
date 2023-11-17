// use super::*;
// use crate::common::{block, committee, keys, MockMempool};
// use crypto::{Hash, SecretKey, SecretShare};
// use std::fs;
// use tokio::sync::mpsc::channel;

// async fn core(
//     name: PublicKey,
//     secret: SecretKey,
//     store_path: &str,
// ) -> (
//     Sender<ConsensusMessage>,
//     Receiver<FilterInput>,
//     Receiver<Block>,
// ) {
//     let (tx_core, rx_core) = channel(1);
//     let (tx_network, rx_network) = channel(3);
//     let (tx_consensus_mempool, rx_consensus_mempool) = channel(1);
//     let (tx_commit, rx_commit) = channel(1);

//     let parameters = Parameters {
//         timeout_delay: 100,
//         ..Parameters::default()
//     };
//     let tss_keys = SecretShare::default();
//     let pk_set = tss_keys.pkset.clone();
//     let signature_service = SignatureService::new(secret, Some(tss_keys.secret.into_inner()));
//     let _ = fs::remove_dir_all(store_path);
//     let store = Store::new_default().unwrap();
//     let leader_elector = LeaderElector::new(committee());
//     MockMempool::run(rx_consensus_mempool);
//     let mempool_driver = MempoolDriver::new(tx_consensus_mempool);
//     let synchronizer = Synchronizer::new(
//         name,
//         committee(),
//         store.clone(),
//         /* network_channel */ tx_network.clone(),
//         /* core_channel */ tx_core.clone(),
//         parameters.sync_retry_delay,
//     )
//     .await;
//     let mut core = Core::new(
//         name,
//         committee(),
//         parameters,
//         signature_service,
//         pk_set,
//         store,
//         leader_elector,
//         mempool_driver,
//         synchronizer,
//         /* core_channel */ rx_core,
//         tx_core.clone(),
//         /* network_channel */ tx_network,
//         /* commit_channel */ tx_commit,
//         false,
//         true,
//     );
//     tokio::spawn(async move {
//         core.run_epoch().await;
//     });
//     (tx_core, rx_network, rx_commit)
// }

// fn leader_keys(height: SeqNumber) -> (PublicKey, SecretKey) {
//     let leader_elector = LeaderElector::new(committee());
//     let leader = leader_elector.get_leader(height);
//     keys()
//         .into_iter()
//         .find(|(public_key, _)| *public_key == leader)
//         .unwrap()
// }

// #[tokio::test]
// async fn handle_spb_proposal() {
//     // Make a block and the vote we expect to receive.
//     let (public_key, secret_key) = leader_keys(1);

//     let proof = SPBProof {
//         height: 1,
//         phase: INIT_PHASE,
//         round: 1,
//         shares: Vec::new(),
//     };
//     let block = block();
//     let value = SPBValue::new(block.clone(), 1, INIT_PHASE).await;

//     // Run a core instance.
//     let store_path = ".db_test_handle_proposal";
//     let (tx_core, mut rx_network, _rx_commit) = core(public_key, secret_key, store_path).await;

//     // Send a block to the core.
//     let message = ConsensusMessage::SPBPropose(value.clone(), proof);
//     tx_core.send(message).await.unwrap();

//     // Ensure we get a vote back.
//     match rx_network.recv().await {
//         Some((message, mut recipient)) => match message {
//             ConsensusMessage::SPBVote(vote) => {
//                 assert_eq!(vote.hash, value.digest());
//                 let address = committee().address(&block.author).unwrap();
//                 assert_eq!(recipient, vec![address]);
//             }
//             ConsensusMessage::SPBPropose(_, _) => {
//                 let mut address = committee().broadcast_addresses(&public_key);
//                 recipient.sort();
//                 address.sort();
//                 assert_eq!(recipient, address);
//             }
//             _ => assert!(false),
//         },
//         _ => assert!(false),
//     }
// }

// // #[tokio::test]
// // async fn generate_lock() {
// //     // Make a block and the vote we expect to receive.
// //     let (public_key, secret_key) = leader_keys(1);
// //     let tss_keys = SecretShare::default();
// //     let block = block();
// //     let value = SPBValue::new(block.clone(), 1, INIT_PHASE).await;
// //     let votes: Vec<_> = keys()
// //         .iter()
// //         .filter(|(pb, _)| pb != &public_key)
// //         .map(|(public_key, secret_key)| {
// //             SPBVote::new(
// //                 value.clone(),
// //                 public_key.clone(),
// //                 SignatureService::new(
// //                     secret_key.clone(),
// //                     Some(tss_keys.secret.clone().into_inner()),
// //                 ),
// //             )
// //             .await;
// //         })
// //         .collect();
// //     // Run a core instance.
// //     let store_path = ".db_test_handle_proposal";
// //     let (tx_core, mut rx_network, _rx_commit) = core(public_key, secret_key, store_path).await;

// //     for vote in &votes {}

// //     // Ensure we get a vote back.
// //     match rx_network.recv().await {
// //         Some((message, mut recipient)) => match message {
// //             ConsensusMessage::SPBVote(vote) => {
// //                 assert_eq!(vote.hash, value.digest());
// //                 let address = committee().address(&block.author).unwrap();
// //                 assert_eq!(recipient, vec![address]);
// //             }
// //             ConsensusMessage::SPBPropose(_, _) => {
// //                 let mut address = committee().broadcast_addresses(&public_key);
// //                 recipient.sort();
// //                 address.sort();
// //                 assert_eq!(recipient, address);
// //             }
// //             _ => assert!(false),
// //         },
// //         _ => assert!(false),
// //     }
// // }

// // #[tokio::test]
// // async fn commit_block() {
// //     // Get enough distinct leaders to form a quorum.
// //     let leaders = vec![leader_keys(1), leader_keys(2), leader_keys(3)];
// //     let chain = chain(leaders);

// //     // Run a core instance.
// //     let store_path = ".db_test_commit_block";
// //     let (public_key, secret_key) = keys().pop().unwrap();
// //     let (tx_core, _rx_network, mut rx_commit) = core(public_key, secret_key, store_path).await;

// //     // Send a the blocks to the core.
// //     let committed = chain[0].clone();
// //     for block in chain {
// //         let message = ConsensusMessage::HsPropose(block);
// //         tx_core.send(message).await.unwrap();
// //     }

// //     // Ensure the core commits the head.
// //     match rx_commit.recv().await {
// //         Some(b) => assert_eq!(b, committed, "commit"),
// //         _ => assert!(false),
// //     }
// // }
