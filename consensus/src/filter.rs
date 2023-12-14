use crate::config::Parameters;
use crate::core::ConsensusMessage;
use bytes::Bytes;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::NetMessage;
use rand::Rng;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

pub type FilterInput = (ConsensusMessage, Vec<SocketAddr>);

pub struct Filter;

impl Filter {
    pub fn run(
        mut core: Receiver<FilterInput>,
        mut core_smvba: Receiver<FilterInput>,
        network: Sender<NetMessage>,
        net_smvba: Sender<NetMessage>,
        parameters: Parameters,
    ) {
        tokio::spawn(async move {
            let mut pending = FuturesUnordered::new();
            let mut pending_smvba = FuturesUnordered::new();
            loop {
                tokio::select! {
                    Some(input) = core.recv() => pending.push(Self::delay(input, parameters.clone())),
                    Some(input) = core_smvba.recv() => pending_smvba.push(Self::delay(input, parameters.clone())),
                    Some(input) = pending.next() => Self::transmit(input, &network).await,
                    Some(input) = pending_smvba.next() => Self::transmit(input, &net_smvba).await,
                    else => break
                }
            }
        });
    }

    async fn transmit(input: FilterInput, network: &Sender<NetMessage>) {
        let (message, addresses) = input;
        let bytes = bincode::serialize(&message).expect("Failed to serialize core message");
        let net_message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = network.send(net_message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
    }

    async fn delay(input: FilterInput, parameters: Parameters) -> FilterInput {
        let (message, _) = &input;
        if let ConsensusMessage::HsPropose(_) = message {
            // NOTE: Increase the delay here (you can use any value from the 'parameters').
            // Only add network delay for non-fallback block proposals
            if parameters.random_ddos
                && rand::thread_rng().gen_bool((parameters.random_ddos_chance as f64) / 100.0)
            {
                sleep(Duration::from_millis(parameters.network_delay)).await;
            } else if parameters.ddos {
                sleep(Duration::from_millis(parameters.network_delay)).await;
            }
        }
        input
    }
}
