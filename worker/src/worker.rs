// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, TransactionData};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Executor, Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{error, info, warn};
use network::{MessageHandler, Writer};
use primary::PrimaryWorkerMessage;
use serde::{Deserialize, Serialize};
use std::error::Error;
use rand::Rng;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use crate::{Transaction};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
const SIG_CHANNEL_CAPACITY: usize = 250_000;
const FANOUT: usize = crate::FANOUT * 2;
pub const CHANNEL_CAPACITY: usize = SIG_CHANNEL_CAPACITY * FANOUT;
/// The primary round number.
// TODO: Move to the primary.
pub type Round = u64;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// The message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

pub struct Worker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        // Define a worker instance.
        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        worker.handle_primary_messages();
        worker.handle_clients_transactions(tx_primary.clone());
        worker.handle_workers_messages(tx_primary);

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
        );

        // NOTE: This log entry is used to compute performance.
        info!(
            "Worker {} successfully booted on {}",
            id,
            worker
                .committee
                .worker(&worker.name, &worker.id)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from our primary.
    fn handle_primary_messages(&self) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);
        let (tx_executor, rx_executor) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from our primary.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        network::Receiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler { tx_synchronizer, tx_executor},
        );

        // The `Synchronizer` is responsible to keep the worker in sync with the others. It handles the commands
        // it receives from the primary (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_synchronizer,
        );

        Executor::spawn(self.store.clone(), rx_executor);

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        let mut tx_sig_verifiers = Vec::new();
        for tag in 0..FANOUT {
            let (tx_sig, rx_sig) = channel(SIG_CHANNEL_CAPACITY);
            tokio::task::spawn(sig_verify(tag, rx_sig, tx_batch_maker.clone()));
            tx_sig_verifiers.push(tx_sig);
        }
        //  let sig_verifiers = SigVerifiersChannels::new_and_spawn(tx_batch_maker);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        network::Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_sig_verifiers },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            /* workers_addresses */
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other workers.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        network::Receiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
            },
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // This `Processor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ tx_primary,
            /* own_batch */ false,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_sig_verifiers: Vec<Sender<Bytes>>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        // let tx : crate::Transaction = bincode::deserialize(&message).unwrap();
        // let good_sig = tx.verify();
        // debug!("SigTestVerify {}", good_sig);
        let idx: u8 = rand::thread_rng().gen();
        // let tx = message.to_vec();
        // let idx = tx[tx.len() - 10] as usize % FANOUT;
        self.tx_sig_verifiers[idx as usize % FANOUT]
            .send(message)
            .await
            .expect("Failed to send transaction");

        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<SerializedBatchMessage>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(..)) => self
                .tx_processor
                .send(serialized.to_vec())
                .await
                .expect("Failed to send batch"),
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
    tx_executor: Sender<Vec<Digest>>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize the message and send it to the synchronizer.
        match bincode::deserialize(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => {
                match message {
                    PrimaryWorkerMessage::Synchronize(_, _) => {
                        self
                            .tx_synchronizer
                            .send(message)
                            .await
                            .expect("Failed to send transaction");
                    }
                    PrimaryWorkerMessage::Cleanup(_) => {}
                    PrimaryWorkerMessage::Execute(batches) => {
                        self
                            .tx_executor.send(batches)
                            .await
                            .expect("Failed to send batches");
                    }
                }
            }
        }
        Ok(())
    }
}

async fn sig_verify(tag: usize, mut rx_sig_verifier: Receiver<Bytes>, tx_batch_maker: Sender<TransactionData>) {
    let mut count: u64 = 0;
    let mut now = Instant::now();
    loop {
        let message = rx_sig_verifier.recv().await.unwrap();
        let tx_data = message.to_vec();
        // format : u8, u64, u8 (serialized tx len), serialized tx, padding
        let payload_len: u8 = bincode::deserialize(&tx_data[(1+8) as usize .. (1+8+1) as usize]).unwrap();
        let tx: Transaction = bincode::deserialize(&tx_data[(1+8+1) as usize .. (1+8+1+payload_len) as usize]).unwrap();
        let good_sig = tx.verify();
        // info!("tx sig {}", good_sig);
        tx_batch_maker.send(tx_data).await.expect("Failed to send transaction");
        count += 1;
        if count % 10_000 == 0 {
            info!("tx sig verify, tag {}, count {}, time {:?}", tag, count, now.elapsed().as_millis());
            now = Instant::now();
        }
    }
}