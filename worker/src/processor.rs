// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{SerializedBatchDigestMessage, WorkerMessage};
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use log::{error, info, warn};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use bytes::Bytes;
use crate::batch_maker::Batch;
use crate::Transaction;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                // Hash the batch.
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

                // Store the batch.
                store.write(digest.to_vec(), batch).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}

pub struct Executor;

impl Executor {
    pub fn spawn(
        mut store: Store,
        mut rx_batch: Receiver<Vec<Digest>>,
    ) {
        tokio::spawn(async move {
            let mut count = 0u32;
            while let Some(batches) = rx_batch.recv().await {
                for hash in batches{
                    match store.read(hash.to_vec()).await {
                        Ok(None) => {
                            warn!("DB don't have batch {}", hash);
                        },
                        Ok(Some(tx_data)) => {
                            let bs = Bytes::from(tx_data);
                            match bincode::deserialize(&*bs).expect("Failed to deserialize batch"){
                                WorkerMessage::Batch(batch) => {
                                    for tx in batch{
                                        let payload_len: u8 = bincode::deserialize(&tx[(1+8) as usize .. (1+8+1) as usize]).unwrap();
                                        let t: Transaction = bincode::deserialize(&tx[(1+8+1) as usize .. (1+8+1+payload_len) as usize]).unwrap();
                                        //info!("batch {:?}, tx sig {}", hash, t.verify());
                                        store.write(t.get_key(), t.get_value()).await;
                                        count += 1;
                                        if count % 10000 == 0 {
                                            info!("count {}", count);
                                        }
                                    }
                                },
                                _ => panic!("Unexpected message"),
                            }
                        },
                        Err(e) => {
                            error!("{}", e);
                        }
                    }
                }
            }
        });
    }
}