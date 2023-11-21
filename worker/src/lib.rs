// Copyright(C) Facebook, Inc. and its affiliates.
mod batch_maker;
mod helper;
mod primary_connector;
mod processor;
mod quorum_waiter;
mod synchronizer;
mod worker;

//use ed25519_dalek::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use crypto::{PublicKey, SecretKey, Hash, Signature};

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

pub use crate::worker::Worker;
pub const FANOUT: usize = 4;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Transaction {
    d1: u64,
    d2: u64,
    //pad: [u128; 16],
    pub public_key: PublicKey,
    pub sig: Signature,
}

impl Transaction {
    pub fn new(d1: u64,
               d2: u64,
               public_key: PublicKey,
               secret_key: &SecretKey,
    ) -> Self {
        //let pad: [u128; 16] = Default::default();
        let d = bincode::serialize(&(d1, d2, public_key)).unwrap().as_slice().digest();
        let sig = Signature::new(d.to_vec(), secret_key);
        // let sig = Default::default();
        Self { d1, d2, public_key, sig }
    }

    pub fn verify(&self)-> bool {
        // true
        let d = bincode::serialize(&(self.d1, self.d2, self.public_key)).unwrap().as_slice().digest();
        self.sig.verify(&d, &self.public_key).is_ok()
    }

    pub fn get_key(&self) -> Vec<u8> {
        (self.d2 & 0xffffu64).to_be_bytes().to_vec()
    }

    pub fn get_value(&self) -> Vec<u8> {
        self.d2.to_be_bytes().to_vec()
    }
}