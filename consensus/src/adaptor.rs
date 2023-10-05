use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use xrpl_consensus_core::{Ledger as LedgerTrait, NetClock, WallNetClock};
use xrpl_consensus_validations::Adaptor;

use crypto::PublicKey;
use primary::{Ledger, SignedValidation};

pub struct ValidationsAdaptor {
    clock: Arc<RwLock<WallNetClock>>
}

impl ValidationsAdaptor {
    pub fn new(clock: Arc<RwLock<WallNetClock>>) -> Self {
        Self { clock }
    }
}

impl Adaptor for ValidationsAdaptor {
    type ValidationType = SignedValidation;
    type LedgerType = Ledger;
    type LedgerIdType = <Ledger as LedgerTrait>::IdType;
    type NodeIdType = PublicKey;
    type NodeKeyType = PublicKey;
    type ClockType = WallNetClock;

    fn now(&self) -> SystemTime {
        self.clock.read().unwrap().now()
    }

    fn acquire(&mut self, ledger_id: &Self::LedgerIdType) -> Option<Self::LedgerType> {
        todo!()
    }
}