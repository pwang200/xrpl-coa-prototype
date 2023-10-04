use std::time::SystemTime;

use xrpl_consensus_core::{Ledger as LedgerTrait, WallNetClock};
use xrpl_consensus_validations::Adaptor;

use crypto::PublicKey;
use primary::{Ledger, SignedValidation};

pub struct ValidationsAdaptor {

}

impl ValidationsAdaptor {
    pub fn new() -> Self {
        Self {}
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
        todo!()
    }

    fn acquire(&mut self, ledger_id: &Self::LedgerIdType) -> Option<Self::LedgerType> {
        todo!()
    }
}