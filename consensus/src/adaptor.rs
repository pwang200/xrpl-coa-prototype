use std::time::SystemTime;
use xrpl_consensus_validations::Adaptor;
use primary::Ledger;
use xrpl_consensus_core::{Ledger as LedgerTrait, LedgerIndex, WallNetClock};
use crypto::PublicKey;

pub struct ValidationsAdaptor {

}

impl ValidationsAdaptor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Adaptor for ValidationsAdaptor {
    type ValidationType = Validation;
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

#[derive(Copy, Clone)]
pub struct Validation {

}

impl xrpl_consensus_core::Validation for Validation {
    type LedgerIdType = <Ledger as LedgerTrait>::IdType;

    fn seq(&self) -> LedgerIndex {
        todo!()
    }

    fn ledger_id(&self) -> Self::LedgerIdType {
        todo!()
    }

    fn sign_time(&self) -> SystemTime {
        todo!()
    }

    fn seen_time(&self) -> SystemTime {
        todo!()
    }

    fn cookie(&self) -> u64 {
        todo!()
    }

    fn trusted(&self) -> bool {
        todo!()
    }

    fn full(&self) -> bool {
        todo!()
    }

    fn load_fee(&self) -> Option<u32> {
        todo!()
    }
}