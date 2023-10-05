use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use xrpl_consensus_core::{Ledger as LedgerTrait, NetClock, WallNetClock};
use xrpl_consensus_validations::Adaptor;

use crypto::PublicKey;
use primary::{Ledger, SignedValidation};

pub struct ValidationsAdaptor {
    clock: Arc<RwLock<WallNetClock>>,
    tx_ledger_master: Sender<(<Self as Adaptor>::LedgerIdType, oneshot::Sender<Option<<Self as Adaptor>::LedgerType>>)>
}

impl ValidationsAdaptor {
    pub fn new(
        clock: Arc<RwLock<WallNetClock>>,
        tx_ledger_master: Sender<(<Self as Adaptor>::LedgerIdType, oneshot::Sender<Option<<Self as Adaptor>::LedgerType>>)>
    ) -> Self {
        Self { clock, tx_ledger_master }
    }
}

#[async_trait]
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

    async fn acquire(&mut self, ledger_id: &Self::LedgerIdType) -> Option<Self::LedgerType> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.tx_ledger_master.send((*ledger_id, sender)).await {
            panic!("Failed to send message to LedgerMaster");
        }

        receiver
            .await
            .expect("Failed to receive response from LedgerMaster")
    }
}