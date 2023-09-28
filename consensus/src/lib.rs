use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};
use xrpl_consensus_core::Ledger as LedgerTrait;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use primary::{ConsensusPrimaryMessage, PrimaryConsensusMessage};
use primary::proposal::Proposal;
use crate::ledger::Ledger;

pub mod ledger;

pub enum ConsensusState {
    NotSynced,
    InitialWait,
    Deliberating,
    Executing,
}

pub struct ConsensusRound(u8);

impl ConsensusRound {
    pub fn next(mut self) -> Self {
        self.0 += 1;
        self
    }

    pub fn reset(mut self) -> Self {
        self.0 = 0;
        self
    }

    pub fn threshold(&self) -> f32 {
        match self.0 {
            0 => 0.5,
            1 => 0.65,
            2 => 0.70,
            _ => 0.95
        }
    }
}

pub struct Consensus {
    /// The UNL information.
    committee: Committee,
    /// The last `Ledger` we have validated.
    latest_ledger: Ledger,
    ///
    round: ConsensusRound,
    state: ConsensusState,
    proposals: HashMap<PublicKey, Proposal>,
    batch_pool: Vec<(Digest, WorkerId)>,

    rx_primary: Receiver<PrimaryConsensusMessage>,
    tx_primary: Sender<ConsensusPrimaryMessage>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        rx_primary: Receiver<PrimaryConsensusMessage>,
        tx_primary: Sender<ConsensusPrimaryMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                latest_ledger: Ledger::make_genesis(),
                round: ConsensusRound(0),
                state: ConsensusState::InitialWait,
                proposals: Default::default(),
                batch_pool: vec![],
                rx_primary,
                tx_primary,
            }
                .run()
                .await;
        });
    }

    async fn run(&mut self) {
        while let Some(message) = self.rx_primary.recv().await {
            match message {
                PrimaryConsensusMessage::Timeout => { self.on_timeout() }
                PrimaryConsensusMessage::OwnBatch(batch) => {
                    // Store any batches that come from the primary in batch_pool to be included
                    // in a future proposal.
                    self.batch_pool.push(batch);
                }
                PrimaryConsensusMessage::Proposal(proposal) => {
                    self.on_proposal_received(proposal);
                }
            }
        }
    }

    fn on_timeout(&mut self) {
        todo!()
    }

    fn on_proposal_received(&mut self, proposal: Proposal) {
        todo!()
    }
}

