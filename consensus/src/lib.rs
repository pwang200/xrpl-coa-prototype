pub mod adaptor;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use log::warn;
use tokio::sync::mpsc::{Receiver, Sender};
use xrpl_consensus_core::{Ledger as LedgerTrait, NetClock, WallNetClock};
use xrpl_consensus_validations::arena_ledger_trie::ArenaLedgerTrie;
use xrpl_consensus_validations::ledger_trie::LedgerTrie;
use xrpl_consensus_validations::{Adaptor, ValidationParams, Validations};
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use primary::{ConsensusPrimaryMessage, PrimaryConsensusMessage};
use primary::proposal::Proposal;
use primary::Ledger;
use crate::adaptor::ValidationsAdaptor;

pub const INITIAL_WAIT: Duration = Duration::from_secs(2);

pub enum ConsensusState {
    NotSynced,
    InitialWait(SystemTime),
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
    clock: Arc<RwLock<<ValidationsAdaptor as Adaptor>::ClockType>>,
    state: ConsensusState,
    proposals: HashMap<PublicKey, Proposal>,
    batch_pool: Vec<(Digest, WorkerId)>,
    validations: Validations<ValidationsAdaptor, ArenaLedgerTrie<Ledger>>,

    rx_primary: Receiver<PrimaryConsensusMessage>,
    tx_primary: Sender<ConsensusPrimaryMessage>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        adaptor: ValidationsAdaptor,
        clock: Arc<RwLock<<ValidationsAdaptor as Adaptor>::ClockType>>,
        rx_primary: Receiver<PrimaryConsensusMessage>,
        tx_primary: Sender<ConsensusPrimaryMessage>,
    ) {
        tokio::spawn(async move {
            let now = clock.read().unwrap().now();
            Self {
                committee,
                latest_ledger: Ledger::make_genesis(),
                round: ConsensusRound(0),
                clock: clock.clone(),
                state: ConsensusState::InitialWait(now),
                proposals: Default::default(),
                batch_pool: vec![],
                validations: Validations::new(ValidationParams::default(), adaptor, clock),
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
                PrimaryConsensusMessage::Timeout => { self.on_timeout().await }
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

    async fn on_timeout(&mut self) {
        if let Some((preferred_seq, preferred_id)) = self.validations.get_preferred(&self.latest_ledger) {
            if preferred_id != self.latest_ledger.id() {
                warn!(
                    "Not on preferred ledger. We are on {:?} and preferred is {:?}",
                    (self.latest_ledger.id(), self.latest_ledger.seq()),
                    (preferred_id, preferred_seq)
                );
                self.state = ConsensusState::NotSynced;
                self.tx_primary.send(ConsensusPrimaryMessage::SyncLedger(preferred_id, preferred_seq))
                    .await
                    .expect("Failed to ask primary to sync ledger.");
            }
        } else {
            match self.state {
                ConsensusState::NotSynced => {
                    // do nothing
                }
                ConsensusState::Executing => {
                    // do nothing
                }
                ConsensusState::InitialWait(wait_start) => {
                    if self.now().duration_since(wait_start).unwrap() > INITIAL_WAIT {
                        // If we're in the InitialWait state and we've waited longer than the configured
                        // initial wait time, make a proposal.
                        self.propose();
                    }

                    // else keep waiting
                }
                ConsensusState::Deliberating => {
                    self.re_propose();
                }
            }
        }

    }

    fn now(&self) -> SystemTime {
        self.clock.read().unwrap().now()
    }

    fn propose(&mut self) {
        todo!()
    }

    fn re_propose(&mut self) {
        todo!()
    }

    fn on_proposal_received(&mut self, proposal: Proposal) {
        todo!()
    }
}

