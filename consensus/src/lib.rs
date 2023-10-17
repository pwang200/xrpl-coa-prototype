use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use log::{error, info, warn};
use rand::RngCore;
use rand::rngs::OsRng;
use tokio::sync::mpsc::{Receiver, Sender};
use xrpl_consensus_core::{Ledger as LedgerTrait, NetClock};
use xrpl_consensus_validations::{Adaptor, ValidationParams, Validations};
use xrpl_consensus_validations::arena_ledger_trie::ArenaLedgerTrie;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use primary::{ConsensusPrimaryMessage, PrimaryConsensusMessage, SignedValidation, Validation};
use primary::Ledger;
use primary::proposal::{ConsensusRound, Proposal, SignedProposal};

use crate::adaptor::ValidationsAdaptor;

pub mod adaptor;

pub const INITIAL_WAIT: Duration = Duration::from_secs(2);

pub enum ConsensusState {
    // TODO: If the primary is taking care of preferred branch selection and dealing with
    //  syncing and switching if we are not on the preferred branch, do we really need this
    //  here, or should the primary keep track of if it's synced or not?
    NotSynced,
    InitialWait(SystemTime),
    Deliberating,
    Executing,
}

pub struct Consensus {
    /// The UNL information.
    committee: Committee,
    node_id: PublicKey,
    /// The last `Ledger` we have validated.
    latest_ledger: Ledger,
    ///
    round: ConsensusRound,
    clock: Arc<RwLock<<ValidationsAdaptor as Adaptor>::ClockType>>,
    state: ConsensusState,
    proposals: HashMap<PublicKey, Arc<SignedProposal>>,
    batch_pool: HashSet<(Digest, WorkerId)>,
    validations: Validations<ValidationsAdaptor, ArenaLedgerTrie<Ledger>>,
    validation_cookie: u64,
    signature_service: SignatureService,

    rx_primary: Receiver<PrimaryConsensusMessage>,
    tx_primary: Sender<ConsensusPrimaryMessage>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        node_id: PublicKey,
        signature_service: SignatureService,
        adaptor: ValidationsAdaptor,
        clock: Arc<RwLock<<ValidationsAdaptor as Adaptor>::ClockType>>,
        rx_primary: Receiver<PrimaryConsensusMessage>,
        tx_primary: Sender<ConsensusPrimaryMessage>,
    ) {
        tokio::spawn(async move {
            let mut rng = OsRng {};
            let now = clock.read().unwrap().now();
            Self {
                committee,
                node_id,
                latest_ledger: Ledger::make_genesis(),
                round: 0.into(),
                clock: clock.clone(),
                state: ConsensusState::InitialWait(now),
                proposals: Default::default(),
                batch_pool: HashSet::new(),
                validations: Validations::new(ValidationParams::default(), adaptor, clock),
                validation_cookie: rng.next_u64(),
                signature_service,
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
                PrimaryConsensusMessage::Timeout => {
                    info!("Received Timeout event.");
                    self.on_timeout().await;
                }
                PrimaryConsensusMessage::Batch(batch) => {
                    // Store any batches that come from the primary in batch_pool to be included
                    // in a future proposal.
                    // info!("Received batch {:?}.", batch.0);
                    self.batch_pool.insert(batch);
                }
                PrimaryConsensusMessage::Proposal(proposal) => {
                    // info!("Received proposal: {:?}", proposal);
                    self.on_proposal_received(proposal);
                }
                PrimaryConsensusMessage::SyncedLedger(synced_ledger) => {
                    // info!("Received SyncedLedger.");
                    self.validations.adaptor_mut().add_ledger(synced_ledger);
                }
                PrimaryConsensusMessage::Validation(validation) => {
                    info!("Consensus Received validation : {:?}.", validation);
                    self.process_validation(validation).await;
                }
            }
        }
    }

    async fn on_timeout(&mut self) {
        if let Some((preferred_seq, preferred_id)) = self.validations.get_preferred(&self.latest_ledger) {
            info!("Preferred branch returned something.");
            if preferred_id != self.latest_ledger.id() {
                info!("We're not on the right branch.");
                warn!(
                    "Not on preferred ledger. We are on {:?} and preferred is {:?}",
                    (self.latest_ledger.id(), self.latest_ledger.seq()),
                    (preferred_id, preferred_seq)
                );

                self.latest_ledger = self.validations.adaptor_mut().acquire(&preferred_id).await
                    .expect("ValidationsAdaptor did not have preferred ledger in cache.");

            }
        }

        match self.state {
            ConsensusState::NotSynced => {
                info!("NotSynced. Doing nothing.");
                // do nothing
            }
            ConsensusState::Executing => {
                info!("Executing. Doing nothing.");
                // do nothing
            }
            ConsensusState::InitialWait(wait_start) => {
                info!("InitialWait. Checking if we should propose.");

                if self.now().duration_since(wait_start).unwrap() > INITIAL_WAIT {
                    info!("We should propose so we are.");
                    // If we're in the InitialWait state and we've waited longer than the configured
                    // initial wait time, make a proposal.
                    self.propose_first().await;
                }

                // else keep waiting
            }
            ConsensusState::Deliberating => {
                info!("Deliberating. Reproposing.");
                self.re_propose().await;
            }
        }
    }

    fn now(&self) -> SystemTime {
        self.clock.read().unwrap().now()
    }

    async fn propose_first(&mut self) {
        self.state = ConsensusState::Deliberating;

        // TODO: Consider redesigning the batch pool to deal with batches that were disputed
        //  and don't make it to the next round of proposals. Currently, if a batch does not make
        //  it to the next proposal round because not enough validators had the batch in their proposal,
        //  that batch will be dropped and will never make it into a validated ledger. Ideally,
        //  that batch should be queued for the next ledger's consensus process.
        let batch_set = self.batch_pool.drain().collect();
        self.propose(batch_set).await;
    }

    async fn re_propose(&mut self) {
        if self.check_consensus() {
            info!("We have consensus!");
            self.build_ledger().await;
        } else {
            info!("We don't have consensus :(");
            // threshold is the percentage of UNL members who need to propose the same set of batches
            let threshold = self.round.threshold();
            info!("Threshold: {:?}", threshold);
            // This is the number of UNL members who need to propose the same set of batches based
            // on the threshold percentage.
            let num_nodes_threshold = (self.committee.authorities.len() as f32 * threshold).ceil() as u32;
            info!("Num nodes needed: {:?}", num_nodes_threshold);

            // This will build a HashMap of (Digest, WorkerId) -> number of validators that proposed it,
            // then filter that HashMap to the (Digest, WorkerId)s that have a count > num_nodes_threshold
            // and collect that into a new proposal set.
            let new_proposal_set: HashSet<(Digest, WorkerId)> = self.proposals.iter()
                .map(|v| v.1)
                .filter(|v| v.proposal.parent_id == self.latest_ledger.id)
                .flat_map(|v| v.proposal.batches.iter())
                .fold(HashMap::<(Digest, WorkerId), u32>::new(), |mut map, digest| {
                    *map.entry(*digest).or_default() += 1;
                    map
                })
                .into_iter()
                .filter(|(_, count)| *count > num_nodes_threshold)
                .map(|(digest, _)| digest)
                .collect();

            self.propose(new_proposal_set).await;
        }
    }

    async fn propose(&mut self, batch_set: HashSet<(Digest, WorkerId)>) {
        let proposal = Proposal::new(
            self.round,
            self.latest_ledger.id(),
            self.latest_ledger.seq() + 1,
            batch_set,
            self.node_id,
        );

        info!("Proposing              {:?}", proposal);
        let signed_proposal = Arc::new(proposal.sign(&mut self.signature_service).await);
        self.proposals.insert(self.node_id, signed_proposal.clone());
        self.tx_primary.send(ConsensusPrimaryMessage::Proposal(signed_proposal)).await
            .expect("Could not send proposal to primary.");
        self.round.next();
    }

    fn on_proposal_received(&mut self, proposal: SignedProposal) {
        info!("Received new proposal: {:?}", proposal.proposal);
        // The Primary will check the signature and make sure the proposal comes from
        // someone in our UNL before sending it to Consensus, therefore we do not need to
        // check here again. Additionally, the Primary will delay sending us a proposal until
        // it has synced all of the batches that it does not have in its local storage.
        if proposal.proposal.parent_id == self.latest_ledger.id() {
            // Either insert the proposal if we haven't seen a proposal from this node,
            // or update an existing node's proposal if the given proposal's round is higher
            // than what we have in our map.
            match self.proposals.entry(proposal.proposal.node_id) {
                Entry::Occupied(mut e) => {
                    if e.get().proposal.round < proposal.proposal.round {
                        e.insert(Arc::new(proposal));
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(Arc::new(proposal));
                }
            }
        }
    }

    fn check_consensus(&self) -> bool {
        // Find our proposal
        let our_proposal = self.proposals.get(&self.node_id)
            .expect("We did not propose anything the first round.");

        // Determine the number of nodes that need to agree with our proposal to reach consensus
        // by multiplying the number of validators in our UNL by 0.80 and taking the ceiling.
        let num_nodes_for_threshold = (self.committee.authorities.len() as f32 * 0.80).ceil() as usize;

        // Determine how many proposals have the same set of batches as us.
        let num_matching_sets = self.proposals.iter()
            .filter(|p| p.1.proposal.batches == our_proposal.proposal.batches)
            .count();

        // If 80% or more of UNL nodes proposed the same batch set, we have reached consensus,
        // otherwise we need another round.
        num_matching_sets >= num_nodes_for_threshold
    }

    async fn build_ledger(&mut self) {
        self.state = ConsensusState::Executing;

        let new_ledger = self.execute();

        let validation = Validation::new(
            new_ledger.seq(),
            new_ledger.id(),
            self.clock.read().unwrap().now(),
            self.clock.read().unwrap().now(),
            self.node_id,
            self.node_id,
            true,
            true,
            self.validation_cookie,
        );

        let signed_validation = validation.sign(&mut self.signature_service).await;

        // Need to add the new ledger to our cache before adding it to self.validations because
        // self.validations.try_add will call Adaptor::acquire, which needs to have the ledger
        // in its cache or else it will panic.
        self.validations.adaptor_mut().add_ledger(new_ledger.clone());

        if let Err(e) = self.validations.try_add(&self.node_id, &signed_validation).await {
            error!("{:?} could not be added. Error: {:?}", signed_validation, e);
            return;
        }

        self.tx_primary.send(ConsensusPrimaryMessage::Validation(signed_validation)).await
            .expect("Failed to send validation to Primary.");

        self.latest_ledger = new_ledger;

        self.tx_primary.send(ConsensusPrimaryMessage::NewLedger(self.latest_ledger.clone())).await
            .expect("Failed to send new ledger to Primary.");

        self.reset();

        info!("Did a new ledger {:?}.", self.latest_ledger);

        #[cfg(feature = "benchmark")]
        for batch in &self.latest_ledger.batch_set {
            info!("Committed {:?} ", batch);
        }
    }

    fn execute(&self) -> Ledger {
        let mut new_ancestors = vec![self.latest_ledger.id()];
        new_ancestors.extend_from_slice(self.latest_ledger.ancestors.as_slice());
        let our_proposal = self.proposals.get(&self.node_id)
            .expect("Could not find our own proposal");

        // TODO: Do we need to store a Vec<Digest> in Ledger and sort batches here so that they
        //  yield the same ID on every validator?
        let batches = our_proposal.proposal.batches.iter()
            .map(|b| b.0)
            .collect();
        Ledger::new(
            self.latest_ledger.seq() + 1,
            new_ancestors,
            batches,
        )
    }

    fn reset(&mut self) {
        self.proposals.clear();
        self.round.reset();
        self.state = ConsensusState::InitialWait(self.now());
    }

    async fn process_validation(&mut self, validation: SignedValidation) {
        info!("Received validation for ({:?}, {:?})", validation.validation.ledger_id, validation.validation.seq);
        if let Err(e) = self.validations.try_add(&validation.validation.node_id, &validation).await {
            error!("{:?} could not be added. Error: {:?}", validation, e);
        }
    }
}