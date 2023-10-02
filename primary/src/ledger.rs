use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};

use xrpl_consensus_core::LedgerIndex;

use crypto::Digest;

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct Ledger {
  // TODO
}

impl Display for Ledger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl xrpl_consensus_core::Ledger for Ledger {
    type IdType = Digest;

    fn id(&self) -> Self::IdType {
        todo!()
    }

    fn seq(&self) -> LedgerIndex {
        todo!()
    }

    fn get_ancestor(&self, seq: LedgerIndex) -> Self::IdType {
        todo!()
    }

    fn make_genesis() -> Self {
        todo!()
    }

    fn mismatch(&self, other: &Self) -> LedgerIndex {
        todo!()
    }
}