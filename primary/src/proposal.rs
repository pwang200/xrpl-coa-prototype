use serde::{Deserialize, Serialize};
use crypto::PublicKey;

#[derive(Debug, Serialize, Deserialize)]
pub struct Proposal {

}

impl Proposal {
    pub fn node_id(&self) -> PublicKey {
        todo!()
    }
}