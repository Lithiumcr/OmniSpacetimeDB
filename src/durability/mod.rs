pub mod example_durability;
pub mod omnipaxos_durability;

use crate::datastore::{tx_data::TxData, TxOffset};
use std::sync::Arc;

pub enum DurabilityLevel {
    Memory,
    Replicated,
}

#[derive(Debug, Copy, Clone)]
pub struct ObserverId(u64);

pub trait DurabilityLayer {
    /// iterate through the log from start
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>>;
    /// iterate through the log from a given offset
    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>>;
    /// tx_offset doesn't really matter.
    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData);
    /// Periodically notify about the durability watermark i.e., where in the log the entry is guaranteed to be durable.
    fn get_durable_tx_offset(&self) -> TxOffset;
}
