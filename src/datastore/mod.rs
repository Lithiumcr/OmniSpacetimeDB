pub(crate) mod error;
pub mod example_datastore;
pub mod tx_data;

use self::error::DatastoreError;
use self::tx_data::{TxData, TxResult};
use crate::durability::DurabilityLevel;

#[derive(Debug, Copy, Clone)]
pub struct TableId(pub u32);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxOffset(pub u64);

pub trait Datastore<K, V> {
    type Tx;
    type MutTx;

    // Transaction methods
    fn begin_tx(&self, durability_level: DurabilityLevel) -> Self::Tx;
    fn release_tx(&self, tx: Self::Tx);

    fn begin_mut_tx(&self) -> Self::MutTx;
    fn rollback_mut_tx(&self, tx: Self::MutTx);
    fn commit_mut_tx(&self, tx: Self::MutTx) -> Result<TxResult, DatastoreError>;

    // Data methods
    fn get_mut_tx(&self, tx: &mut Self::MutTx, key: &K) -> Option<V>;
    fn set_mut_tx(&self, tx: &mut Self::MutTx, key: K, value: V);
    fn delete_mut_tx(&self, tx: &mut Self::MutTx, key: &K) -> Option<V>;

    fn get_tx(&self, tx: &mut Self::Tx, key: &K) -> Option<V>;

    // Durability methods
    fn advance_replicated_durability_offset(
        &self,
        tx_offset: TxOffset,
    ) -> Result<(), DatastoreError>;
    fn rollback_to_replicated_durability_offset(&self) -> Result<(), DatastoreError>;
    fn get_replicated_offset(&self) -> Option<TxOffset>;
    fn get_cur_offset(&self) -> Option<TxOffset>;

    // Init methods
    fn replay_transaction(&self, data: &TxData) -> Result<(), DatastoreError>;
}
