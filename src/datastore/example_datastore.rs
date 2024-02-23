use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock, RwLock,
};
use std::{collections::HashMap, sync::Arc};

use crate::durability::DurabilityLevel;

use super::{
    tx_data::{
        serde::{read_varint, write_varint},
        DeleteList, InsertList, RowData, TxData, TxResult,
    },
    Datastore, TxOffset,
};

type SharedWriteGuard<T> = ArcRwLockWriteGuard<RawRwLock, T>;
type SharedReadGuard<T> = ArcRwLockReadGuard<RawRwLock, T>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExampleRow {
    key: String,
    value: String,
}

use std::io::{self, Read, Write};

// Assuming write_varint and read_varint are already implemented

// Serialize an ExampleRow into a vector of bytes
fn serialize_example_row(row: &ExampleRow) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    // Serialize `key`
    write_varint(&mut bytes, row.key.len() as u32)?;
    bytes.write_all(row.key.as_bytes())?;
    // Serialize `value`
    write_varint(&mut bytes, row.value.len() as u32)?;
    bytes.write_all(row.value.as_bytes())?;
    Ok(bytes)
}

// Deserialize an ExampleRow from a slice of bytes
fn deserialize_example_row(bytes: &[u8]) -> io::Result<ExampleRow> {
    let mut cursor = io::Cursor::new(bytes);

    // Deserialize `key`
    let key_len = read_varint(&mut cursor)? as usize;
    let mut key_bytes = vec![0u8; key_len];
    cursor.read_exact(&mut key_bytes)?;
    let key = String::from_utf8(key_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 for key"))?;

    // Deserialize `value`
    let value_len = read_varint(&mut cursor)? as usize;
    let mut value_bytes = vec![0u8; value_len];
    cursor.read_exact(&mut value_bytes)?;
    let value = String::from_utf8(value_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 for value"))?;

    Ok(ExampleRow { key, value })
}

pub struct ExampleDatastore {
    committed_state: Arc<RwLock<CommittedState>>,
}

impl ExampleDatastore {
    pub fn new() -> Self {
        ExampleDatastore {
            committed_state: Arc::new(RwLock::new(CommittedState {
                next_tx_offset: TxOffset(0),
                replicated_tx_offset: None,
                replicated_state: HashMap::new(),
                committed_diff: HashMap::new(),
            })),
        }
    }
}

impl Default for ExampleDatastore {
    fn default() -> Self {
        Self::new()
    }
}

struct TxDiff {
    history: Vec<(TxOffset, Diff)>,
}

impl TxDiff {
    fn get_latest(&self) -> &(TxOffset, Diff) {
        self.history.last().unwrap()
    }

    fn insert_diff(&mut self, offset: TxOffset, diff: Diff) {
        self.history.push((offset, diff))
    }
}

#[derive(Debug, Clone)]
enum Diff {
    Insert(String),
    Delete,
}

struct CommittedState {
    next_tx_offset: TxOffset,
    replicated_tx_offset: Option<TxOffset>,
    replicated_state: HashMap<String, String>,
    committed_diff: HashMap<String, TxDiff>,
}

impl CommittedState {
    fn get(&self, key: &String, durability_level: &DurabilityLevel) -> Option<String> {
        match durability_level {
            DurabilityLevel::Memory => {
                if let Some(tx_diff) = self.committed_diff.get(key) {
                    let (_, diff) = tx_diff.get_latest();
                    match diff {
                        Diff::Insert(v) => return Some(v.clone()),
                        Diff::Delete => return None,
                    }
                }
            }
            DurabilityLevel::Replicated => {}
        }
        return self.replicated_state.get(key).cloned();
    }

    fn advance_replicated_durability_offset(
        &mut self,
        tx_offset: super::TxOffset,
    ) -> Result<(), super::error::DatastoreError> {
        for (k, tx_diff) in &mut self.committed_diff {
            while !tx_diff.history.is_empty() {
                // TODO: This is all a bit silly.
                // This should binary search to the latest
                // offset less than or equal to the durability offset
                let (offset, diff) = tx_diff.history.remove(0);
                if offset.0 <= tx_offset.0 {
                    match diff {
                        Diff::Insert(value) => {
                            self.replicated_state.insert(k.clone(), value);
                        }
                        Diff::Delete => {
                            self.replicated_state.remove(k);
                        }
                    }
                } else {
                    break;
                }
            }
        }
        self.committed_diff
            .retain(|_, tx_diff| !tx_diff.history.is_empty());
        self.replicated_tx_offset = Some(tx_offset);
        Ok(())
    }

    /// In order to rollback all changes that are ahead of the replicated state, all we need
    /// to do is to clear the committed_diff.
    fn rollback_to_replicated_durability_offset(
        &mut self,
    ) -> Result<(), super::error::DatastoreError> {
        self.committed_diff = HashMap::new();
        self.next_tx_offset = self.replicated_tx_offset.map_or(TxOffset(0), |replicated| TxOffset(replicated.0 + 1));
        Ok(())
    }
}

pub struct Tx {
    durability_level: DurabilityLevel,
    committed_state: SharedReadGuard<CommittedState>,
}

impl Tx {
    pub fn get(&self, key: &String) -> Option<String> {
        self.committed_state.get(key, &self.durability_level)
    }
}

pub struct MutTx {
    diff: HashMap<String, Diff>,
    committed_state: SharedWriteGuard<CommittedState>,
}

impl MutTx {
    pub fn get(&self, key: &String) -> Option<String> {
        match self.diff.get(key) {
            Some(Diff::Insert(v)) => return Some(v.clone()),
            Some(Diff::Delete) => return None,
            None => {}
        }
        self.committed_state.get(key, &DurabilityLevel::Memory)
    }

    pub fn set(&mut self, key: String, value: String) {
        self.diff.insert(key, Diff::Insert(value));
    }

    pub(crate) fn delete(&mut self, key: &String) -> Option<String> {
        let old = match self.diff.remove(key) {
            Some(Diff::Insert(v)) => Some(v),
            Some(Diff::Delete) => None,
            None => self.committed_state.get(key, &DurabilityLevel::Memory),
        };
        self.diff.insert(key.clone(), Diff::Delete);
        old
    }
}

impl Datastore<String, String> for ExampleDatastore {
    type Tx = Tx;
    type MutTx = MutTx;

    fn begin_tx(&self, durability_level: crate::durability::DurabilityLevel) -> Self::Tx {
        Tx {
            durability_level,
            committed_state: self.committed_state.read_arc(),
        }
    }

    fn release_tx(&self, _tx: Self::Tx) {
        // Do nothing, just drop Tx
    }

    fn begin_mut_tx(&self) -> Self::MutTx {
        MutTx {
            diff: HashMap::new(),
            committed_state: self.committed_state.write_arc(),
        }
    }

    fn rollback_mut_tx(&self, _tx: Self::MutTx) {
        // Do nothing, just drop MutTx
    }

    fn commit_mut_tx(
        &self,
        mut tx: Self::MutTx,
    ) -> Result<super::tx_data::TxResult, super::error::DatastoreError> {
        let offset = tx.committed_state.next_tx_offset;
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        for (key, diff) in tx.diff {
            match tx.committed_state.committed_diff.get_mut(&key) {
                Some(tx_diff) => tx_diff.insert_diff(offset, diff.clone()),
                None => {
                    let mut tx_diff = TxDiff {
                        history: Vec::new(),
                    };
                    tx_diff.insert_diff(offset, diff.clone());
                    tx.committed_state
                        .committed_diff
                        .insert(key.clone(), tx_diff);
                }
            }
            match diff {
                Diff::Insert(value) => inserts.push(ExampleRow { key, value }),
                Diff::Delete => deletes.push(key),
            }
        }
        let tx_data = TxData {
            inserts: Arc::new([InsertList {
                table_id: super::TableId(0),
                inserts: inserts
                    .iter()
                    .map(|s| RowData(Arc::from(serialize_example_row(s).unwrap())))
                    .collect::<Arc<_>>(),
            }]),
            deletes: Arc::new([DeleteList {
                table_id: super::TableId(0),
                deletes: deletes
                    .iter()
                    .map(|s| RowData(Arc::from(s.as_bytes())))
                    .collect::<Arc<_>>(),
            }]),
            truncs: Arc::new([]),
        };
        tx.committed_state.next_tx_offset.0 += 1;
        Ok(TxResult {
            tx_offset: offset,
            tx_data,
        })
    }

    fn get_mut_tx(&self, tx: &mut Self::MutTx, key: &String) -> Option<String> {
        tx.get(key)
    }

    fn set_mut_tx(&self, tx: &mut Self::MutTx, key: String, value: String) {
        tx.set(key, value);
    }

    fn delete_mut_tx(&self, tx: &mut Self::MutTx, key: &String) -> Option<String> {
        tx.delete(key)
    }

    fn get_tx(&self, tx: &mut Self::Tx, key: &String) -> Option<String> {
        tx.get(key)
    }

    fn advance_replicated_durability_offset(
        &self,
        tx_offset: super::TxOffset,
    ) -> Result<(), super::error::DatastoreError> {
        let mut committed_state = self.committed_state.write();
        committed_state.advance_replicated_durability_offset(tx_offset)
    }

    fn rollback_to_replicated_durability_offset(&self) -> Result<(), super::error::DatastoreError> {
        let mut committed_state = self.committed_state.write();
        committed_state.rollback_to_replicated_durability_offset()
    }

    fn replay_transaction(&self, data: &TxData) -> Result<(), super::error::DatastoreError> {
        let mut committed_state = self.committed_state.write();
        // You should not be able to replay a transaction on top of a state that is ahead of the replicated state
        match committed_state.replicated_tx_offset {
            Some(offset) => {
                if committed_state.next_tx_offset.0 != offset.0 + 1 {
                    return Err(super::error::DatastoreError::InvalidDurabilityOffset);
                }
            }
            None => {
                if committed_state.next_tx_offset != TxOffset(0) {
                    return Err(super::error::DatastoreError::InvalidDurabilityOffset);
                }
            }
        }
        committed_state.replicated_tx_offset = Some(
            committed_state
                .replicated_tx_offset
                .map_or(TxOffset(0), |val| TxOffset(val.0 + 1)),
        );
        committed_state.next_tx_offset.0 += 1;

        for insert_list in data.inserts.iter() {
            assert!(
                insert_list.table_id.0 == 0,
                "This datastore is only capable of storing one table."
            );
            for insert in insert_list.inserts.iter() {
                let row = deserialize_example_row(&insert.0)?;
                committed_state.replicated_state.insert(row.key, row.value);
            }
        }
        for delete_list in data.deletes.iter() {
            assert!(
                delete_list.table_id.0 == 0,
                "This datastore is only capable of storing one table."
            );
            for delete in delete_list.deletes.iter() {
                let key = std::str::from_utf8(&delete.0)?;
                committed_state.replicated_state.remove(key);
            }
        }
        Ok(())
    }

    /// Warning: This will deadlock if used on the same thread as an open mutable transaction
    fn get_replicated_offset(&self) -> Option<TxOffset> {
        self.committed_state.read().replicated_tx_offset
    }

    /// Warning: This will deadlock if used on the same thread as an open mutable transaction
    fn get_cur_offset(&self) -> Option<TxOffset> {
        if self.committed_state.read().next_tx_offset == TxOffset(0) {
            None
        } else {
            Some(TxOffset(self.committed_state.read().next_tx_offset.0 - 1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example_datastore() {
        let datastore = ExampleDatastore::new();
        let mut tx = datastore.begin_mut_tx();

        assert_eq!(tx.get(&"foo".to_string()), None);

        tx.set("foo".to_string(), "bar".to_string());
        assert_eq!(tx.get(&"foo".to_string()), Some("bar".to_string()));

        tx.delete(&"foo".to_string());
        assert_eq!(tx.get(&"foo".to_string()), None);

        let result = datastore.commit_mut_tx(tx).unwrap();

        assert_eq!(result.tx_offset.0, 0);
        assert_eq!(result.tx_data.inserts.len(), 1);
        assert_eq!(result.tx_data.deletes.len(), 1);
        assert_eq!(result.tx_data.truncs.len(), 0);
        assert_eq!(result.tx_data.inserts[0].inserts.len(), 0);
        assert_eq!(result.tx_data.deletes[0].deletes.len(), 1);
        assert_eq!(result.tx_data.deletes[0].deletes[0].0.as_ref(), b"foo");
        assert_eq!(result.tx_data.inserts[0].table_id.0, 0);
    }

    #[test]
    /// This test first executes a set of transactions in datastore 1 and then
    /// creates a new datastore, datastore 2, which calls replay_transaction to
    /// replay their effects them in datastore 2, checking at the end that the
    /// contents of the datastores are the same.
    fn test_example_datastore_replay() {
        let datastore1 = ExampleDatastore::new();
        let mut tx1 = datastore1.begin_mut_tx();
        tx1.set("foo".to_string(), "bar".to_string());
        let result1 = datastore1.commit_mut_tx(tx1).unwrap();

        let mut tx2 = datastore1.begin_mut_tx();
        tx2.set("bar".to_string(), "baz".to_string());
        tx2.delete(&"foo".to_string());
        let result2 = datastore1.commit_mut_tx(tx2).unwrap();

        let datastore2 = ExampleDatastore::new();
        datastore2.replay_transaction(&result1.tx_data).unwrap();
        datastore2.replay_transaction(&result2.tx_data).unwrap();

        let tx2 = datastore2.begin_tx(DurabilityLevel::Memory);
        assert_eq!(tx2.get(&"foo".to_string()), None);
        assert_eq!(tx2.get(&"bar".to_string()), Some("baz".to_string()));
        datastore2.release_tx(tx2);
    }

    #[test]
    /// This test tests that the effects of transactions whose offsets are not
    /// greater than the replicated durability offset do not yet show up when
    /// querying the datastore through a read only transaction.
    fn text_example_datastore_replicated_durability() {
        let datastore = ExampleDatastore::new();

        // The replicated durability offset is initially None
        assert!(datastore.get_cur_offset().is_none());

        // We create a transaction and commit it
        let mut tx = datastore.begin_mut_tx();
        tx.set("foo".to_string(), "bar".to_string());
        let result = datastore.commit_mut_tx(tx).unwrap();

        // The replicated durability offset is still None
        // but the current offset is 0
        assert_eq!(datastore.get_cur_offset().unwrap(), TxOffset(0));
        assert!(datastore.get_replicated_offset().is_none());

        // We advance the replicated durability offset to 0
        datastore
            .advance_replicated_durability_offset(result.tx_offset)
            .unwrap();
        assert_eq!(datastore.get_replicated_offset().unwrap(), TxOffset(0));

        // We verify that the effects of the transaction have become visible
        // at the replicated durability level
        let tx = datastore.begin_tx(DurabilityLevel::Replicated);
        assert_eq!(tx.get(&"foo".to_string()), Some("bar".to_string()));
        datastore.release_tx(tx);

        // We delete the key and commit the transaction
        let mut tx = datastore.begin_mut_tx();
        tx.delete(&"foo".to_string());
        let result = datastore.commit_mut_tx(tx).unwrap();

        // The replicated durability offset is still 0
        assert_eq!(
            datastore
                .committed_state
                .read()
                .replicated_tx_offset
                .unwrap()
                .0,
            0
        );

        // The deletion is not yet visible at the replicated durability level
        let tx = datastore.begin_tx(DurabilityLevel::Replicated);
        assert_eq!(tx.get(&"foo".to_string()), Some("bar".to_string()));
        datastore.release_tx(tx);

        // The deletion is visible at the memory durability level
        let tx = datastore.begin_tx(DurabilityLevel::Memory);
        assert_eq!(tx.get(&"foo".to_string()), None);
        datastore.release_tx(tx);

        // We advance the replicated durability offset to 1 and now the deletion
        // becomes visible at the replicated durability level
        datastore
            .advance_replicated_durability_offset(result.tx_offset)
            .unwrap();
        let tx = datastore.begin_tx(DurabilityLevel::Replicated);
        assert_eq!(tx.get(&"foo".to_string()), None);
        datastore.release_tx(tx);
    }

    #[test]
    /// This test tests that replay transactions are only allowed to be replayed
    /// on top of a state that is at the replicated durability offset.
    fn test_example_datastore_replay_invalid_offset() {
        let datastore = ExampleDatastore::new();

        // We create a transaction and commit it
        let mut tx = datastore.begin_mut_tx();
        tx.set("foo".to_string(), "bar".to_string());
        let result = datastore.commit_mut_tx(tx).unwrap();

        // We advance the replicated durability offset to 0
        datastore
            .advance_replicated_durability_offset(result.tx_offset)
            .unwrap();

        // We create a new datastore and try to replay the transaction
        let datastore2 = ExampleDatastore::new();
        let result = datastore2.replay_transaction(&result.tx_data);
        assert!(result.is_ok());

        // We create a new transaction and commit it
        let mut tx = datastore.begin_mut_tx();
        tx.set("foo".to_string(), "baz".to_string());
        let result = datastore.commit_mut_tx(tx).unwrap();

        // We advance the replicated durability offset to 1
        datastore
            .advance_replicated_durability_offset(result.tx_offset)
            .unwrap();

        // We create a new transaction in datastore 2 and commit it
        let mut tx = datastore2.begin_mut_tx();
        tx.set("foo".to_string(), "bar".to_string());
        let result = datastore2.commit_mut_tx(tx).unwrap();

        // We try to replay the transaction on top of a state that is ahead of the replicated durability offset
        let result = datastore2.replay_transaction(&result.tx_data);
        assert!(result.is_err());
    }
}
