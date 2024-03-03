use super::*;


use crate::datastore::{tx_data::TxData, TxOffset};

use super::DurabilityLayer;
use omnipaxos::macros::Entry;
use omnipaxos::util::LogEntry as OmniPaxosLogEntry;
use omnipaxos::*;
use omnipaxos_storage::memory_storage::MemoryStorage;

#[derive(Clone, Debug, Entry)]
struct Log {
    tx_offset: TxOffset,
    tx_data: TxData,
}

type OmniPaxosLog = OmniPaxos<Log, MemoryStorage<Log>>;

/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    omni_paxos: OmniPaxosLog,
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        self.iter_starting_from_offset(TxOffset(0))
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // do we read to the end of the log or just to the decided index?
        let read_entries: Option<Vec<util::LogEntry<Log>>> =
            self.omni_paxos.read_decided_suffix(offset.0);
        match read_entries {
            None => Box::new(std::iter::empty()), // Return an empty iterator if no entries are found
            Some(entries) => {
                let iter = entries
                .into_iter()
                .map(|entry| match entry {
                    OmniPaxosLogEntry::Decided(log) => (log.tx_offset.clone(), log.tx_data.clone()),
                    _ => panic!("Unexpected log entry type"),
                });
                Box::new(iter)
            }
        }
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        let log = Log { tx_offset, tx_data };
        self.omni_paxos
            .append(log)
            .expect("Failed to append to OmniPaxos log");
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        let durable_tx_offset: u64 = self.omni_paxos.get_decided_idx();
        TxOffset(durable_tx_offset)
    }
}
