use crate::datastore::{tx_data::TxData, TxOffset};
use super::*;

use crate::datastore::{tx_data::TxData, TxOffset};
use self::example_durability::ExampleDurabilityLayer;

/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    // TODO
    append_tx: Option<mpsc::Sender<LogEntry>>,
    durable_offset: Arc<Mutex<Option<TxOffset>>>,
    log_file: Arc<Mutex<Option<AsyncFile>>>,
    filepath: String,
}

impl OmniPaxosDurability {
    pub fn new(filepath: &str) -> Self {
        OmniPaxosDurability {
            append_tx: None,
            durable_offset: Arc::new(Mutex::new(None)),
            log_file: Arc::new(Mutex::new(None)),
            filepath: filepath.to_string(),
        }
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        todo!()
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        todo!()
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        todo!()
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
    }
}
