use crate::datastore::{tx_data::TxData, TxOffset};
use super::*;

use std::{
    fs::File,
    io::{Read, Write},
    sync::{Arc, Mutex},
};
use tokio::{
    fs::{File as AsyncFile, OpenOptions as AsyncOpenOptions},
    io::AsyncWriteExt,
    sync::mpsc,
};
struct LogEntry {
    tx_offset: TxOffset,
    tx_data: TxData,
}

impl LogEntry {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.tx_offset.0.to_le_bytes())?;
        self.tx_data.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        let tx_offset = TxOffset(u64::from_le_bytes(bytes));
        let tx_data = TxData::deserialize(reader)?;
        Ok(LogEntry { tx_offset, tx_data })
    }
}

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
        todo!()
    }
}
