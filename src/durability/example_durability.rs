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

use crate::datastore::{tx_data::TxData, TxOffset};

use super::DurabilityLayer;

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

#[derive(Clone)]
pub struct ExampleDurabilityLayer {
    tx: Option<mpsc::Sender<LogEntry>>,
    durable_offset: Arc<Mutex<Option<TxOffset>>>,
    log_file: Arc<Mutex<Option<AsyncFile>>>,
    filepath: String,
}

impl ExampleDurabilityLayer {
    pub fn new(filepath: &str) -> Self {
        ExampleDurabilityLayer {
            tx: None,
            durable_offset: Arc::new(Mutex::new(None)),
            log_file: Arc::new(Mutex::new(None)),
            filepath: filepath.to_string(),
        }
    }

    pub async fn open_log_file(filepath: &str) -> AsyncFile {
        AsyncOpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath)
            .await
            .expect("Failed to open log file")
    }

    pub fn close_log_file(&mut self) {
        // Close the log file.
        self.log_file = Arc::new(Mutex::new(None));
    }

    async fn file_write_task(
        mut rx: mpsc::Receiver<LogEntry>,
        out_tx: mpsc::Sender<TxOffset>,
        filepath: String,
    ) {
        let mut file = AsyncFile::create(&filepath)
            .await
            .expect("Failed to open file");

        while let Some(request) = rx.recv().await {
            // Serialize your data to a Vec<u8> as needed
            let mut src = Vec::new();
            // Assuming you have a serialize method
            request
                .serialize(&mut src)
                .expect("Failed to serialize data");

            // Perform the write operation
            file.write_all(&src).await.expect("Failed to write to file");
            file.flush().await.expect("Failed to flush file");
            file.sync_all().await.expect("Failed to sync file");
            out_tx
                .send(request.tx_offset)
                .await
                .expect("Failed to send write request");
        }
    }
}

impl DurabilityLayer for ExampleDurabilityLayer {
    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        // This is where the durability layer would append the transaction to the log.
        // For the example durability layer, we will be writing the TxData to an append only file.

        if self.tx.is_none() {
            let (tx, rx) = tokio::sync::mpsc::channel::<LogEntry>(64);
            let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<TxOffset>(64);
            let filepath = self.filepath.clone();
            tokio::spawn(Self::file_write_task(rx, out_tx, filepath));
            let durable_offset = self.durable_offset.clone();
            tokio::spawn(async move {
                while let Some(offset) = out_rx.recv().await {
                    let mut durable_offset = durable_offset.lock().unwrap();
                    *durable_offset = Some(offset);
                }
            });
            self.tx = Some(tx);
        }

        let tx = self.tx.as_ref().cloned().unwrap();
        tokio::spawn(async move {
            tx.send(LogEntry { tx_offset, tx_data })
                .await
                .expect("Failed to send write request");
        });
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        let mut durable_offset = self.durable_offset.lock().unwrap();
        if let Some(offset) = durable_offset.as_ref() {
            // TODO: It's silly to read the whole log to get this number
            return *offset;
        }
        let offset = self
            .iter()
            .last()
            .map(|(tx_offset, _)| tx_offset)
            .unwrap_or(TxOffset(0));
        *durable_offset = Some(offset);
        offset
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // This is where the durability layer would iterate over the log.
        // For the example durability layer, we will be reading the TxData from the log file.

        // Open the log file for reading.
        let mut file = File::open(&self.filepath).expect("Failed to open log file");

        // Create an iterator that reads the log file and deserializes the transactions.
        Box::new(std::iter::from_fn(move || {
            match LogEntry::deserialize(&mut file) {
                Ok(log_entry) => Some((log_entry.tx_offset, log_entry.tx_data)),
                Err(_) => None,
            }
        }))
    }

    fn iter_starting_from_offset(
        &self,
        _offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::{Runtime, Builder};
    use crate::datastore::{tx_data::InsertList, self};
    use super::*;
    use std::{sync::Arc, time::Duration};

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    const WAIT_TIMEOUT: Duration = Duration::from_secs(1);

    #[test]
    fn test_append_tx() {
        let tmp_dir = tempdir::TempDir::new("test_append_tx").unwrap();
        let path = tmp_dir.path().join("log.bin");
        let tx_offset = TxOffset(0);
        let durability_layer = ExampleDurabilityLayer::new(path.to_str().unwrap());
        let runtime = create_runtime();
        let mut dl = durability_layer.clone();
        runtime.spawn(async move {
            let tx_data = TxData {
                inserts: Arc::new([InsertList {
                    table_id: datastore::TableId(0),
                    inserts: Arc::new([]),
                }]),
                deletes: Arc::new([]),
                truncs: Arc::new([]),
            };
            dl.append_tx(tx_offset, tx_data);
        });
        std::thread::sleep(WAIT_TIMEOUT);   // wait for tx1 to be replicated...

        let mut iter = durability_layer.iter();
        let (actual_tx_offset, _actual_tx_data) = iter.next().unwrap();
        assert_eq!(actual_tx_offset.0, tx_offset.0);
        assert!(iter.next().is_none());
    }
}
