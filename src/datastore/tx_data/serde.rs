use super::TableId;
use super::{DeleteList, InsertList, RowData, TxData};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{self, Read, Write},
    sync::Arc,
};

// Writes a varint
pub fn write_varint<W: Write>(writer: &mut W, mut value: u32) -> io::Result<()> {
    while value >= 0x80 {
        writer.write_all(&[((value as u8) & 0x7F) | 0x80])?;
        value >>= 7;
    }
    writer.write_all(&[value as u8])
}

// Reads a varint
pub fn read_varint<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut value = 0;
    let mut shift = 0;
    loop {
        let byte = reader.read_u8()?;
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(value)
}

// Helper to write a u32 in big-endian format
fn _write_u32<W: Write>(writer: &mut W, value: u32) -> io::Result<()> {
    writer.write_u32::<BigEndian>(value)
}

// Helper to read a u32 in big-endian format
fn _read_u32<R: Read>(reader: &mut R) -> io::Result<u32> {
    reader.read_u32::<BigEndian>()
}

// Serialize RowData
fn serialize_row_data<W: Write>(writer: &mut W, row_data: &RowData) -> io::Result<()> {
    write_varint(writer, row_data.0.len() as u32)?;
    writer.write_all(&row_data.0)
}

// Deserialize RowData
fn deserialize_row_data<R: Read>(reader: &mut R) -> io::Result<RowData> {
    let len = read_varint(reader)? as usize;
    let mut buffer = vec![0u8; len];
    reader.read_exact(&mut buffer)?;
    Ok(RowData(Arc::from(buffer.into_boxed_slice())))
}

// Serialize InsertList
fn serialize_insert_list<W: Write>(writer: &mut W, insert_list: &InsertList) -> io::Result<()> {
    write_varint(writer, insert_list.table_id.0)?;
    write_varint(writer, insert_list.inserts.len() as u32)?;
    for insert in insert_list.inserts.iter() {
        serialize_row_data(writer, insert)?;
    }
    Ok(())
}

// Deserialize InsertList
fn deserialize_insert_list<R: Read>(reader: &mut R) -> io::Result<InsertList> {
    let table_id = TableId(read_varint(reader)?);
    let num_inserts = read_varint(reader)? as usize;
    let mut inserts = Vec::with_capacity(num_inserts);
    for _ in 0..num_inserts {
        inserts.push(deserialize_row_data(reader)?);
    }
    Ok(InsertList {
        table_id,
        inserts: Arc::from(inserts.into_boxed_slice()),
    })
}

// Serialize DeleteList
fn serialize_delete_list<W: Write>(writer: &mut W, delete_list: &DeleteList) -> io::Result<()> {
    write_varint(writer, delete_list.table_id.0)?;
    write_varint(writer, delete_list.deletes.len() as u32)?;
    for delete in delete_list.deletes.iter() {
        serialize_row_data(writer, delete)?;
    }
    Ok(())
}

// Deserialize DeleteList
fn deserialize_delete_list<R: Read>(reader: &mut R) -> io::Result<DeleteList> {
    let table_id = TableId(read_varint(reader)?);
    let num_deletes = read_varint(reader)? as usize;
    let mut deletes = Vec::with_capacity(num_deletes);
    for _ in 0..num_deletes {
        deletes.push(deserialize_row_data(reader)?);
    }
    Ok(DeleteList {
        table_id,
        deletes: Arc::from(deletes.into_boxed_slice()),
    })
}

impl TxData {
    // Serialize TxData
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Serialize inserts
        write_varint(writer, self.inserts.len() as u32)?;
        for insert_list in self.inserts.iter() {
            serialize_insert_list(writer, insert_list)?;
        }

        // Serialize deletes
        write_varint(writer, self.deletes.len() as u32)?;
        for delete_list in self.deletes.iter() {
            serialize_delete_list(writer, delete_list)?;
        }

        // Serialize truncs
        write_varint(writer, self.truncs.len() as u32)?;
        for &trunc in self.truncs.iter() {
            write_varint(writer, trunc.0)?;
        }

        Ok(())
    }

    // Deserialize TxData
    pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Deserialize inserts
        let num_inserts = read_varint(reader)? as usize;
        let mut inserts = Vec::with_capacity(num_inserts);
        for _ in 0..num_inserts {
            inserts.push(deserialize_insert_list(reader)?);
        }

        // Deserialize deletes
        let num_deletes = read_varint(reader)? as usize;
        let mut deletes = Vec::with_capacity(num_deletes);
        for _ in 0..num_deletes {
            deletes.push(deserialize_delete_list(reader)?);
        }

        // Deserialize truncs
        let num_truncs = read_varint(reader)? as usize;
        let mut truncs = Vec::with_capacity(num_truncs);
        for _ in 0..num_truncs {
            truncs.push(TableId(read_varint(reader)?));
        }

        Ok(TxData {
            inserts: Arc::from(inserts.into_boxed_slice()),
            deletes: Arc::from(deletes.into_boxed_slice()),
            truncs: Arc::from(truncs.into_boxed_slice()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_serialize_empty_tx_data() -> io::Result<()> {
        let tx_data = TxData {
            inserts: Arc::new([]),
            deletes: Arc::new([]),
            truncs: Arc::new([]),
        };

        let mut bytes = Vec::new();
        tx_data.serialize(&mut bytes)?;

        // Assert that the length is 3 bytes
        assert_eq!(bytes.len(), 3);

        // Assert that each byte is zero
        for byte in bytes.iter() {
            assert_eq!(*byte, 0);
        }

        Ok(())
    }

    #[test]
    fn test_serialize_tx_data() -> io::Result<()> {
        let tx_data = TxData {
            inserts: Arc::new([InsertList {
                table_id: TableId(42),
                inserts: Arc::new([]),
            }]),
            deletes: Arc::new([]),
            truncs: Arc::new([]),
        };

        let mut bytes = Vec::new();
        tx_data.serialize(&mut bytes)?;

        let expected_bytes = [1, 42, 0, 0, 0];
        assert_eq!(bytes, expected_bytes);

        Ok(())
    }

    #[test]
    fn test_serialize_tx_data_2() -> io::Result<()> {
        let tx_data = TxData {
            inserts: Arc::new([
                InsertList {
                    table_id: TableId(42),
                    inserts: Arc::new([]),
                },
                InsertList {
                    table_id: TableId(33),
                    inserts: Arc::new([]),
                },
            ]),
            deletes: Arc::new([]),
            truncs: Arc::new([]),
        };

        let mut bytes = Vec::new();
        tx_data.serialize(&mut bytes)?;

        let expected_bytes = [2, 42, 0, 33, 0, 0, 0];
        assert_eq!(bytes, expected_bytes);

        Ok(())
    }

    #[test]
    fn test_serialize_tx_data_3() -> io::Result<()> {
        let tx_data = TxData {
            inserts: Arc::new([
                InsertList {
                    table_id: TableId(42),
                    inserts: Arc::new([RowData(Arc::from([1, 2, 3, 4, 5]))]),
                },
                InsertList {
                    table_id: TableId(33),
                    inserts: Arc::new([RowData(Arc::from([6, 7, 8, 9, 10]))]),
                },
            ]),
            deletes: Arc::new([]),
            truncs: Arc::new([]),
        };

        let mut bytes = Vec::new();
        tx_data.serialize(&mut bytes)?;

        let expected_bytes = [2, 42, 1, 5, 1, 2, 3, 4, 5, 33, 1, 5, 6, 7, 8, 9, 10, 0, 0];
        assert_eq!(bytes, expected_bytes);

        Ok(())
    }
}
