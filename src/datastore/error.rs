use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatastoreError {
    #[error("Serialization Error. Transaction rolled back.")]
    Serialization,
    #[error("Error setting durability offset")]
    DurabilityOffsetError,
    #[error("Cannot deserialize insert.")]
    InsertDeserializationError,
    #[error("Cannot deserialize delete.")]
    DeleteDeserializationError,
    #[error("Cannot make a mutable transaction if not the leader.")]
    NotLeader,
    #[error("Cannot replay a transaction on top of a datastore with unreplicated changes.")]
    InvalidDurabilityOffset,
}

impl Default for DatastoreError {
    fn default() -> Self {
        Self::Serialization
    }
}

impl From<std::io::Error> for DatastoreError {
    fn from(_err: std::io::Error) -> Self {
        DatastoreError::InsertDeserializationError
    }
}

impl From<std::str::Utf8Error> for DatastoreError {
    fn from(_err: std::str::Utf8Error) -> Self {
        DatastoreError::DeleteDeserializationError
    }
}
