//! RocksDB-backed persistent key-value storage.

mod engine;

pub use engine::{RocksDbEngine, StorageEngine};

/// Errors returned by the storage engine.
#[derive(Debug)]
pub enum StorageError {
    RocksDb(rocksdb::Error),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RocksDb(err) => write!(f, "rocksdb error: {err}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RocksDb(err) => Some(err),
        }
    }
}

impl From<rocksdb::Error> for StorageError {
    fn from(err: rocksdb::Error) -> Self {
        Self::RocksDb(err)
    }
}
