//! RocksDB storage engine with read, write, delete, and durable save.

use std::path::Path;

use rocksdb::{DB, Options};

use crate::state_machine::Command;

use super::StorageError;

/// Persistent key-value storage used by a Raft node.
pub trait StorageEngine {
    /// Read the value for `key`, if present.
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;

    /// Write `value` at `key`, replacing any existing value.
    fn write(&self, key: &str, value: &[u8]) -> Result<(), StorageError>;

    /// Remove `key` from the store.
    fn delete(&self, key: &str) -> Result<(), StorageError>;

    /// Durably persist buffered writes to disk.
    fn save(&self) -> Result<(), StorageError>;
}

/// RocksDB-backed implementation of [`StorageEngine`].
pub struct RocksDbEngine {
    db: DB,
}

impl RocksDbEngine {
    /// Open or create a RocksDB database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let db = DB::open(&options, path)?;

        Ok(Self { db })
    }

    /// Apply a replicated state-machine command to the local store.
    pub fn apply_command(&self, command: &Command) -> Result<(), StorageError> {
        match command {
            Command::Set { key, value } => self.write(key, value.as_bytes()),
            Command::Delete { key } => self.delete(key),
        }
    }

    fn key_bytes(key: &str) -> &[u8] {
        key.as_bytes()
    }
}

impl StorageEngine for RocksDbEngine {
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.db.get(Self::key_bytes(key))?)
    }

    fn write(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.db.put(Self::key_bytes(key), value)?;
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.db.delete(Self::key_bytes(key))?;
        Ok(())
    }

    fn save(&self) -> Result<(), StorageError> {
        self.db.flush()?;
        Ok(())
    }
}
