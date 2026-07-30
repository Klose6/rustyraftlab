//! In-memory key-value state machine driven by committed Raft log entries.

use std::collections::HashMap;

/// Client command applied to the replicated state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

/// Error decoding a command from a log entry payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    Empty,
    UnknownTag(u8),
    Truncated,
    InvalidUtf8,
}

impl Command {
    pub fn set(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Set {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn delete(key: impl Into<String>) -> Self {
        Self::Delete { key: key.into() }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Set { key, value } => {
                let mut bytes = vec![1];
                extend_length_prefixed_str(&mut bytes, key);
                extend_length_prefixed_str(&mut bytes, value);
                bytes
            }
            Self::Delete { key } => {
                let mut bytes = vec![2];
                extend_length_prefixed_str(&mut bytes, key);
                bytes
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CommandError> {
        let (tag, rest) = bytes
            .split_first()
            .ok_or(CommandError::Empty)?;

        match *tag {
            1 => {
                let (key, rest) = read_length_prefixed_str(rest)?;
                let (value, rest) = read_length_prefixed_str(rest)?;
                if !rest.is_empty() {
                    return Err(CommandError::Truncated);
                }
                Ok(Self::Set { key, value })
            }
            2 => {
                let (key, rest) = read_length_prefixed_str(rest)?;
                if !rest.is_empty() {
                    return Err(CommandError::Truncated);
                }
                Ok(Self::Delete { key })
            }
            other => Err(CommandError::UnknownTag(other)),
        }
    }
}

/// Deterministic key-value store updated by committed log entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateMachine {
    store: HashMap<String, String>,
    applied_commands: Vec<Command>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, command: &Command) {
        match command {
            Command::Set { key, value } => {
                self.store.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.store.remove(key);
            }
        }
        self.applied_commands.push(command.clone());
    }

    pub fn apply_bytes(&mut self, data: &[u8]) -> Result<(), CommandError> {
        self.apply(&Command::decode(data)?);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.store.get(key).map(String::as_str)
    }

    pub fn store(&self) -> &HashMap<String, String> {
        &self.store
    }

    pub fn applied_commands(&self) -> &[Command] {
        &self.applied_commands
    }
}

fn extend_length_prefixed_str(bytes: &mut Vec<u8>, value: &str) {
    let value_bytes = value.as_bytes();
    bytes.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
    bytes.extend_from_slice(value_bytes);
}

fn read_length_prefixed_str(bytes: &[u8]) -> Result<(String, &[u8]), CommandError> {
    if bytes.len() < 4 {
        return Err(CommandError::Truncated);
    }

    let len = u32::from_be_bytes(bytes[..4].try_into().expect("slice length")) as usize;
    let bytes = &bytes[4..];
    if bytes.len() < len {
        return Err(CommandError::Truncated);
    }

    let (raw, rest) = bytes.split_at(len);
    let value = std::str::from_utf8(raw)
        .map(|s| s.to_owned())
        .map_err(|_| CommandError::InvalidUtf8)?;

    Ok((value, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_set_command() {
        let command = Command::set("foo", "bar");
        let decoded = Command::decode(&command.encode()).expect("decode");
        assert_eq!(decoded, command);
    }

    #[test]
    fn apply_updates_store() {
        let mut sm = StateMachine::new();
        sm.apply(&Command::set("x", "1"));
        sm.apply(&Command::set("y", "2"));
        sm.apply(&Command::delete("x"));

        assert_eq!(sm.get("x"), None);
        assert_eq!(sm.get("y"), Some("2"));
    }
}
