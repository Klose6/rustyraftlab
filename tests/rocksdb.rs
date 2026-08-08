use rustyraftlab::rocksdb::{RocksDbEngine, StorageEngine};
use rustyraftlab::state_machine::Command;
use tempfile::TempDir;

fn open_test_engine() -> (TempDir, RocksDbEngine) {
    let dir = TempDir::new().expect("temp dir");
    let engine = RocksDbEngine::open(dir.path()).expect("open rocksdb");
    (dir, engine)
}

#[test]
fn write_read_delete_round_trip() {
    let (_dir, engine) = open_test_engine();

    assert_eq!(engine.read("foo").expect("read"), None);

    engine.write("foo", b"bar").expect("write");
    assert_eq!(engine.read("foo").expect("read"), Some(b"bar".to_vec()));

    engine.delete("foo").expect("delete");
    assert_eq!(engine.read("foo").expect("read"), None);
}

#[test]
fn save_survives_reopen() {
    let dir = TempDir::new().expect("temp dir");
    {
        let engine = RocksDbEngine::open(dir.path()).expect("open");
        engine.write("persist", b"yes").expect("write");
        engine.save().expect("save");
    }

    let engine = RocksDbEngine::open(dir.path()).expect("reopen");
    assert_eq!(
        engine.read("persist").expect("read"),
        Some(b"yes".to_vec())
    );
}

#[test]
fn apply_command_updates_store() {
    let (_dir, engine) = open_test_engine();

    engine
        .apply_command(&Command::set("k", "v"))
        .expect("set");
    assert_eq!(engine.read("k").expect("read"), Some(b"v".to_vec()));

    engine
        .apply_command(&Command::delete("k"))
        .expect("delete");
    assert_eq!(engine.read("k").expect("read"), None);
}
