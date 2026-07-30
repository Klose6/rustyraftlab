//! Raft correctness properties exercised through the deterministic simulator.

use rustyraftlab::raft::{LogEntry, Role};
use rustyraftlab::simulator::Simulator;
use rustyraftlab::state_machine::Command;

fn assert_agreement(sim: &Simulator) {
    assert!(
        sim.cluster_state_matches(),
        "live nodes should have identical state machines"
    );
    assert!(
        sim.logs_satisfy_matching_property(),
        "logs should satisfy the log-matching property"
    );
    assert!(
        sim.committed_prefix_matches(),
        "committed indices should match across live nodes"
    );
}

fn live_leader(sim: &Simulator) -> u64 {
    sim.leaders()
        .into_iter()
        .next()
        .expect("expected a live leader")
}

#[test]
fn election_safety_never_two_live_leaders() {
    let mut sim = Simulator::new(&[1, 2, 3, 4, 5], 150);

    for _ in 0..1_000 {
        sim.tick();
        assert!(
            sim.leaders().len() <= 1,
            "at most one live leader at any tick"
        );
    }
}

#[test]
fn log_matching_holds_after_replicated_proposals() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.propose_and_settle(leader, Command::set("a", "1"))
        .expect("first proposal");
    sim.propose_and_settle(leader, Command::set("b", "2"))
        .expect("second proposal");

    assert!(sim.logs_satisfy_matching_property());
    assert_agreement(&sim);
}

#[test]
fn committed_entries_survive_leader_replacement() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.propose_and_settle(leader, Command::set("persist", "yes"))
        .expect("commit before crash");

    let committed = sim.node(leader).commit_index;
    assert!(committed >= 1);

    sim.crash(leader);
    sim.run_ticks(500);

    let new_leader = live_leader(&sim);
    sim.propose_and_settle(new_leader, Command::set("after", "election"))
        .expect("commit after new leader");

    sim.restart(leader);
    sim.run_ticks(400);

    assert_agreement(&sim);
    for &id in &[1, 2, 3] {
        assert_eq!(sim.node(id).state_machine.get("persist"), Some("yes"));
        assert_eq!(sim.node(id).state_machine.get("after"), Some("election"));
    }
}

#[test]
fn minority_partition_cannot_commit_without_quorum() {
    let mut sim = Simulator::new(&[1, 2, 3, 4, 5], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.propose_and_settle(leader, Command::set("baseline", "1"))
        .expect("baseline commit");

    // Isolate a two-node minority.
    sim.partition(1, 3);
    sim.partition(1, 4);
    sim.partition(1, 5);
    sim.partition(2, 3);
    sim.partition(2, 4);
    sim.partition(2, 5);

    sim.run_ticks(500);

    let majority_leader = [3, 4, 5]
        .into_iter()
        .find(|&id| sim.node(id).role == Role::Leader);
    assert!(
        majority_leader.is_some(),
        "majority component should elect a leader"
    );

    let majority_leader = majority_leader.unwrap();
    sim.propose_and_settle(majority_leader, Command::set("majority", "ok"))
        .expect("majority should commit");

    for &id in &[3, 4, 5] {
        assert_eq!(sim.node(id).state_machine.get("majority"), Some("ok"));
    }

    // Minority nodes must not have applied the majority-only write.
    for &id in &[1, 2] {
        assert_eq!(sim.node(id).state_machine.get("majority"), None);
    }
}

#[test]
fn figure8_conflicting_uncommitted_entry_is_overwritten() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.propose_and_settle(leader, Command::set("key", "committed"))
        .expect("shared committed entry");

    // A follower diverges with an uncommitted entry from an old term.
    let divergent = [1u64, 2, 3]
        .into_iter()
        .find(|&id| id != leader)
        .expect("follower exists");
    sim.node_mut(divergent).term = sim.node(divergent).term.max(2);
    sim.node_mut(divergent).log.push(LogEntry {
        term: 2,
        data: Command::set("key", "stale").encode(),
    });

    sim.crash(leader);
    sim.run_ticks(500);

    let new_leader = [1, 2, 3]
        .into_iter()
        .find(|&id| !sim.is_crashed(id) && sim.node(id).role == Role::Leader)
        .expect("cluster should elect a new leader");

    sim.propose_and_settle(new_leader, Command::set("key", "winner"))
        .expect("new leader overwrites conflict");

    if sim.is_crashed(leader) {
        sim.restart(leader);
        sim.run_ticks(300);
    }

    assert_agreement(&sim);
    for &id in &[1, 2, 3] {
        assert_eq!(sim.node(id).state_machine.get("key"), Some("winner"));
    }
}

#[test]
fn sequential_leaders_produce_linearizable_history() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let mut expected = Vec::new();

    let first = sim.run_until_leader(500).expect("first leader");
    sim.propose_and_settle(first, Command::set("x", "1"))
        .expect("write under first leader");
    expected.push(Command::set("x", "1"));

    sim.crash(first);
    sim.run_ticks(500);

    let second = live_leader(&sim);
    sim.propose_and_settle(second, Command::set("y", "2"))
        .expect("write under second leader");
    expected.push(Command::set("y", "2"));

    // Bring the first leader back so two live nodes can still form a majority.
    sim.restart(first);
    sim.run_ticks(200);

    sim.crash(second);
    sim.run_ticks(500);

    let third = live_leader(&sim);
    sim.propose_and_settle(third, Command::set("x", "3"))
        .expect("write under third leader");
    expected.push(Command::set("x", "3"));

    if sim.is_crashed(first) {
        sim.restart(first);
    }
    if sim.is_crashed(second) {
        sim.restart(second);
    }
    sim.run_ticks(500);

    assert_agreement(&sim);
    for &id in &[1, 2, 3] {
        assert_eq!(sim.node(id).state_machine.applied_commands(), expected);
        assert_eq!(sim.node(id).state_machine.get("x"), Some("3"));
        assert_eq!(sim.node(id).state_machine.get("y"), Some("2"));
    }
}

#[test]
fn agreement_holds_under_delay_and_drops() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.set_message_delay(10);
    sim.set_drop_every_nth(Some(4));

    for index in 0..5 {
        sim.propose_command(leader, Command::set(format!("k{index}"), "v"))
            .expect("proposal");
        sim.run_ticks(120);
    }

    assert_agreement(&sim);
    for index in 0..5 {
        for &id in &[1, 2, 3] {
            assert_eq!(
                sim.node(id).state_machine.get(&format!("k{index}")),
                Some("v")
            );
        }
    }
}

#[test]
fn restarted_minority_catches_up_without_forking_state() {
    let mut sim = Simulator::new(&[1, 2, 3, 4, 5], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.propose_and_settle(leader, Command::set("shared", "1"))
        .expect("initial write");

    sim.crash(1);
    sim.crash(2);
    sim.run_ticks(200);

    let majority_leader = live_leader(&sim);
    sim.propose_and_settle(majority_leader, Command::set("shared", "2"))
        .expect("majority write while minority is down");

    sim.restart(1);
    sim.restart(2);
    sim.run_ticks(600);

    assert_agreement(&sim);
    for id in 1..=5 {
        assert_eq!(sim.node(id).state_machine.get("shared"), Some("2"));
    }
}
