use rustyraftlab::raft::Role;
use rustyraftlab::simulator::Simulator;
use rustyraftlab::state_machine::Command;

#[test]
fn three_node_cluster_elects_a_leader() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);

    let leader = sim
        .run_until_leader(500)
        .expect("cluster should elect a leader within 500 ticks");

    assert!(sim.has_stable_leader());
    assert_eq!(sim.node(leader).role, Role::Leader);

    for &id in &[1, 2, 3] {
        if id != leader {
            assert_eq!(sim.node(id).role, Role::Follower);
        }
    }
}

#[test]
fn staggered_timeouts_elect_first_node_in_three_node_cluster() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);

    let leader = sim.run_until_leader(500).expect("leader should be elected");

    // Node 1 has the earliest election deadline (150 + 1*10).
    assert_eq!(leader, 1);
}

#[test]
fn partition_leaves_isolated_node_without_leadership() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    sim.partition(1, 2);
    sim.partition(1, 3);

    sim.run_ticks(500);

    assert_ne!(sim.node(1).role, Role::Leader, "node 1 cannot reach a quorum");
    assert!(
        sim.has_stable_leader(),
        "nodes 2 and 3 should elect a leader among themselves"
    );
    let leader = sim.leaders()[0];
    assert!(leader == 2 || leader == 3);
}

#[test]
fn healing_partition_allows_isolated_node_to_catch_up() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    sim.partition(1, 2);
    sim.partition(1, 3);
    sim.run_ticks(200);

    sim.heal(1, 2);
    sim.heal(1, 3);

    let leader = sim
        .run_until_leader(500)
        .expect("cluster should elect after heal");

    assert_eq!(sim.node(leader).role, Role::Leader);
}

#[test]
fn propose_replicates_and_applies_on_all_nodes() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.propose_command(leader, Command::set("cmd1", "value1"))
        .expect("proposal should succeed");

    assert!(sim.cluster_state_matches());
    for &id in &[1, 2, 3] {
        assert_eq!(sim.node(id).commit_index, 1);
        assert_eq!(sim.node(id).last_applied, 1);
        assert_eq!(sim.node(id).state_machine.get("cmd1"), Some("value1"));
    }
}

#[test]
fn cluster_reaches_agreement_after_multiple_commands() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.propose_command(leader, Command::set("a", "1"))
        .expect("first proposal");
    sim.propose_command(leader, Command::set("b", "2"))
        .expect("second proposal");
    sim.propose_command(leader, Command::delete("a"))
        .expect("third proposal");

    assert!(sim.cluster_state_matches());
    for &id in &[1, 2, 3] {
        assert_eq!(sim.node(id).commit_index, 3);
        assert_eq!(sim.node(id).last_applied, 3);
        assert_eq!(sim.node(id).state_machine.get("a"), None);
        assert_eq!(sim.node(id).state_machine.get("b"), Some("2"));
        assert_eq!(
            sim.node(id).state_machine.applied_commands(),
            &[
                Command::set("a", "1"),
                Command::set("b", "2"),
                Command::delete("a"),
            ]
        );
    }
}

#[test]
fn periodic_heartbeats_maintain_leadership_over_long_run() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    sim.run_ticks(1_000);

    assert_eq!(sim.leaders(), vec![leader]);
    for &id in &[1, 2, 3] {
        if id != leader {
            assert_eq!(sim.node(id).role, Role::Follower);
        }
    }
}

#[test]
fn initial_heartbeats_keep_followers_from_electing_immediately() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    let leader = sim.run_until_leader(500).expect("leader elected");

    // Initial heartbeats reset follower timers; no new election within one timeout window.
    sim.run_ticks(50);

    assert_eq!(sim.leaders(), vec![leader]);
    for &id in &[1, 2, 3] {
        if id != leader {
            assert_eq!(sim.node(id).role, Role::Follower);
        }
    }
}

#[test]
fn partitioned_node_catches_up_state_after_heal() {
    let mut sim = Simulator::new(&[1, 2, 3], 150);
    sim.run_until_leader(500).expect("initial leader");

    sim.partition(1, 2);
    sim.partition(1, 3);
    sim.run_ticks(500);

    let leader = [2, 3]
        .into_iter()
        .find(|&id| sim.node(id).role == Role::Leader)
        .expect("majority partition should elect leader among nodes 2 and 3");

    sim.propose_command(leader, Command::set("k", "v"))
        .expect("majority should commit");

    sim.heal(1, 2);
    sim.heal(1, 3);
    sim.run_ticks(300);

    assert!(sim.cluster_state_matches());
    for &id in &[1, 2, 3] {
        assert_eq!(sim.node(id).state_machine.get("k"), Some("v"));
    }
}
