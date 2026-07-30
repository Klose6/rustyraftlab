//! Deterministic fault-injection simulator for Raft correctness testing.
//!
//! The simulator owns all nodes, advances a global tick counter, delivers RPC
//! messages through an inbox, and supports partitions, delay, drops, and crashes.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::raft::{Action, Message, PersistentState, ProposeError, RaftNode, Role};
use crate::state_machine::Command;

/// A message waiting in the simulated network.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingMessage {
    from: u64,
    to: u64,
    msg: Message,
    deliver_at: u64,
}

/// In-memory multi-node Raft cluster with deterministic timing and faults.
#[derive(Debug)]
pub struct Simulator {
    tick: u64,
    nodes: HashMap<u64, RaftNode>,
    inbox: VecDeque<PendingMessage>,
    delayed: VecDeque<PendingMessage>,
    /// Undirected pairs of nodes that cannot exchange messages.
    partitions: HashSet<(u64, u64)>,
    /// Nodes that are currently down and ignore RPCs and timers.
    crashed: HashSet<u64>,
    /// Persistent state saved at crash time, restored on restart.
    crash_snapshots: HashMap<u64, PersistentState>,
    /// Ticks to wait before a newly enqueued message becomes deliverable.
    message_delay_ticks: u64,
    /// Deterministically drop every Nth enqueued message (`None` disables drops).
    drop_every_nth: Option<u64>,
    /// Counts enqueued messages for deterministic drops.
    enqueue_count: u64,
}

impl Simulator {
    /// Create a cluster with one node per ID. Election timeouts are staggered by `id * 10`
    /// ticks so nodes do not start elections simultaneously.
    pub fn new(node_ids: &[u64], election_timeout: u64) -> Self {
        let mut nodes = HashMap::new();
        for &id in node_ids {
            let peer_ids: Vec<u64> = node_ids.iter().copied().filter(|&peer| peer != id).collect();
            let timeout = election_timeout + id * 10;
            nodes.insert(id, RaftNode::with_election_timeout(id, peer_ids, timeout));
        }

        Self {
            tick: 0,
            nodes,
            inbox: VecDeque::new(),
            delayed: VecDeque::new(),
            partitions: HashSet::new(),
            crashed: HashSet::new(),
            crash_snapshots: HashMap::new(),
            message_delay_ticks: 0,
            drop_every_nth: None,
            enqueue_count: 0,
        }
    }

    pub fn tick_count(&self) -> u64 {
        self.tick
    }

    pub fn node(&self, id: u64) -> &RaftNode {
        self.nodes.get(&id).expect("unknown node id")
    }

    pub fn node_mut(&mut self, id: u64) -> &mut RaftNode {
        self.nodes.get_mut(&id).expect("unknown node id")
    }

    pub fn node_ids(&self) -> Vec<u64> {
        let mut ids: Vec<_> = self.nodes.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn is_crashed(&self, id: u64) -> bool {
        self.crashed.contains(&id)
    }

    /// Delay all newly enqueued messages by this many ticks.
    pub fn set_message_delay(&mut self, ticks: u64) {
        self.message_delay_ticks = ticks;
    }

    /// Drop every Nth enqueued message deterministically.
    pub fn set_drop_every_nth(&mut self, n: Option<u64>) {
        self.drop_every_nth = n.filter(|&value| value > 0);
    }

    /// IDs of nodes currently in the leader role.
    pub fn leaders(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|(&id, node)| !self.crashed.contains(&id) && node.role == Role::Leader)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Returns true when exactly one live leader exists.
    pub fn has_stable_leader(&self) -> bool {
        self.leaders().len() == 1
    }

    /// Block all messages between two nodes (undirected).
    pub fn partition(&mut self, a: u64, b: u64) {
        self.partitions.insert((a.min(b), a.max(b)));
    }

    /// Restore messaging between two nodes.
    pub fn heal(&mut self, a: u64, b: u64) {
        self.partitions.remove(&(a.min(b), a.max(b)));
    }

    /// Crash a node: save persistent state and stop processing RPCs and timers.
    pub fn crash(&mut self, id: u64) {
        if self.crashed.contains(&id) {
            return;
        }

        let snapshot = self.node(id).snapshot_persistent();
        self.crash_snapshots.insert(id, snapshot);
        self.crashed.insert(id);
    }

    /// Restart a crashed node from its saved persistent state.
    pub fn restart(&mut self, id: u64) {
        assert!(self.crashed.contains(&id), "node must be crashed before restart");

        let persistent = self
            .crash_snapshots
            .remove(&id)
            .expect("missing crash snapshot");
        let peer_ids = self.node(id).peer_ids.clone();
        let election_timeout = self.node(id).election_timeout;

        let mut node = RaftNode::with_election_timeout(id, peer_ids, election_timeout);
        node.restore_persistent(persistent);
        node.reset_volatile_after_crash(self.tick);
        self.nodes.insert(id, node);
        self.crashed.remove(&id);
    }

    /// Propose a command to the cluster through the given leader and deliver replication RPCs.
    pub fn propose_command(&mut self, leader_id: u64, command: Command) -> Result<(), ProposeError> {
        let actions = self.node_mut(leader_id).propose_command(command)?;
        self.enqueue_actions(leader_id, actions);
        self.drain_inbox();
        Ok(())
    }

    /// Returns true if every live node has the same state machine contents.
    pub fn cluster_state_matches(&self) -> bool {
        let live_ids: Vec<_> = self
            .live_node_ids()
            .collect();
        let Some(first) = live_ids.first().copied() else {
            return true;
        };

        let expected = self.node(first).state_machine.clone();
        live_ids
            .iter()
            .skip(1)
            .all(|&id| self.node(id).state_machine == expected)
    }

    /// Returns true if shared log prefixes satisfy the Raft log-matching property.
    pub fn logs_satisfy_matching_property(&self) -> bool {
        let ids: Vec<_> = self.live_node_ids().collect();
        for left in 0..ids.len() {
            for right in (left + 1)..ids.len() {
                let left_node = self.node(ids[left]);
                let right_node = self.node(ids[right]);
                let shared_last = left_node.last_log_index().min(right_node.last_log_index());

                for index in 0..=shared_last {
                    if left_node.log_term_at(index) != right_node.log_term_at(index) {
                        return false;
                    }
                    if left_node.log[index as usize].data != right_node.log[index as usize].data {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Returns true if all live nodes share the same committed prefix and apply index.
    pub fn committed_prefix_matches(&self) -> bool {
        let ids: Vec<_> = self.live_node_ids().collect();
        let Some(first) = ids.first().copied() else {
            return true;
        };

        let expected_commit = self.node(first).commit_index;
        let expected_applied = self.node(first).last_applied;

        ids.iter().all(|&id| {
            self.node(id).commit_index == expected_commit
                && self.node(id).last_applied == expected_applied
        })
    }

    /// Propose a command, then run ticks so replication can settle.
    pub fn propose_and_settle(&mut self, leader_id: u64, command: Command) -> Result<(), ProposeError> {
        self.propose_command(leader_id, command)?;
        self.run_ticks(60);
        Ok(())
    }

    fn live_node_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.node_ids()
            .into_iter()
            .filter(|id| !self.crashed.contains(id))
    }

    /// Propose raw log bytes to the cluster through the given leader.
    pub fn propose(&mut self, leader_id: u64, data: Vec<u8>) -> Result<(), ProposeError> {
        let actions = self.node_mut(leader_id).propose(data)?;
        self.enqueue_actions(leader_id, actions);
        self.drain_inbox();
        Ok(())
    }

    /// Number of messages waiting for a future tick.
    pub fn delayed_message_count(&self) -> usize {
        self.delayed.len()
    }

    /// Advance one tick: release delayed messages, heartbeats, election timers, deliver inbox.
    pub fn tick(&mut self) {
        self.tick += 1;
        self.release_delayed_messages();
        self.process_heartbeats();
        self.process_election_timeouts();
        self.drain_inbox();
    }

    /// Advance multiple ticks.
    pub fn run_ticks(&mut self, count: u64) {
        for _ in 0..count {
            self.tick();
        }
    }

    /// Run until one live leader exists or `max_ticks` is reached.
    pub fn run_until_leader(&mut self, max_ticks: u64) -> Option<u64> {
        for _ in 0..max_ticks {
            self.tick();
            if let [leader] = self.leaders().as_slice() {
                return Some(*leader);
            }
        }
        None
    }

    fn release_delayed_messages(&mut self) {
        let now = self.tick;
        let mut still_delayed = VecDeque::new();

        while let Some(pending) = self.delayed.pop_front() {
            if pending.deliver_at <= now {
                self.inbox.push_back(pending);
            } else {
                still_delayed.push_back(pending);
            }
        }

        self.delayed = still_delayed;
    }

    fn process_heartbeats(&mut self) {
        let now = self.tick;
        let ids = self
            .node_ids()
            .into_iter()
            .filter(|id| !self.crashed.contains(id))
            .collect::<Vec<_>>();

        for id in ids {
            let actions = self.node_mut(id).on_heartbeat_tick(now);
            self.enqueue_actions(id, actions);
        }
    }

    fn process_election_timeouts(&mut self) {
        let now = self.tick;
        let ids = self
            .node_ids()
            .into_iter()
            .filter(|id| !self.crashed.contains(id))
            .collect::<Vec<_>>();

        for id in ids {
            let actions = {
                let node = self.node_mut(id);
                if node.election_timed_out(now) {
                    node.on_election_timeout(now)
                } else {
                    Vec::new()
                }
            };
            self.enqueue_actions(id, actions);
        }
    }

    fn drain_inbox(&mut self) {
        while let Some(pending) = self.inbox.pop_front() {
            if self.should_drop_message(&pending) {
                continue;
            }

            let replies = self.deliver(pending.from, pending.to, pending.msg);
            for reply in replies {
                self.enqueue_message(reply);
            }
        }
    }

    fn should_drop_message(&self, pending: &PendingMessage) -> bool {
        self.crashed.contains(&pending.from)
            || self.crashed.contains(&pending.to)
            || self.is_partitioned(pending.from, pending.to)
    }

    fn deliver(&mut self, from: u64, to: u64, msg: Message) -> Vec<PendingMessage> {
        let now = self.tick;

        match msg {
            Message::RequestVote(request) => {
                let response = self.node_mut(to).handle_request_vote(now, request);
                vec![PendingMessage {
                    from: to,
                    to: from,
                    msg: Message::RequestVoteResponse(response),
                    deliver_at: now,
                }]
            }
            Message::AppendEntries(request) => {
                let response = self.node_mut(to).handle_append_entries(now, request);
                vec![PendingMessage {
                    from: to,
                    to: from,
                    msg: Message::AppendEntriesResponse(response),
                    deliver_at: now,
                }]
            }
            Message::RequestVoteResponse(response) => {
                let actions = self
                    .node_mut(to)
                    .handle_request_vote_response(now, from, response);
                self.enqueue_actions(to, actions);
                Vec::new()
            }
            Message::AppendEntriesResponse(response) => {
                let actions = self
                    .node_mut(to)
                    .handle_append_entries_response(now, from, response);
                self.enqueue_actions(to, actions);
                Vec::new()
            }
        }
    }

    fn enqueue_actions(&mut self, from: u64, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, msg } => {
                    self.enqueue_message(PendingMessage {
                        from,
                        to,
                        msg,
                        deliver_at: self.tick,
                    });
                }
                Action::Apply { .. } => {
                    // Already applied on the node in apply_committed_entries.
                }
            }
        }
    }

    fn enqueue_message(&mut self, mut pending: PendingMessage) {
        self.enqueue_count += 1;
        if self
            .drop_every_nth
            .is_some_and(|n| self.enqueue_count % n == 0)
        {
            return;
        }

        if self.message_delay_ticks > 0 {
            pending.deliver_at = self.tick + self.message_delay_ticks;
            self.delayed.push_back(pending);
        } else {
            self.inbox.push_back(pending);
        }
    }

    fn is_partitioned(&self, a: u64, b: u64) -> bool {
        self.partitions.contains(&(a.min(b), a.max(b)))
    }
}
