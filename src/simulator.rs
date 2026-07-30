//! Deterministic fault-injection simulator for Raft correctness testing.
//!
//! The simulator owns all nodes, advances a global tick counter, delivers RPC
//! messages synchronously from an inbox, and optionally blocks links between nodes.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::raft::{Action, Message, ProposeError, RaftNode, Role};
use crate::state_machine::Command;

/// A message waiting in the simulated network.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingMessage {
    from: u64,
    to: u64,
    msg: Message,
}

/// In-memory multi-node Raft cluster with deterministic timing.
#[derive(Debug)]
pub struct Simulator {
    tick: u64,
    nodes: HashMap<u64, RaftNode>,
    inbox: VecDeque<PendingMessage>,
    /// Undirected pairs of nodes that cannot exchange messages.
    partitions: HashSet<(u64, u64)>,
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
            partitions: HashSet::new(),
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

    /// IDs of nodes currently in the leader role.
    pub fn leaders(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.role == Role::Leader)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Returns true when exactly one leader exists.
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

    /// Propose a command to the cluster through the given leader and deliver replication RPCs.
    pub fn propose_command(&mut self, leader_id: u64, command: Command) -> Result<(), ProposeError> {
        let actions = self.node_mut(leader_id).propose_command(command)?;
        self.enqueue_actions(leader_id, actions);
        self.drain_inbox();
        Ok(())
    }

    /// Returns true if every node has the same state machine contents.
    pub fn cluster_state_matches(&self) -> bool {
        let ids = self.node_ids();
        let Some(first) = ids.first().copied() else {
            return true;
        };

        let expected = self.node(first).state_machine.clone();
        ids.iter()
            .skip(1)
            .all(|&id| self.node(id).state_machine == expected)
    }

    /// Propose raw log bytes to the cluster through the given leader.
    pub fn propose(&mut self, leader_id: u64, data: Vec<u8>) -> Result<(), ProposeError> {
        let actions = self.node_mut(leader_id).propose(data)?;
        self.enqueue_actions(leader_id, actions);
        self.drain_inbox();
        Ok(())
    }

    /// Advance one tick: leader heartbeats, election timers, then deliver pending messages.
    pub fn tick(&mut self) {
        self.tick += 1;
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

    /// Run until `has_stable_leader` or `max_ticks` is reached. Returns the leader ID if found.
    pub fn run_until_leader(&mut self, max_ticks: u64) -> Option<u64> {
        for _ in 0..max_ticks {
            self.tick();
            if let [leader] = self.leaders().as_slice() {
                return Some(*leader);
            }
        }
        None
    }

    fn process_heartbeats(&mut self) {
        let now = self.tick;
        let ids = self.node_ids();
        for id in ids {
            let actions = self.node_mut(id).on_heartbeat_tick(now);
            self.enqueue_actions(id, actions);
        }
    }

    fn process_election_timeouts(&mut self) {
        let now = self.tick;
        let ids: Vec<u64> = self.node_ids();
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
            if self.is_partitioned(pending.from, pending.to) {
                continue;
            }
            let replies = self.deliver(pending.from, pending.to, pending.msg);
            for reply in replies {
                self.inbox.push_back(reply);
            }
        }
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
                }]
            }
            Message::AppendEntries(request) => {
                let response = self.node_mut(to).handle_append_entries(now, request);
                vec![PendingMessage {
                    from: to,
                    to: from,
                    msg: Message::AppendEntriesResponse(response),
                }]
            }
            Message::RequestVoteResponse(response) => {
                let actions = self
                    .node_mut(to)
                    .handle_request_vote_response(now, from, response);
                self.actions_to_pending(to, actions)
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
                    self.inbox.push_back(PendingMessage { from, to, msg });
                }
                Action::Apply { .. } => {
                    // Already applied on the node in apply_committed_entries.
                }
            }
        }
    }

    fn actions_to_pending(&self, from: u64, actions: Vec<Action>) -> Vec<PendingMessage> {
        actions
            .into_iter()
            .filter_map(|action| match action {
                Action::Send { to, msg } => Some(PendingMessage { from, to, msg }),
                Action::Apply { .. } => None,
            })
            .collect()
    }

    fn is_partitioned(&self, a: u64, b: u64) -> bool {
        self.partitions.contains(&(a.min(b), a.max(b)))
    }
}
