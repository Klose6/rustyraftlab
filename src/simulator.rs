//! Deterministic fault-injection simulator for Raft correctness testing.
//!
//! The simulator owns all nodes, advances a global tick counter, delivers RPC
//! messages synchronously from an inbox, and optionally blocks links between nodes.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::raft::{Action, AppendEntriesResponse, Message, RaftNode, Role};

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

    /// Advance one tick: fire election timers, then deliver all pending messages.
    pub fn tick(&mut self) {
        self.tick += 1;
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
                self.handle_append_entries_response(to, from, response);
                Vec::new()
            }
        }
    }

    fn handle_append_entries_response(
        &mut self,
        _leader_id: u64,
        _from: u64,
        _response: AppendEntriesResponse,
    ) {
        // TODO: leader replication once handle_append_entries_response exists on RaftNode
    }

    fn enqueue_actions(&mut self, from: u64, actions: Vec<Action>) {
        for pending in self.actions_to_pending(from, actions) {
            self.inbox.push_back(pending);
        }
    }

    fn actions_to_pending(&self, from: u64, actions: Vec<Action>) -> Vec<PendingMessage> {
        actions
            .into_iter()
            .filter_map(|action| match action {
                Action::Send { to, msg } => Some(PendingMessage { from, to, msg }),
            })
            .collect()
    }

    fn is_partitioned(&self, a: u64, b: u64) -> bool {
        self.partitions.contains(&(a.min(b), a.max(b)))
    }
}
