//! Core Raft node state and RPC message types.
//!
//! # Implementation status
//!
//! **Done**
//! - Node state, log sentinel, RPC message types
//! - Follower: `handle_request_vote`, `handle_append_entries`
//! - Election timer, `Action` enum, `start_election`, `handle_request_vote_response`
//! - Leader: heartbeats, `propose`, `handle_append_entries_response`
//! - Apply loop: advance `last_applied` into the key-value state machine
//! - Deterministic simulator (`simulator.rs`)

use crate::state_machine::{Command, StateMachine};

use std::collections::{HashMap, HashSet};

/// Side effects produced by a node step for the simulator to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Deliver an RPC message to another node.
    Send { to: u64, msg: Message },
    /// Apply a committed log entry to the state machine.
    Apply { index: u64, command: Command },
}

/// Error returned when a client command cannot be proposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,
}

/// A node's role in the cluster at a given moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// A single replicated log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// Term when this entry was first created by the leader.
    pub term: u64,
    /// Opaque command applied to the state machine once committed.
    pub data: Vec<u8>,
}

impl LogEntry {
    /// Sentinel entry at Raft index 0; never committed or applied.
    pub fn sentinel() -> Self {
        Self {
            term: 0,
            data: Vec::new(),
        }
    }
}

/// RPC messages exchanged between Raft servers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    RequestVote(RequestVote),
    AppendEntries(AppendEntries),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntriesResponse(AppendEntriesResponse),
}

/// RequestVote RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    /// Candidate's term.
    pub term: u64,
    /// Candidate requesting the vote.
    pub candidate_id: u64,
    /// Index of the candidate's last log entry.
    pub last_log_index: u64,
    /// Term of the candidate's last log entry.
    pub last_log_term: u64,
}

/// AppendEntries RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    /// Leader's term.
    pub term: u64,
    /// So followers can redirect clients.
    pub leader_id: u64,
    /// Index of the log entry immediately preceding the new ones.
    pub prev_log_index: u64,
    /// Term of the entry at `prev_log_index`.
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's `commit_index`.
    pub leader_commit_index: u64,
}

/// Response to RequestVote RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    /// Current term, for the candidate to update itself.
    pub term: u64,
    /// True if the recipient voted for the candidate.
    pub vote_granted: bool,
}

/// Response to AppendEntries RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// Current term, for the leader to update itself.
    pub term: u64,
    /// True if follower contained an entry matching `prev_log_index` and `prev_log_term`.
    pub success: bool,
}

/// One server's Raft state and leader replication progress.
#[derive(Debug, Clone)]
pub struct RaftNode {
    /// Unique ID of this node in the cluster.
    pub id: u64,
    /// (Persistent) Latest term this node has seen (monotonically increasing).
    pub term: u64,
    /// (Volatile) Current role of this node.
    pub role: Role,
    /// (Persistent) The node that this node voted for in the current term.
    pub voted_for: Option<u64>,
    /// (Persistent) Replicated log entries. Index 0 is a sentinel; real entries start at index 1.
    pub log: Vec<LogEntry>,
    /// (Volatile) Highest log index known to be committed.
    pub commit_index: u64,
    /// (Volatile) Highest log index applied to the state machine.
    pub last_applied: u64,
    /// (Volatile) Per-follower next log index to send (leader only).
    pub next_index: HashMap<u64, u64>,
    /// (Volatile) Per-follower highest replicated index (leader only).
    pub match_index: HashMap<u64, u64>,
    /// Peer IDs in the cluster (excludes self).
    pub peer_ids: Vec<u64>,
    /// (Volatile) Ticks to wait before starting an election when not leader.
    pub election_timeout: u64,
    /// (Volatile) Simulator tick at which the election timer fires.
    pub election_deadline: u64,
    /// (Volatile) Peers that granted a vote to this candidate in the current term.
    pub votes_granted: HashSet<u64>,
    /// (Volatile) Ticks between leader heartbeats (must be less than election timeout).
    pub heartbeat_interval: u64,
    /// (Volatile) Simulator tick at which the leader should send the next heartbeat.
    pub heartbeat_deadline: u64,
    /// (Volatile) Key-value state machine updated from committed log entries.
    pub state_machine: StateMachine,
}

impl RaftNode {
    pub fn new(id: u64, peer_ids: Vec<u64>) -> Self {
        Self::with_election_timeout(id, peer_ids, 150)
    }

    pub fn with_election_timeout(id: u64, peer_ids: Vec<u64>, election_timeout: u64) -> Self {
        let heartbeat_interval = (election_timeout / 3).max(1);
        let mut node = Self {
            id,
            term: 0,
            role: Role::Follower,
            voted_for: None,
            log: vec![LogEntry::sentinel()],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peer_ids,
            election_timeout,
            election_deadline: 0,
            votes_granted: HashSet::new(),
            heartbeat_interval,
            heartbeat_deadline: 0,
            state_machine: StateMachine::new(),
        };
        node.reset_election_timer(0);
        node
    }

    /// Raft index of the last log entry.
    pub fn last_log_index(&self) -> u64 {
        self.log.len().saturating_sub(1) as u64
    }

    /// Term of the last log entry.
    pub fn last_log_term(&self) -> u64 {
        self.log_term_at(self.last_log_index())
    }

    /// Term of the log entry at the given Raft index.
    pub fn log_term_at(&self, index: u64) -> u64 {
        self.log
            .get(index as usize)
            .map(|entry| entry.term)
            .unwrap_or(0)
    }

    /// Returns true if a follower/candidate should start an election at `now`.
    pub fn election_timed_out(&self, now: u64) -> bool {
        self.role != Role::Leader && now >= self.election_deadline
    }

    /// Start an election after the timer fires. Returns outbound actions for the simulator.
    pub fn on_election_timeout(&mut self, now: u64) -> Vec<Action> {
        if !self.election_timed_out(now) {
            return Vec::new();
        }
        self.start_election(now)
    }

    /// Returns true if the leader should send heartbeats at `now`.
    pub fn heartbeat_due(&self, now: u64) -> bool {
        self.role == Role::Leader && now >= self.heartbeat_deadline
    }

    /// Send periodic heartbeats when due. Returns outbound actions for the simulator.
    pub fn on_heartbeat_tick(&mut self, now: u64) -> Vec<Action> {
        if !self.heartbeat_due(now) {
            return Vec::new();
        }
        self.reset_heartbeat_timer(now);
        self.send_heartbeats()
    }

    /// Transition to candidate, request votes, and become leader immediately if alone in the cluster.
    pub fn start_election(&mut self, now: u64) -> Vec<Action> {
        self.become_candidate(now)
    }

    /// Handle an incoming RequestVote RPC (follower/candidate side).
    pub fn handle_request_vote(&mut self, now: u64, request: RequestVote) -> RequestVoteResponse {
        // 1. Reject if the candidate's term is stale.
        if request.term < self.term {
            return RequestVoteResponse {
                term: self.term,
                vote_granted: false,
            };
        }

        // 2. If the candidate's term is newer, update local term and clear vote.
        if request.term > self.term {
            self.become_follower(request.term, now);
        }
        // 3. Valid RPC from a candidate → remain/step down to follower.
        self.role = Role::Follower;
        self.votes_granted.clear();
        // 4. Reset election timer on any non-stale RequestVote RPC.
        self.reset_election_timer(now);

        // 5. Grant vote only if the candidate's log is at least as up-to-date as ours.
        let up_to_date = Self::log_is_up_to_date(
            request.last_log_term,
            request.last_log_index,
            self.last_log_term(),
            self.last_log_index(),
        );

        // 6. Grant vote if we haven't voted yet, or already voted for this candidate.
        if up_to_date
            && (self.voted_for.is_none() || self.voted_for == Some(request.candidate_id))
        {
            self.voted_for = Some(request.candidate_id);
            RequestVoteResponse {
                term: self.term,
                vote_granted: true,
            }
        } else {
            RequestVoteResponse {
                term: self.term,
                vote_granted: false,
            }
        }
    }

    /// Handle an incoming AppendEntries RPC (follower side).
    pub fn handle_append_entries(
        &mut self,
        now: u64,
        request: AppendEntries,
    ) -> AppendEntriesResponse {
        // 1. Reject if the leader's term is stale.
        if request.term < self.term {
            return AppendEntriesResponse {
                term: self.term,
                success: false,
            };
        }

        // 2. If the leader's term is newer, update local term and clear vote.
        if request.term > self.term {
            self.become_follower(request.term, now);
        }
        // 3. Valid RPC from a leader → remain/step down to follower.
        self.role = Role::Follower;
        self.votes_granted.clear();

        // 4. Reject if the log doesn't contain a matching entry at prev_log_index.
        if self.log_term_at(request.prev_log_index) != request.prev_log_term {
            return AppendEntriesResponse {
                term: self.term,
                success: false,
            };
        }

        // 5. Reset election timer on successful AppendEntries (including heartbeats).
        self.reset_election_timer(now);

        // 6. On conflict (same index, different term), truncate the suffix and append new entries.
        for (i, entry) in request.entries.iter().enumerate() {
            let index = request.prev_log_index + 1 + i as u64;
            if index < self.log.len() as u64 {
                if self.log[index as usize].term != entry.term {
                    self.log.truncate(index as usize);
                    self.log.extend_from_slice(&request.entries[i..]);
                    break;
                }
            } else {
                self.log.extend_from_slice(&request.entries[i..]);
                break;
            }
        }

        // 7. Advance commit_index if the leader has committed further (capped by our log length).
        if request.leader_commit_index > self.commit_index {
            self.commit_index = std::cmp::min(request.leader_commit_index, self.last_log_index());
        }

        // 8. Apply newly committed entries to the state machine.
        self.apply_committed_entries();

        AppendEntriesResponse {
            term: self.term,
            success: true,
        }
    }

    /// Handle a RequestVote response (candidate side).
    pub fn handle_request_vote_response(
        &mut self,
        now: u64,
        from: u64,
        response: RequestVoteResponse,
    ) -> Vec<Action> {
        // 1. If the response carries a newer term, step down to follower.
        if response.term > self.term {
            self.become_follower(response.term, now);
            return Vec::new();
        }

        // 2. Ignore stale responses or replies when not a candidate.
        if self.role != Role::Candidate || response.term < self.term {
            return Vec::new();
        }

        // 3. Count granted votes; become leader on a majority.
        if response.vote_granted {
            self.votes_granted.insert(from);
            if self.has_election_majority() {
                return self.become_leader(now);
            }
        }

        Vec::new()
    }

    /// Append a client command to the log and replicate it to followers (leader only).
    pub fn propose_command(&mut self, command: Command) -> Result<Vec<Action>, ProposeError> {
        self.propose(command.encode())
    }

    /// Append raw log bytes to the log and replicate them to followers (leader only).
    pub fn propose(&mut self, data: Vec<u8>) -> Result<Vec<Action>, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }

        self.log.push(LogEntry {
            term: self.term,
            data,
        });

        Ok(self.replication_actions())
    }

    /// Handle an AppendEntries response (leader side).
    pub fn handle_append_entries_response(
        &mut self,
        now: u64,
        from: u64,
        response: AppendEntriesResponse,
    ) -> Vec<Action> {
        // 1. If the response carries a newer term, step down to follower.
        if response.term > self.term {
            self.become_follower(response.term, now);
            return Vec::new();
        }

        // 2. Ignore stale responses or replies when not leader.
        if self.role != Role::Leader || response.term < self.term {
            return Vec::new();
        }

        let next_index = self.next_index.get(&from).copied().unwrap_or(1);

        // 3. On success, advance follower progress and try to commit.
        if response.success {
            let matched = if next_index <= self.last_log_index() {
                self.last_log_index()
            } else {
                next_index.saturating_sub(1)
            };
            self.match_index.insert(from, matched);
            self.next_index.insert(from, matched + 1);
            return self.advance_commit_index();
        }

        // 4. On failure, back up next_index and retry replication to this follower.
        if next_index > 1 {
            self.next_index.insert(from, next_index - 1);
        }

        vec![Action::Send {
            to: from,
            msg: Message::AppendEntries(self.append_entries_for_peer(from)),
        }]
    }

    /// Build AppendEntries RPCs for all peers (heartbeats or replication).
    pub fn replication_actions(&self) -> Vec<Action> {
        self.peer_ids
            .iter()
            .map(|&to| Action::Send {
                to,
                msg: Message::AppendEntries(self.append_entries_for_peer(to)),
            })
            .collect()
    }

    /// Build heartbeat AppendEntries messages for all peers.
    pub fn send_heartbeats(&self) -> Vec<Action> {
        self.replication_actions()
    }

    fn append_entries_for_peer(&self, peer: u64) -> AppendEntries {
        let next_index = self.next_index.get(&peer).copied().unwrap_or(1);
        let prev_log_index = next_index.saturating_sub(1);
        let entries = if next_index <= self.last_log_index() {
            self.log[next_index as usize..].to_vec()
        } else {
            Vec::new()
        };

        AppendEntries {
            term: self.term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term: self.log_term_at(prev_log_index),
            entries,
            leader_commit_index: self.commit_index,
        }
    }

    fn advance_commit_index(&mut self) -> Vec<Action> {
        let old_commit_index = self.commit_index;

        for index in (self.commit_index + 1)..=self.last_log_index() {
            if self.log[index as usize].term != self.term {
                continue;
            }

            let mut replicated = 1;
            for &matched in self.match_index.values() {
                if matched >= index {
                    replicated += 1;
                }
            }

            if replicated >= self.cluster_size() / 2 + 1 {
                self.commit_index = index;
            }
        }

        let mut actions = self.apply_committed_entries();
        if self.commit_index > old_commit_index {
            actions.extend(self.replication_actions());
        }
        actions
    }

    fn apply_committed_entries(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();

        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let data = &self.log[self.last_applied as usize].data;
            let command = Command::decode(data).expect("committed entry must decode");
            self.state_machine.apply(&command);
            actions.push(Action::Apply {
                index: self.last_applied,
                command,
            });
        }

        actions
    }

    fn become_follower(&mut self, term: u64, now: u64) {
        self.term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes_granted.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.reset_election_timer(now);
    }

    fn become_candidate(&mut self, now: u64) -> Vec<Action> {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.votes_granted.clear();
        self.votes_granted.insert(self.id);
        self.next_index.clear();
        self.match_index.clear();
        self.reset_election_timer(now);

        if self.has_election_majority() {
            return self.become_leader(now);
        }

        self.request_vote_actions()
    }

    fn become_leader(&mut self, now: u64) -> Vec<Action> {
        self.role = Role::Leader;
        self.votes_granted.clear();

        let next = self.last_log_index() + 1;
        for &peer in &self.peer_ids {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }

        self.reset_heartbeat_timer(now);
        self.send_heartbeats()
    }

    fn request_vote_actions(&self) -> Vec<Action> {
        let request = RequestVote {
            term: self.term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        self.peer_ids
            .iter()
            .map(|&to| Action::Send {
                to,
                msg: Message::RequestVote(request.clone()),
            })
            .collect()
    }

    fn reset_election_timer(&mut self, now: u64) {
        self.election_deadline = now + self.election_timeout;
    }

    fn reset_heartbeat_timer(&mut self, now: u64) {
        self.heartbeat_deadline = now + self.heartbeat_interval;
    }

    fn cluster_size(&self) -> usize {
        self.peer_ids.len() + 1
    }

    fn has_election_majority(&self) -> bool {
        self.votes_granted.len() >= self.cluster_size() / 2 + 1
    }

    /// Returns true if the candidate log is at least as up-to-date as the receiver log.
    /// Compare `(last_log_term, last_log_index)` lexicographically.
    fn log_is_up_to_date(
        candidate_last_term: u64,
        candidate_last_index: u64,
        receiver_last_term: u64,
        receiver_last_index: u64,
    ) -> bool {
        candidate_last_term > receiver_last_term
            || (candidate_last_term == receiver_last_term
                && candidate_last_index >= receiver_last_index)
    }
}
