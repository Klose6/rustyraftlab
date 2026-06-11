//! Core Raft node state and RPC message types.
//!
//! Models persistent/volatile state and RPC messages from the Raft paper.

use std::collections::HashMap;

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
    /// Peer IDs in the cluster.
    pub peer_ids: Vec<u64>,
}

impl RaftNode {
    pub fn new(id: u64, peer_ids: Vec<u64>) -> Self {
        Self {
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
        }
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

    pub fn handle_request_vote(&mut self, request: RequestVote) -> RequestVoteResponse {
        /// If the candidate's term is less than the current term, reject the vote.
        if request.term < self.term {
            return RequestVoteResponse {
                term: self.term,
                vote_granted: false,
            };
        }
        /// If the candidate has not voted for anyone in the current term, or the candidate is the same as the voted for node, vote for the candidate in the request.
        if self.voted_for.is_none() || self.voted_for.unwrap() == request.candidate_id {
            self.voted_for = Some(request.candidate_id);
            return RequestVoteResponse {
                term: self.term,
                vote_granted: true,
            };
        }
        return RequestVoteResponse {
            term: self.term,
            vote_granted: false,
        };
    }

    pub fn handle_append_entries(&mut self, request: AppendEntries) -> AppendEntriesResponse {
        if request.term < self.term {
            return AppendEntriesResponse {
                term: self.term,
                success: false,
            };
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
    pub leader_commit: u64,
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
