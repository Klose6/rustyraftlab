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

/// One server's Raft state and leader replication progress.
#[derive(Debug, Clone)]
pub struct RaftNode {
    /// Unique ID of this node in the cluster.
    pub id: u64,
    /// Latest term this node has seen (monotonically increasing).
    pub term: u64,
    /// Current role of this node.
    pub role: Role,
    /// Replicated log entries.
    pub log: Vec<LogEntry>,
    /// Highest log index known to be committed.
    pub commit_index: u64,
    /// Highest log index applied to the state machine.
    pub last_applied: u64,
    /// Per-follower next log index to send (leader only).
    pub next_index: HashMap<u64, u64>,
    /// Per-follower highest replicated index (leader only).
    pub match_index: HashMap<u64, u64>,
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
