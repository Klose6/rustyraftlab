use rustyraftlab::raft::{AppendEntries, LogEntry, RaftNode, RequestVote, Role};

fn entry(term: u64, data: &str) -> LogEntry {
    LogEntry {
        term,
        data: data.as_bytes().to_vec(),
    }
}

fn node_with_log(id: u64, entries: Vec<LogEntry>) -> RaftNode {
    let mut node = RaftNode::new(id, vec![]);
    node.log.extend(entries);
    node
}

#[test]
fn rejects_stale_term_request_vote() {
    let mut node = RaftNode::new(1, vec![]);
    node.term = 2;

    let response = node.handle_request_vote(RequestVote {
        term: 1,
        candidate_id: 2,
        last_log_index: 0,
        last_log_term: 0,
    });

    assert!(!response.vote_granted);
    assert_eq!(response.term, 2);
}

#[test]
fn grants_vote_on_higher_term_up_to_date_candidate() {
    let mut node = RaftNode::new(1, vec![]);

    let response = node.handle_request_vote(RequestVote {
        term: 1,
        candidate_id: 2,
        last_log_index: 0,
        last_log_term: 0,
    });

    assert!(response.vote_granted);
    assert_eq!(response.term, 1);
    assert_eq!(node.term, 1);
    assert_eq!(node.voted_for, Some(2));
    assert_eq!(node.role, Role::Follower);
}

#[test]
fn rejects_if_already_voted_for_other() {
    let mut node = RaftNode::new(1, vec![]);
    node.term = 1;
    node.voted_for = Some(3);

    let response = node.handle_request_vote(RequestVote {
        term: 1,
        candidate_id: 2,
        last_log_index: 0,
        last_log_term: 0,
    });

    assert!(!response.vote_granted);
    assert_eq!(node.voted_for, Some(3));
}

#[test]
fn rejects_less_up_to_date_candidate() {
    let mut node = node_with_log(1, vec![entry(2, "a"), entry(2, "b")]);
    node.term = 2;

    let response = node.handle_request_vote(RequestVote {
        term: 3,
        candidate_id: 2,
        last_log_index: 1,
        last_log_term: 2,
    });

    assert!(!response.vote_granted);
    assert_eq!(node.term, 3);
    assert_eq!(node.voted_for, None);
}

#[test]
fn append_entries_rejects_stale_term() {
    let mut node = RaftNode::new(1, vec![]);
    node.term = 2;

    let response = node.handle_append_entries(AppendEntries {
        term: 1,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    });

    assert!(!response.success);
    assert_eq!(response.term, 2);
}

#[test]
fn append_entries_accepts_heartbeat() {
    let mut node = RaftNode::new(1, vec![]);

    let response = node.handle_append_entries(AppendEntries {
        term: 1,
        leader_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    });

    assert!(response.success);
    assert_eq!(node.term, 1);
    assert_eq!(node.role, Role::Follower);
    assert_eq!(node.last_log_index(), 0);
}

#[test]
fn append_entries_truncates_conflicting_suffix() {
    let mut node = node_with_log(1, vec![entry(1, "a"), entry(1, "b"), entry(1, "c")]);

    let response = node.handle_append_entries(AppendEntries {
        term: 2,
        leader_id: 2,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: vec![entry(2, "b2"), entry(2, "d")],
        leader_commit: 0,
    });

    assert!(response.success);
    assert_eq!(node.log.len(), 4);
    assert_eq!(node.log[1], entry(1, "a"));
    assert_eq!(node.log[2], entry(2, "b2"));
    assert_eq!(node.log[3], entry(2, "d"));
}

#[test]
fn append_entries_advances_commit_index() {
    let mut node = node_with_log(1, vec![entry(1, "a"), entry(1, "b")]);

    let response = node.handle_append_entries(AppendEntries {
        term: 1,
        leader_id: 2,
        prev_log_index: 2,
        prev_log_term: 1,
        entries: vec![],
        leader_commit: 2,
    });

    assert!(response.success);
    assert_eq!(node.commit_index, 2);
}
