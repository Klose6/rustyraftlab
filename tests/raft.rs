use rustyraftlab::raft::{
    Action, AppendEntries, LogEntry, Message, RaftNode, RequestVote, RequestVoteResponse, Role,
};

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

    let response = node.handle_request_vote(
        0,
        RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
    );

    assert!(!response.vote_granted);
    assert_eq!(response.term, 2);
}

#[test]
fn grants_vote_on_higher_term_up_to_date_candidate() {
    let mut node = RaftNode::new(1, vec![]);

    let response = node.handle_request_vote(
        0,
        RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
    );

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

    let response = node.handle_request_vote(
        0,
        RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
    );

    assert!(!response.vote_granted);
    assert_eq!(node.voted_for, Some(3));
}

#[test]
fn rejects_less_up_to_date_candidate() {
    let mut node = node_with_log(1, vec![entry(2, "a"), entry(2, "b")]);
    node.term = 2;

    let response = node.handle_request_vote(
        0,
        RequestVote {
            term: 3,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 2,
        },
    );

    assert!(!response.vote_granted);
    assert_eq!(node.term, 3);
    assert_eq!(node.voted_for, None);
}

#[test]
fn append_entries_rejects_stale_term() {
    let mut node = RaftNode::new(1, vec![]);
    node.term = 2;

    let response = node.handle_append_entries(
        0,
        AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    );

    assert!(!response.success);
    assert_eq!(response.term, 2);
}

#[test]
fn append_entries_accepts_heartbeat() {
    let mut node = RaftNode::new(1, vec![]);

    let response = node.handle_append_entries(
        10,
        AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    );

    assert!(response.success);
    assert_eq!(node.term, 1);
    assert_eq!(node.role, Role::Follower);
    assert_eq!(node.last_log_index(), 0);
    assert_eq!(node.election_deadline, 160);
}

#[test]
fn append_entries_truncates_conflicting_suffix() {
    let mut node = node_with_log(1, vec![entry(1, "a"), entry(1, "b"), entry(1, "c")]);

    let response = node.handle_append_entries(
        0,
        AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![entry(2, "b2"), entry(2, "d")],
            leader_commit: 0,
        },
    );

    assert!(response.success);
    assert_eq!(node.log.len(), 4);
    assert_eq!(node.log[1], entry(1, "a"));
    assert_eq!(node.log[2], entry(2, "b2"));
    assert_eq!(node.log[3], entry(2, "d"));
}

#[test]
fn append_entries_advances_commit_index() {
    let mut node = node_with_log(1, vec![entry(1, "a"), entry(1, "b")]);

    let response = node.handle_append_entries(
        0,
        AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 2,
        },
    );

    assert!(response.success);
    assert_eq!(node.commit_index, 2);
}

#[test]
fn start_election_sends_request_vote_to_peers() {
    let mut node = RaftNode::new(1, vec![2, 3]);

    let actions = node.start_election(100);

    assert_eq!(node.role, Role::Candidate);
    assert_eq!(node.term, 1);
    assert_eq!(node.voted_for, Some(1));
    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[0],
        Action::Send {
            to: 2,
            msg: Message::RequestVote(RequestVote { term: 1, candidate_id: 1, .. })
        }
    ));
}

#[test]
fn single_node_becomes_leader_immediately_on_election() {
    let mut node = RaftNode::new(1, vec![]);

    let actions = node.start_election(100);

    assert_eq!(node.role, Role::Leader);
    assert_eq!(node.term, 1);
    assert_eq!(actions, Vec::<Action>::new());
}

#[test]
fn candidate_becomes_leader_on_majority_votes() {
    let mut node = RaftNode::new(1, vec![2, 3, 4]);
    node.start_election(100);

    let actions = node.handle_request_vote_response(
        100,
        2,
        RequestVoteResponse {
            term: 1,
            vote_granted: true,
        },
    );
    assert!(actions.is_empty());
    assert_eq!(node.role, Role::Candidate);

    let actions = node.handle_request_vote_response(
        100,
        3,
        RequestVoteResponse {
            term: 1,
            vote_granted: true,
        },
    );

    assert_eq!(node.role, Role::Leader);
    assert_eq!(actions.len(), 3);
    assert!(matches!(
        &actions[0],
        Action::Send {
            to: 2,
            msg: Message::AppendEntries(_)
        }
    ));
}

#[test]
fn election_timeout_triggers_election() {
    let mut node = RaftNode::with_election_timeout(1, vec![2], 50);
    assert!(!node.election_timed_out(49));

    let actions = node.on_election_timeout(50);

    assert_eq!(node.role, Role::Candidate);
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        Action::Send {
            to: 2,
            msg: Message::RequestVote(_)
        }
    ));
}

#[test]
fn higher_term_in_vote_response_steps_down_to_follower() {
    let mut node = RaftNode::new(1, vec![2]);
    node.start_election(100);

    let actions = node.handle_request_vote_response(
        100,
        2,
        RequestVoteResponse {
            term: 2,
            vote_granted: false,
        },
    );

    assert!(actions.is_empty());
    assert_eq!(node.role, Role::Follower);
    assert_eq!(node.term, 2);
}
