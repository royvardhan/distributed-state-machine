use std::{
    collections::{HashMap, HashSet},
    sync::{mpsc, Arc, Mutex},
};

enum MessageType {
    Proposal,
    Acknowledgment,
    Commit,
}

enum State {
    Init,
    Running,
    Stopped,
}

struct Message {
    sender_id: u64,
    message_type: MessageType,
    proposed_state: State,
    proposal_id: String,
}

struct Node {
    id: u64,
    state: Arc<Mutex<State>>,
    peers: HashMap<u64, String>,
    address: String,
    tx: mpsc::Sender<Message>,
    proposal_acknowledment: Arc<Mutex<HashMap<String, HashSet<u64>>>>,
}

fn main() {}
