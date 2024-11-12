use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    spawn,
    sync::{mpsc, Mutex},
    time::sleep,
};

use metrics::counter;
use thiserror::Error;
use tracing::{error, info, warn};
use uuid::Uuid;

const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB limit

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum MessageType {
    Proposal,
    Acknowledgment,
    Commit,
    Reject,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum State {
    Init,
    Running,
    Stopped,
}

impl State {
    fn can_transition_to(&self, new_state: &State) -> bool {
        match (self, new_state) {
            (State::Init, State::Running) => true,
            (State::Running, State::Stopped) => true,
            (State::Stopped, State::Init) => true,
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    sender_id: u64,
    message_type: MessageType,
    proposed_state: State,
    proposal_id: String,
    timestamp: u64,
    signature: Option<Vec<u8>>, // For future authentication implementation
}

struct Node {
    id: u64,
    state: Arc<Mutex<State>>,
    peers: HashMap<u64, String>,
    address: String,
    tx: mpsc::Sender<Message>,
    proposal_acknowledgement: Arc<Mutex<HashMap<String, HashSet<u64>>>>,
    config: NodeConfig,
    last_commit_timestamp: u64,
}

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Network error: {0}")]
    Network(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Consensus timeout")]
    ConsensusTimeout,
    #[error("Message send failed")]
    MessageSendFailed,
    #[error("Message too large")]
    MessageTooLarge,
}

#[derive(Clone)]
struct NodeConfig {
    consensus_timeout: Duration,
    message_retry_attempts: u32,
    message_retry_delay: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            consensus_timeout: Duration::from_secs(5),
            message_retry_attempts: 3,
            message_retry_delay: Duration::from_millis(100),
        }
    }
}

impl Node {
    async fn send_message(&self, message: &Message, receiver: &str) -> Result<(), NodeError> {
        let mut attempts = 0;
        while attempts < self.config.message_retry_attempts {
            match self.try_send_message(message, receiver).await {
                Ok(_) => {
                    counter!("message_sent_success", 1);
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    warn!(
                        "Failed to send message, attempt {}/{}: {:?}",
                        attempts, self.config.message_retry_attempts, e
                    );
                    if attempts < self.config.message_retry_attempts {
                        sleep(self.config.message_retry_delay).await;
                    }
                }
            }
        }
        counter!("message_sent_failure", 1);
        Err(NodeError::MessageSendFailed)
    }

    async fn listen(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        println!("Node {} listening on {}", self.id, self.address);

        loop {
            let (mut socket, _) = listener.accept().await?;

            let tx = self.tx.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) => {
                            println!("Connection closed");
                            break; // Connection was closed
                        }
                        Ok(n) => {
                            if let Ok(message) = serde_json::from_slice::<Message>(&buf[..n]) {
                                tx.send(message)
                                    .await
                                    .expect("Failed to send message to channel");
                            } else {
                                println!("Failed to deserialize message");
                            }
                        }
                        Err(e) => {
                            println!("Failed to read from socket: {:?}", e);
                            break;
                        }
                    }
                }
            });
        }
    }

    async fn try_send_message(&self, message: &Message, receiver: &str) -> Result<(), NodeError> {
        let ser_message = serde_json::to_vec(message)?;
        if ser_message.len() > MAX_MESSAGE_SIZE {
            return Err(NodeError::MessageTooLarge);
        }
        let mut stream = TcpStream::connect(receiver).await?;
        stream.write_all(&ser_message).await?;
        Ok(())
    }

    async fn broadcast_proposal(&self, proposed_state: State) -> Result<(), NodeError> {
        let proposal_id = Uuid::new_v4().to_string();
        info!(
            node_id = self.id,
            proposal_id = %proposal_id,
            "Broadcasting proposal"
        );

        let message = Message {
            sender_id: self.id,
            message_type: MessageType::Proposal,
            proposed_state: proposed_state.clone(),
            proposal_id: proposal_id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        let mut proposal_acknowledgement = self.proposal_acknowledgement.lock().await;
        proposal_acknowledgement.insert(proposal_id.clone(), HashSet::new());

        for (peer_id, address) in self.peers.iter() {
            match self.send_message(&message, address).await {
                Ok(_) => info!("Sent proposal to peer {}", peer_id),
                Err(e) => error!("Failed to send proposal to peer {}: {:?}", peer_id, e),
            }
        }

        self.wait_for_acknowledgements(proposal_id).await
    }

    async fn handle_incoming_messages(&mut self, mut rx: mpsc::Receiver<Message>) {
        while let Some(message) = rx.recv().await {
            counter!("messages_received", 1);
            match message.message_type {
                MessageType::Proposal => {
                    info!(
                        "Received proposal from node {} with id {}",
                        message.sender_id, message.proposal_id
                    );

                    let current_state = self.state.lock().await.clone();
                    if !current_state.can_transition_to(&message.proposed_state) {
                        self.send_reject_message(&message).await;
                        continue;
                    }

                    self.handle_proposal(message).await;
                }
                MessageType::Acknowledgment => {
                    info!(
                        "Received acknowledgment from node {} for proposal {}",
                        message.sender_id, message.proposal_id
                    );
                    self.handle_acknowledgment(message).await;
                }
                MessageType::Commit => {
                    info!(
                        "Received commit from node {} for proposal {}",
                        message.sender_id, message.proposal_id
                    );
                    self.handle_commit(message).await;
                }
                MessageType::Reject => {
                    warn!(
                        "Received reject from node {} for proposal {}",
                        message.sender_id, message.proposal_id
                    );
                    self.handle_reject(message).await;
                }
            }
        }
    }

    async fn handle_proposal(&self, message: Message) {
        let ack = Message {
            sender_id: self.id,
            message_type: MessageType::Acknowledgment,
            proposed_state: message.proposed_state,
            proposal_id: message.proposal_id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        if let Some(sender_addr) = self.peers.get(&message.sender_id) {
            if let Err(e) = self.send_message(&ack, sender_addr).await {
                error!("Failed to send acknowledgment: {:?}", e);
            }
        }
    }

    async fn handle_acknowledgment(&self, message: Message) {
        let mut acks = self.proposal_acknowledgement.lock().await;
        if let Some(ack_set) = acks.get_mut(&message.proposal_id) {
            ack_set.insert(message.sender_id);
        }
    }

    async fn handle_commit(&mut self, message: Message) {
        let mut current_state = self.state.lock().await;
        if message.timestamp < self.last_commit_timestamp {
            warn!("Received outdated commit message");
            return;
        }
        if current_state.can_transition_to(&message.proposed_state) {
            *current_state = message.proposed_state;
            self.last_commit_timestamp = message.timestamp;
            info!("State updated to {:?}", *current_state);
        }
    }

    async fn handle_reject(&self, message: Message) {
        let mut acks = self.proposal_acknowledgement.lock().await;
        acks.remove(&message.proposal_id);
    }

    async fn send_reject_message(&self, original_message: &Message) {
        let reject_message = Message {
            sender_id: self.id,
            message_type: MessageType::Reject,
            proposed_state: original_message.proposed_state.clone(),
            proposal_id: original_message.proposal_id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        if let Some(sender_addr) = self.peers.get(&original_message.sender_id) {
            if let Err(e) = self.send_message(&reject_message, sender_addr).await {
                error!("Failed to send reject message: {:?}", e);
            }
        }
    }

    async fn wait_for_acknowledgements(&self, proposal_id: String) -> Result<(), NodeError> {
        let total_nodes = self.peers.len() + 1; // Include self
        let majority = (total_nodes * 2 / 3) + 1; // Use 2/3 majority for Byzantine tolerance
        let timeout = tokio::time::sleep(self.config.consensus_timeout);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => {
                    error!("Consensus timeout reached for proposal {}", proposal_id);
                    return Err(NodeError::ConsensusTimeout);
                }
                _ = sleep(Duration::from_millis(100)) => {
                    let ack_count = {
                        let acks = self.proposal_acknowledgement.lock().await;
                        acks.get(&proposal_id).map(|acks| acks.len()).unwrap_or(0)
                    };

                    if ack_count >= majority {
                        info!("Consensus reached for proposal {}", proposal_id);
                        self.broadcast_commit(proposal_id).await?;
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn broadcast_commit(&self, proposal_id: String) -> Result<(), NodeError> {
        let commit_message = Message {
            sender_id: self.id,
            message_type: MessageType::Commit,
            proposed_state: self.state.lock().await.clone(),
            proposal_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        for address in self.peers.values() {
            if let Err(e) = self.send_message(&commit_message, address).await {
                error!("Failed to send commit message: {:?}", e);
            }
        }

        Ok(())
    }
}

async fn simulate_client_interaction() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await.unwrap(); // Connect to Node 1
    let proposal_message = Message {
        sender_id: 999,
        message_type: MessageType::Proposal,
        proposed_state: State::Running,
        proposal_id: Uuid::new_v4().to_string(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        signature: None,
    };

    let serialized_message = serde_json::to_vec(&proposal_message).unwrap();
    stream.write_all(&serialized_message).await.unwrap();
    stream.flush().await?; // Ensure the message is sent immediately
    println!("Simulated client sent proposal to Node 1");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), NodeError> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting P2P State Machine");

    let state = Arc::new(Mutex::new(State::Init));
    let proposal_acknowledgments = Arc::new(Mutex::new(HashMap::new()));
    let config = NodeConfig::default();

    let (sender1, receiver1) = mpsc::channel(32);

    let node1 = Arc::new(Mutex::new(Node {
        id: 1,
        state: state.clone(),
        peers: HashMap::from([(2, "0.0.0.0:8081".to_string())]),
        tx: sender1,
        address: "0.0.0.0:8080".to_string(),
        proposal_acknowledgement: proposal_acknowledgments.clone(),
        config: config.clone(),
        last_commit_timestamp: 0,
    }));

    let (sender2, receiver2) = mpsc::channel(32);

    let node2 = Arc::new(Mutex::new(Node {
        id: 2,
        state: state.clone(),
        peers: HashMap::from([(1, "0.0.0.0:8080".to_string())]),
        tx: sender2,
        address: "0.0.0.0:8081".to_string(),
        proposal_acknowledgement: proposal_acknowledgments,
        config: config.clone(),
        last_commit_timestamp: 0,
    }));

    let node1_clone_for_messages = Arc::clone(&node1);
    spawn(async move {
        node1_clone_for_messages
            .lock()
            .await
            .handle_incoming_messages(receiver1)
            .await;
    });

    let node2_clone_for_messages = Arc::clone(&node2);
    spawn(async move {
        node2_clone_for_messages
            .lock()
            .await
            .handle_incoming_messages(receiver2)
            .await;
    });

    // Listen for incoming connections
    let node1_clone_for_listen = Arc::clone(&node1);
    tokio::spawn(async move {
        node1_clone_for_listen
            .lock()
            .await
            .listen()
            .await
            .expect("Node 1 failed to listen");
    });

    let node2_clone_for_listen = Arc::clone(&node2);
    tokio::spawn(async move {
        node2_clone_for_listen
            .lock()
            .await
            .listen()
            .await
            .expect("Node 2 failed to listen");
    });

    // Ensure the servers have time to start up
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Use the original `node1` Arc to broadcast a proposal
    node1
        .lock()
        .await
        .broadcast_proposal(State::Running)
        .await?;

    // Start the simulation after a short delay to ensure nodes are listening
    tokio::time::sleep(Duration::from_secs(2)).await;
    if let Err(e) = simulate_client_interaction().await {
        eprintln!("Failed to simulate client: {:?}", e);
    }

    // Start metrics collection
    metrics::gauge!("node_count", 2.0);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;

    // Helper function to create a test node
    async fn create_test_node(id: u64, address: &str) -> Arc<Mutex<Node>> {
        let state = Arc::new(Mutex::new(State::Init));
        let proposal_acknowledgments = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _) = mpsc::channel(32);

        Arc::new(Mutex::new(Node {
            id,
            state: state.clone(),
            peers: HashMap::new(),
            address: address.to_string(),
            tx,
            proposal_acknowledgement: proposal_acknowledgments,
            config: NodeConfig::default(),
            last_commit_timestamp: 0,
        }))
    }

    #[tokio::test]
    async fn test_state_transitions() {
        let node = create_test_node(1, "127.0.0.1:8080").await;

        // Test valid transitions
        {
            let node_lock = node.lock().await;
            let mut state = node_lock.state.lock().await;
            assert_eq!(*state, State::Init);
            assert!(state.can_transition_to(&State::Running));
            *state = State::Running;
        }

        {
            let node_lock = node.lock().await;
            let mut state = node_lock.state.lock().await;
            assert_eq!(*state, State::Running);
            assert!(state.can_transition_to(&State::Stopped));
            assert!(!state.can_transition_to(&State::Init));
        }
    }

    #[tokio::test]
    async fn test_message_creation() {
        let node = create_test_node(1, "127.0.0.1:8080").await;
        let proposal_id = Uuid::new_v4().to_string();

        let message = Message {
            sender_id: node.lock().await.id,
            message_type: MessageType::Proposal,
            proposed_state: State::Running,
            proposal_id: proposal_id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        assert_eq!(message.sender_id, 1);
        assert_eq!(message.message_type, MessageType::Proposal);
        assert_eq!(message.proposed_state, State::Running);
        assert_eq!(message.proposal_id, proposal_id);
    }

    #[tokio::test]
    async fn test_handle_proposal() {
        // Create a mock network implementation for testing
        let (tx, mut rx) = mpsc::channel(32);
        let mock_peers = HashMap::from([(2, "127.0.0.1:8081".to_string())]);

        struct MockNode {
            tx: mpsc::Sender<Message>,
            sent_messages: Arc<Mutex<Vec<Message>>>,
        }

        impl MockNode {
            async fn send_message(
                &self,
                message: &Message,
                _receiver: &str,
            ) -> Result<(), NodeError> {
                self.tx
                    .send(message.clone())
                    .await
                    .map_err(|_| NodeError::MessageSendFailed)?;
                Ok(())
            }
        }

        let sent_messages = Arc::new(Mutex::new(Vec::new()));

        let node = MockNode {
            tx: tx.clone(),
            sent_messages: sent_messages.clone(),
        };

        let proposal = Message {
            sender_id: 2,
            message_type: MessageType::Proposal,
            proposed_state: State::Running,
            proposal_id: Uuid::new_v4().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        // Create and send acknowledgment
        let ack = Message {
            sender_id: 1,
            message_type: MessageType::Acknowledgment,
            proposed_state: proposal.proposed_state.clone(),
            proposal_id: proposal.proposal_id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        if let Err(e) = node.send_message(&ack, "127.0.0.1:8081").await {
            panic!("Failed to send acknowledgment: {:?}", e);
        }

        // Check if acknowledgment was sent
        match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(msg)) => {
                assert_eq!(msg.message_type, MessageType::Acknowledgment);
                assert_eq!(msg.proposal_id, proposal.proposal_id);
            }
            Ok(None) => panic!("Channel closed"),
            Err(_) => panic!("Timeout waiting for acknowledgment"),
        }
    }

    #[tokio::test]
    async fn test_handle_commit() {
        let node = create_test_node(1, "127.0.0.1:8080").await;

        let commit_message = Message {
            sender_id: 2,
            message_type: MessageType::Commit,
            proposed_state: State::Running,
            proposal_id: Uuid::new_v4().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        // Initial state should be Init
        {
            let node_lock = node.lock().await;
            let state = node_lock.state.lock().await;
            assert_eq!(*state, State::Init);
        }

        // Handle commit message
        node.lock().await.handle_commit(commit_message).await;

        // State should be updated to Running
        {
            let node_lock = node.lock().await;
            let state = node_lock.state.lock().await;
            assert_eq!(*state, State::Running);
        }
    }

    #[tokio::test]
    async fn test_invalid_state_transition() {
        let node = create_test_node(1, "127.0.0.1:8080").await;

        // Try to transition from Init to Stopped (invalid)
        let commit_message = Message {
            sender_id: 2,
            message_type: MessageType::Commit,
            proposed_state: State::Stopped,
            proposal_id: Uuid::new_v4().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        // Initial state should be Init
        {
            let node_lock = node.lock().await;
            let state = node_lock.state.lock().await;
            assert_eq!(*state, State::Init);
        }

        // Handle commit message
        node.lock().await.handle_commit(commit_message).await;

        // State should still be Init
        {
            let node_lock = node.lock().await;
            let state = node_lock.state.lock().await;
            assert_eq!(*state, State::Init);
        }
    }

    // Integration test
    #[tokio::test]
    async fn test_node_communication() {
        let (tx1, mut rx1) = mpsc::channel(32);
        let (tx2, mut rx2) = mpsc::channel(32);

        // Use in-memory channels instead of TCP for testing
        let (network_tx1, mut network_rx1) = mpsc::channel(32);
        let (network_tx2, mut network_rx2) = mpsc::channel(32);

        struct TestNode {
            id: u64,
            tx: mpsc::Sender<Message>,
            network_tx: mpsc::Sender<Message>,
        }

        impl TestNode {
            async fn send_message(
                &self,
                message: &Message,
                _receiver: &str,
            ) -> Result<(), NodeError> {
                self.network_tx
                    .send(message.clone())
                    .await
                    .map_err(|_| NodeError::MessageSendFailed)?;
                Ok(())
            }
        }

        let node1 = Arc::new(TestNode {
            id: 1,
            tx: tx1,
            network_tx: network_tx2, // Send to node2's network receiver
        });

        let node2 = Arc::new(TestNode {
            id: 2,
            tx: tx2,
            network_tx: network_tx1, // Send to node1's network receiver
        });

        // Spawn message handlers
        let node1_clone = Arc::clone(&node1);
        let node2_clone = Arc::clone(&node2);

        tokio::spawn(async move {
            while let Some(msg) = network_rx1.recv().await {
                node1_clone.tx.send(msg).await.unwrap();
            }
        });

        tokio::spawn(async move {
            while let Some(msg) = network_rx2.recv().await {
                node2_clone.tx.send(msg).await.unwrap();
            }
        });

        // Test message sending
        let test_message = Message {
            sender_id: 1,
            message_type: MessageType::Proposal,
            proposed_state: State::Running,
            proposal_id: Uuid::new_v4().to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: None,
        };

        // Send message from node1 to node2
        node1
            .send_message(&test_message, "dummy_address")
            .await
            .unwrap();

        // Check if node2 received the message
        match tokio::time::timeout(Duration::from_secs(1), rx2.recv()).await {
            Ok(Some(msg)) => {
                assert_eq!(msg.message_type, MessageType::Proposal);
                assert_eq!(msg.proposed_state, State::Running);
                assert_eq!(msg.sender_id, 1);
            }
            Ok(None) => panic!("Channel closed"),
            Err(_) => panic!("Timeout waiting for message"),
        }
    }
}
