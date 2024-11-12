use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    spawn,
    sync::{mpsc, Mutex},
    time::sleep,
};

use uuid::Uuid;

use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Serialize, Deserialize, Debug)]
enum MessageType {
    Proposal,
    Acknowledgment,
    Commit,
}

#[derive(Serialize, Deserialize, Debug)]
enum State {
    Init,
    Running,
    Stopped,
}
#[derive(Serialize, Deserialize, Debug)]
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
    proposal_acknowledgement: Arc<Mutex<HashMap<String, HashSet<u64>>>>,
}

impl Node {
    async fn send_message(&self, message: &Message, reciever: &str) -> io::Result<()> {
        let mut stream = TcpStream::connect(reciever).await?;
        let ser_message = serde_json::to_vec(message)?;
        stream.write_all(&ser_message).await
    }

    async fn broadcast_proposal(&self, proposed_state: State) {
        let proposal_id = Uuid::new_v4().to_string();
        let message = Message {
            sender_id: self.id,
            message_type: MessageType::Proposal,
            proposed_state: proposed_state,
            proposal_id: proposal_id.clone(),
        };

        let mut proposal_acknowledgement = self.proposal_acknowledgement.lock().await;
        proposal_acknowledgement.insert(proposal_id.clone(), HashSet::new());

        for address in self.peers.values() {
            if let Err(e) = self.send_message(&message, &address) {
                eprintln!("Failed to send message to {}: {:?}", address, e);
            };
        }

        self.wait_for_acknowledgements(proposal_id).await;
    }

    async fn listen(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        println!("Node {} listening on {}", self.id, self.address);

        loop {
            let (mut socket, _) = listener.accept().await?;
            let tx = self.tx.clone();
            spawn(async move {
                let mut buf = [0u8, 1024];
                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) => break, // Connection closed,
                        Ok(n) => {
                            if let Ok(message) = serde_json::from_slice::<Message>(&buf[..n]) {
                                tx.send(message)
                                    .await
                                    .expect("Failed to send message to channel");
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    }

    async fn handle_incoming_messages(&self, mut rx: mpsc::Receiver<Message>) {
        while let Some(message) = rx.recv().await {
            match message.message_type {
                MessageType::Proposal => {
                    // Handle proposal: Send acknowledgment back
                }
                MessageType::Acknowledgment => {
                    // Track acknowledgment and check for consensus
                }
                MessageType::Commit => {
                    // Commit the proposed state change
                }
                _ => {}
            }
        }
    }

    async fn wait_for_acknowledgements(&self, proposal_id: String) {
        let majority = (self.peers.len() / 2) + 1;

        loop {
            let ack_count = {
                let acks = self.proposal_acknowledgement.lock().await;
                acks.get(&proposal_id).map(|acks| acks.len()).unwrap()
            };

            if ack_count >= majority {
                // Commit the proposal
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn simulate_client_interaction() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await.unwrap(); // Connect to Node 1
    let proposal_message = Message {
        sender_id: 999,
        message_type: MessageType::Proposal,
        proposed_state: State::Running,
        proposal_id: Uuid::new_v4().to_string(),
    };

    let serialized_message = serde_json::to_vec(&proposal_message).unwrap();
    stream.write_all(&serialized_message).await.unwrap();
    stream.flush().await?; // Ensure the message is sent immediately
    println!("Simulated client sent proposal to Node 1");
    Ok(())
}

#[tokio::main]
async fn main() {
    let state = Arc::new(Mutex::new(State::Init));
    let proposal_acknowledgments = Arc::new(Mutex::new(HashMap::new()));

    let (sender1, receiver1) = mpsc::channel(32);

    let node1 = Arc::new(Node {
        id: 1,
        state: state.clone(),
        peers: HashMap::from([(2, "0.0.0.0:8081".to_string())]),
        tx: sender1,
        address: "0.0.0.0:8080".to_string(),
        proposal_acknowledgement: proposal_acknowledgments.clone(),
    });

    let (sender2, receiver2) = mpsc::channel(32);

    let node2 = Arc::new(Node {
        id: 2,
        state: state.clone(),
        peers: HashMap::from([(1, "0.0.0.0:8080".to_string())]),
        tx: sender2,
        address: "0.0.0.0:8081".to_string(),
        proposal_acknowledgement: proposal_acknowledgments,
    });

    let node1_clone_for_messages = Arc::clone(&node1);
    spawn(async move {
        node1_clone_for_messages
            .handle_incoming_messages(receiver1)
            .await;
    });

    let node2_clone_for_messages = Arc::clone(&node1);
    spawn(async move {
        node2_clone_for_messages
            .handle_incoming_messages(receiver2)
            .await;
    });

    // Listen for incoming connections
    let node1_clone_for_listen = Arc::clone(&node1);
    tokio::spawn(async move {
        node1_clone_for_listen
            .listen()
            .await
            .expect("Node 1 failed to listen");
    });

    let node2_clone_for_listen = Arc::clone(&node2);
    tokio::spawn(async move {
        node2_clone_for_listen
            .listen()
            .await
            .expect("Node 2 failed to listen");
    });

    // Ensure the servers have time to start up
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Use the original `node1` Arc to broadcast a proposal
    node1.broadcast_proposal(State::Running).await;

    // Start the simulation after a short delay to ensure nodes are listening
    tokio::time::sleep(Duration::from_secs(2)).await;
    if let Err(e) = simulate_client_interaction().await {
        eprintln!("Failed to simulate client: {:?}", e);
    }
}
