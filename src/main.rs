use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
};

use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    spawn,
    sync::{mpsc, Mutex},
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
}

fn main() {}
