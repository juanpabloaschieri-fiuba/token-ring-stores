use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use actix::Addr;

use crate::ecomerce::Ecommerce;
use crate::token_ring_messages::{
    ByteConvertable, MsgId, NodeConnection, RingMessage, SetConnectionStatus,
};
use crate::token_ring_receiver::TokenRingReceiver;
use crate::token_ring_sender::TokenRingSender;
use colored::*;

pub type SharedRingAddresses = Arc<Mutex<HashMap<u16, SocketAddr>>>;
pub type SharedRingTopology = Arc<Mutex<VecDeque<u16>>>;

#[derive(Debug, PartialEq, Eq)]
pub enum TokenRingError {
    ErrorTimeOut,
    ErrorDisconnected,
    ErrorWindowFull,
    ErrorSendingMsg,
    ErrorSharingData,
    ErrorTimedOutTooManyTimes,
    ErrorReceivingMsg,
    ErrorInvalidByteSequence,
    ErrorReceivedFromUnknownSource,
    ErrorInitializingNode,
    ErrorSettingStatus,
    ErrorJoiningThread,
    EndOfProgram,
}

pub struct TokenRing {
    sender_join_handle: JoinHandle<Result<(), TokenRingError>>,
    receiver_join_handle: JoinHandle<Result<(), TokenRingError>>,
    topology: SharedRingTopology,
    node_addr: SocketAddr,
    socket: UdpSocket,
}

impl TokenRing {
    pub fn new(
        node_addr: SocketAddr,
        ring_node_addr: Option<SocketAddr>,
        ecommerce_addr: Addr<Ecommerce>,
        msg_sender: Sender<RingMessage>,
        msg_receiver: Receiver<RingMessage>,
    ) -> Result<TokenRing, TokenRingError> {
        let socket =
            UdpSocket::bind(node_addr).map_err(|_| TokenRingError::ErrorInitializingNode)?;
        let mut topology = VecDeque::new();
        let mut addresses = HashMap::new();
        let mut stablished_ring = true;
        let next_peer = match ring_node_addr {
            Some(ring_node_addr) => {
                topology.extend([0, u16::MAX]);
                addresses.insert(0, ring_node_addr);
                addresses.insert(u16::MAX, node_addr);
                ring_node_addr
            }
            None => {
                topology.push_back(0);
                addresses.insert(0, node_addr);
                stablished_ring = false;
                node_addr
            }
        };
        let node_id = *topology
            .back()
            .ok_or(TokenRingError::ErrorInitializingNode)?;
        let topology = Arc::new(Mutex::from(topology));
        let addresses = Arc::new(Mutex::from(addresses));
        let cloned_socket1 = socket
            .try_clone()
            .map_err(|_| TokenRingError::ErrorInitializingNode)?;
        let cloned_socket2 = socket
            .try_clone()
            .map_err(|_| TokenRingError::ErrorInitializingNode)?;
        let created_messages = SharedCreatedMessages::new();
        let tr_sender = TokenRingSender::new(
            node_id,
            msg_receiver,
            topology.clone(),
            addresses.clone(),
            created_messages.clone(),
            cloned_socket1,
            next_peer,
        );
        let sender_join_handle =
            thread::spawn(move || run_sender(tr_sender, stablished_ring, node_addr));
        let tr_receiver = TokenRingReceiver::new(
            node_id,
            msg_sender,
            topology.clone(),
            addresses.clone(),
            created_messages,
            socket,
            ecommerce_addr,
        );
        let receiver_join_handle = thread::spawn(move || run_receiver(tr_receiver));

        print("Created".to_string());
        Ok(TokenRing {
            sender_join_handle,
            receiver_join_handle,
            topology,
            node_addr,
            socket: cloned_socket2,
        })
    }

    pub fn disconnect(&self) -> Result<(), TokenRingError> {
        self.set_connection_status(false, None)
    }

    pub fn connect(&self, addr: Option<SocketAddr>) -> Result<(), TokenRingError> {
        self.set_connection_status(true, addr)
    }

    fn set_connection_status(
        &self,
        connected: bool,
        addr: Option<SocketAddr>,
    ) -> Result<(), TokenRingError> {
        let msg =
            RingMessage::from_set_connection_status(SetConnectionStatus::new(connected, addr));
        self.socket
            .send_to(&msg.to_bytes(), self.node_addr)
            .map_err(|_| TokenRingError::ErrorSettingStatus)?;
        Ok(())
    }

    pub fn stop(self) -> Result<(), TokenRingError> {
        let msg = RingMessage::new_end_of_program();
        self.socket
            .send_to(&msg.to_bytes(), self.node_addr)
            .map_err(|_| TokenRingError::ErrorSendingMsg)?;

        let sender_res = self
            .sender_join_handle
            .join()
            .map_err(|_| TokenRingError::ErrorJoiningThread)?;
        print(format!("Sender finished: {:?}", sender_res));
        let receiver_res = self
            .receiver_join_handle
            .join()
            .map_err(|_| TokenRingError::ErrorJoiningThread)?;
        print(format!("Receiver Finished: {:?}", receiver_res));

        Ok(())
    }

    pub fn print_topology(&self) -> Result<(), TokenRingError> {
        let topology = self
            .topology
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut string = "Topology: [".to_string();
        for peer in topology.iter() {
            string = format!("{}{:?},", string, peer);
        }
        string.pop();
        string.push(']');
        print(string);
        Ok(())
    }
}

fn run_sender(
    mut tr_sender: TokenRingSender,
    stablished_ring: bool,
    node_addr: SocketAddr,
) -> Result<(), TokenRingError> {
    if stablished_ring {
        let msg = RingMessage::from_node_connection(NodeConnection::start(node_addr));
        tr_sender.process_msg(msg)?;
    }

    let mut res = Ok(());
    loop {
        tr_sender.rcv_and_process()?;
        if let Err(error) = tr_sender.rcv_and_process() {
            match error {
                TokenRingError::EndOfProgram => break,
                _ => {
                    res = Err(error);
                    break;
                }
            };
        }
    }
    print(format!("Sender finished: {:?}", res));
    res
}

fn run_receiver(mut tr_receiver: TokenRingReceiver) -> Result<(), TokenRingError> {
    let mut res = Ok(());
    loop {
        if let Err(err) = tr_receiver.recv_and_handle() {
            match err {
                TokenRingError::EndOfProgram => {}
                _ => res = Err(err),
            };
            break;
        }
    }
    tr_receiver.signal_end_of_program_to_sender();
    print(format!("receiver finished: {:?}", res));
    res
}

#[derive(Debug, Clone, Default)]
pub struct SharedCreatedMessages {
    created_messages: Arc<Mutex<HashMap<MsgId, RingMessage>>>,
}

impl SharedCreatedMessages {
    pub fn new() -> SharedCreatedMessages {
        SharedCreatedMessages {
            created_messages: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn insert(
        &mut self,
        id: MsgId,
        msg: RingMessage,
    ) -> Result<Option<RingMessage>, TokenRingError> {
        Ok(self
            .created_messages
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?
            .insert(id, msg))
    }

    pub fn remove(&mut self, id: &MsgId) -> Result<Option<RingMessage>, TokenRingError> {
        Ok(self
            .created_messages
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?
            .remove(id))
    }

    pub fn len(&self) -> Result<usize, TokenRingError> {
        Ok(self
            .created_messages
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?
            .len())
    }

    pub fn is_empty(&self) -> Result<bool, TokenRingError> {
        Ok(self
            .created_messages
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?
            .is_empty())
    }

    pub fn contains(&self, msg_id: &MsgId) -> Result<bool, TokenRingError> {
        Ok(self
            .created_messages
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?
            .contains_key(msg_id))
    }

    pub fn remove_all_from(&mut self, node_id: u16) -> Result<(), TokenRingError> {
        let mut created_messages = self
            .created_messages
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?;
        created_messages.retain(|id, _msg| id.node_id != node_id);

        Ok(())
    }

    pub fn reset(&mut self) -> Result<(), TokenRingError> {
        let mut created_messages = self
            .created_messages
            .lock()
            .map_err(|_| TokenRingError::ErrorSharingData)?;
        *created_messages = HashMap::new();
        Ok(())
    }
}

fn print(string: String) {
    println!("{} {}", "[TokenRingNode]".purple(), string);
}
