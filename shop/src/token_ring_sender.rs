use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::collections::{VecDeque, HashMap};
use std::time::Duration;
use chrono::Utc;
use colored::*;

use crate::token_ring::*;
use crate::token_ring_messages::*;

const WINDOW_SIZE: u8 = 15;
const PEER_TIMEOUT_MS: i64 = 1000;
const MAX_PEER_TIMEOUT : u8 = 4;


/// Struct that represents the Token Ring Sender to send messages in the Token Ring.
///
/// # Attributes
/// * `message_window` - The window used to send the messages.
/// * `messages_to_send` - The messages that are queued to be sent if the window is full.
/// * `message_receiver` - The channel used to receive messages from the Token Ring.
/// * `created_messages` - The messages that were created by the node and are waiting to be acknowledged.
/// * `final_times_created_messages` - The times at which the created messages will time out.
/// * `ring_topology` - The topology of the ring.
/// * `ring_addresses` - The addresses of the nodes in the ring.
pub struct TokenRingSender{
    message_window: Window,
    messages_to_send: VecDeque<RingMessage>,
    message_receiver: Receiver<RingMessage>,
    created_messages: SharedCreatedMessages,
    final_times_created_messages: VecDeque<FinalTime>,
    ring_topology: SharedRingTopology,
    ring_addresses: SharedRingAddresses,
    connected: bool
}

impl TokenRingSender {
    /// Public constructor for the TokenRingSender.
    pub fn new(node_id: u16, rx: Receiver<RingMessage>, ring_topology: SharedRingTopology, ring_addresses: SharedRingAddresses, created_messages: SharedCreatedMessages, socket: UdpSocket, initial_next_peer_addr: SocketAddr) -> TokenRingSender {
        TokenRingSender {
            message_window: Window::new(node_id, WINDOW_SIZE,socket, initial_next_peer_addr),
            messages_to_send: VecDeque::new(),
            message_receiver: rx,
            created_messages,
            final_times_created_messages: VecDeque::new(),
            ring_topology,
            ring_addresses,
            connected: true
        }
    }

    /// Returns the smallest timeout of the messages in the window and the created messages.
    fn smallest_timeout(&self) -> Option<Duration> {
        let win_timeout = self.message_window.smallest_timeout();
        let created_messages_timeout = smallest_timeout(&self.final_times_created_messages);
        if win_timeout.is_none() && created_messages_timeout.is_none(){
            return None;
        }
        let win_timeout = win_timeout.unwrap_or(Duration::MAX);
        let created_messages_timeout = created_messages_timeout.unwrap_or(Duration::MAX);
        Some(win_timeout.min(created_messages_timeout))
    }

    /// Based on the smallest timeout, it blocks the execution until a message is received
    /// or the smallest timeout is reached. Returns the corresponding error if it timed out or
    /// if the channel was disconnected.
    fn recv_until_smallest_timeout(&self) -> Result<RingMessage, TokenRingError>{
        match self.smallest_timeout(){
            Some(timeout) => self.message_receiver.recv_timeout(timeout).map_err(|error|
                match error{
                    RecvTimeoutError::Timeout => TokenRingError::ErrorTimeOut,
                    RecvTimeoutError::Disconnected => TokenRingError::ErrorDisconnected,
                }
            ),
            None =>self.message_receiver.recv().map_err(|_| TokenRingError::ErrorDisconnected),
        }
    }

    /// Receives a RingMessage and it removes it from the window and from the
    /// messages to send.
    fn acknowledge_sent_msg(&mut self, msg: RingMessage)-> Option<RingMessage>{
        self.message_window.acknowledge_sent_msg(msg.header);
        self.messages_to_send.pop_front()
    }

    /// Receives a RingMessage and it adds it to the created messages.
    fn add_created_message(&mut self, msg: RingMessage) -> Result<(), TokenRingError>{
        if !msg.header.recently_created_by(self.message_window.node_id){
            return Ok(());
        }
        let nodes_in_ring = match self.ring_topology.lock(){
            Ok(topology) => topology.len() as i64,
            Err(_) => return Err(TokenRingError::ErrorSharingData),
        };
        self.final_times_created_messages.push_back(FinalTime::from_timeout(PEER_TIMEOUT_MS * nodes_in_ring * 3, msg.header.id));
        self.created_messages.insert(msg.header.id, msg)?;
        Ok(())
    }

    /// Receives a RingMessage and tries to send it to the next peer, if the window is full
    /// depending if the message is priority or not it is added to the front or back of the queue.
    fn send_with_priority(&mut self, mut msg: RingMessage, prioritice: bool)->Result<(),TokenRingError>{
        match self.message_window.send_to_next_peer(&mut msg){
            Ok(_) => self.add_created_message(msg)?,
            Err(error) => match error{
                TokenRingError::ErrorWindowFull => {
                    if prioritice{
                        self.messages_to_send.push_front(msg);
                    } else {
                        self.messages_to_send.push_back(msg);
                    }
                }
                _ => return Err(error),
            },
        }
        Ok(())
    }

    /// Tries to send a message to the next peer, if the window is full
    /// it is queued to be sent after all the messages in the queue.
    pub fn send(&mut self, msg: RingMessage) -> Result<(),TokenRingError>{
        self.send_with_priority(msg, false)
    }

    /// Handles if a message in the created messages timed out
    fn handle_created_messages_timeout(&mut self)-> Result<(), TokenRingError>{
        let final_time = match self.final_times_created_messages.pop_front(){
            Some(final_time) => {
                if final_time.remaining_time() > 0{
                    self.final_times_created_messages.push_front(final_time);
                    return Ok(());
                };
                final_time},
            None => return Ok(())
        };
        if let Some(msg) = self.created_messages.remove(&final_time.id)?{
            print(format!("Resending  {:?} de tipo {:?}, {:?}, tiene las firmas {:?}", self.message_window.next_peer,msg.header.msg_type, msg.header.id, msg.signatures));
            if msg.header.id.is_from_node(self.message_window.node_id){
                self.send_with_priority(msg, true)?;
            }
        }

        Ok(())
    }

    /// Handles the timeout of the window and it checks if the peer timed out too many times.
    /// If it did it proceeds to change the next peer.
    fn handle_timeout(&mut self) -> Result<(),TokenRingError>{
        if let Err(error) = self.message_window.handle_timeout(){
            match error{
                TokenRingError::ErrorTimedOutTooManyTimes => return self.handle_next_peer_disconection(),
                _=> return Err(error),
            }
        }

        self.handle_created_messages_timeout()
    }

    /// Deletes the next peer from the topology and addresses and returns the id and address of the next peer.
    fn delete_peer_from_topology(&mut self) -> Result<(u16, SocketAddr), TokenRingError>{
        print!("Deleting next peer from topology");

        let mut ring_topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut ring_addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;

        let removed_id = ring_topology.pop_front().ok_or(TokenRingError::ErrorDisconnected)?;
        ring_addresses.remove(&removed_id).ok_or(TokenRingError::ErrorDisconnected)?;

        let next_id = ring_topology.front().ok_or(TokenRingError::ErrorDisconnected)?;
        let addr = *ring_addresses.get(next_id).ok_or(TokenRingError::ErrorDisconnected)?;
        Ok((removed_id, addr))
    }

    /// Handles the disconection of an inactive peer and connects to the next peer in the topology.
    fn handle_next_peer_disconection(&mut self) -> Result<(),TokenRingError>{

        let (disconected_peer_id, next_addr) = self.delete_peer_from_topology()?;

        self.message_window.change_peer(next_addr)?;
        let node_disconection = NodeDisconnected::new(MsgId::from_node(self.message_window.node_id), Vec::new(), disconected_peer_id);
        let msg = RingMessage::from_node_disconected(node_disconection);
        self.send_with_priority(msg, true)?;

        Ok(())
    }
    
    /// Receives a RingMessage with the type NodeConnection and it updates the next peer of the window
    pub fn update_next_peer(&mut self, mut msg: RingMessage)->Result<Option<RingMessage>, TokenRingError>{

        let topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        
        if self.message_window.node_id == u16::MAX{
            self.final_times_created_messages.retain(|final_time| final_time.id.node_id != u16::MAX);
            self.created_messages.remove_all_from(u16::MAX).map_err(|_| TokenRingError::ErrorSharingData)?;
            self.message_window.node_id = *topology.back().ok_or(TokenRingError::ErrorDisconnected)?;
        }
        let previous_next_peer = self.message_window.next_peer;
        self.message_window.next_peer = addresses[topology.front().ok_or(TokenRingError::ErrorDisconnected)?];
        if previous_next_peer != self.message_window.next_peer{
            self.message_window.reset_final_times();
            if !msg.did_full_lap(self.message_window.node_id) {
                self.message_window.send_now(&mut msg, true)?;
                return Ok(None);
            }
        }
        Ok(Some(msg))
    }

    fn delete_topology(&mut self) -> Result<SocketAddr,TokenRingError>{
        let mut topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let node_addr = addresses[&self.message_window.node_id];
        *topology = VecDeque::from([u16::MAX]);
        *addresses = HashMap::new();
        addresses.insert(u16::MAX, node_addr);
        Ok(node_addr)
    }

    fn reset_node(&mut self) -> Result<(),TokenRingError> {
        let node_addr = self.delete_topology()?;
        self.message_window.reset(node_addr);
        self.final_times_created_messages = VecDeque::new();
        self.created_messages.reset()
    }

    fn notify_disconnection(&mut self)-> Result<(),TokenRingError> {
        let disconnection_msg = NodeDisconnected::new(MsgId::from_node(self.message_window.node_id), Vec::new(), self.message_window.node_id);
        let mut msg = RingMessage::from_node_disconected(disconnection_msg);
        self.message_window.send_now(&mut msg, true)?;
        Ok(())
    }

    fn reconnect(&mut self, msg: RingMessage)-> Result<(),TokenRingError> {
        let connect_msg = SetConnectionStatus::from_ring_message(msg)?;
        let node_addr = {
            let addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
            *addresses.get(&u16::MAX).ok_or(TokenRingError::ErrorDisconnected)?
        };
        let msg = match connect_msg.connect_to {
            Some(addr) => {
                print(format!("Entre a la rama del some  con addr: {:?}",addr));
                self.message_window.next_peer = addr;
                RingMessage::from_node_connection(NodeConnection::start(node_addr))},
            None => {
                println!("Mandamos el mensaje con la addr{:?}",node_addr);
                let msg = NodeConnection::new(MsgId::default(), Vec::new(), 1, node_addr, VecDeque::from([0]), vec![node_addr]);
                RingMessage::from_node_connection(msg)
            }
        };
        self.send(msg)
    }

    fn set_connection_status(&mut self, msg: RingMessage) -> Result<(),TokenRingError> {
        let msg = SetConnectionStatus::from_ring_message(msg)?;
        if !msg.new_connection_status && self.connected{
            self.connected = msg.new_connection_status;
            self.notify_disconnection()?;
            self.reset_node()?;
        }
        else if msg.new_connection_status && !self.connected{
            self.connected = msg.new_connection_status;
            let msg = RingMessage::from_set_connection_status(msg);
            self.reconnect(msg)?;
        }
        Ok(())
    }

    /// Receives a RingMessage and it processes it depending on the type of message.
    pub fn process_msg(&mut self, msg: RingMessage)-> Result<(),TokenRingError>{
        if !self.connected && msg.header.msg_type != RingMessageType::SetConnectionStatus{
            return Ok(())
        }

        let msg = match msg.header.msg_type{
            RingMessageType::ReceivedAck => self.acknowledge_sent_msg(msg),
            RingMessageType::SendAck => return self.send_acknowledgement(&msg),
            RingMessageType::SetConnectionStatus => return self.set_connection_status(msg),
            RingMessageType::NodeDisconnected => self.update_next_peer(msg)?,
            RingMessageType::NodeConnection => self.update_next_peer(msg)?,
            _ => Some(msg),
        };
        if let Some(msg) = msg{
            if !msg.did_full_lap(self.message_window.node_id) {
                self.send(msg)?;
            }
        };
        Ok(())
    }

    /// Listens for messages in the channel and it processes them.
    pub fn rcv_and_process(&mut self)->Result<(), TokenRingError>{
        let msg = self.recv_until_smallest_timeout();
        match msg{
            Ok(msg) => {
                match msg.header.msg_type{  
                    RingMessageType::EndOfProgram => return Err(TokenRingError::EndOfProgram),
                    _ => self.process_msg(msg)?,
                }
            }
            Err(error) => match error{
                TokenRingError::ErrorTimeOut => self.handle_timeout()?,
                _ => return Err(error),
            },
        };
        Ok(())
    }

    /// Given a MsgId it sends the coresponding ack message to the node that sent the message.
    fn send_acknowledgement(&mut self, ack_msg: &RingMessage )-> Result<(),TokenRingError>{
        if let Some(addr) = ack_msg.header.sender_addr{
            print(format!("El mensaje a enviar en el sender es de tipo {:?}, {:?}, se lo mandamos a {:?}", "ACK", ack_msg.header.id, ack_msg.header.sender_addr));
            self.message_window.socket.send_to(&ack_msg.to_bytes(), addr).map_err(|_| TokenRingError::ErrorSendingMsg)?;
        }

        Ok(())
    }
}

/// Struct that represents the time at which a message will time out.
#[derive(Eq, PartialEq)]
struct FinalTime{
    time: i64,
    amount_of_timeouts: u8,
    id: MsgId
}

impl FinalTime{
    /// Constructor for the FinalTime.
    fn new(time:i64, id: MsgId)->FinalTime{
        FinalTime {time, amount_of_timeouts: 0,id}
    }

    /// Constructor for the FinalTime, it receives a timeout and a MsgId and it creates a FinalTime
    /// with the current time plus the timeout.
    fn from_timeout(timeout:i64, id:MsgId)->FinalTime{
        FinalTime::new(Utc::now().timestamp_millis() + timeout, id)
    }

    /// Returns the remaining time for the FinalTime to time out.
    fn remaining_time(&self)->i64{
        (self.time - Utc::now().timestamp_millis()).max(0)
    }
}


impl Ord for FinalTime{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.time.cmp(&self.time)
    }
}

impl PartialOrd for FinalTime{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {Some(self.cmp(other))}
}

/// Implementation for a Message Window, used for the Token Ring Sender.
/// It is used to send the messages in the correct order and to resend them if they time out.
/// The messages are sent via an UDP Socket, and it holds the next peer address to send the messages to.
///
/// # Attributes
///
/// * `node_id` - The id of the node that owns the window.
/// * `window_size` - The size of the window.
/// * `messages` - The messages that are currently in the window.
/// * `final_times` - The times at which the messages in the window will time out.
/// * `next_id` - The next id that will be used to send a message.
/// * `socket` - The UDP Socket used to send the messages.
/// * `next_peer` - The address of the next peer to send the messages to.
struct Window{
    node_id: u16,
    window_size: u8,
    messages: HashMap<MsgId,RingMessage>,
    final_times: VecDeque<FinalTime>,
    next_seq_num: u16,
    socket: UdpSocket,
    next_peer: SocketAddr
}

impl Window{

    /// Constructor for the Window.
    fn new(node_id: u16, window_size:u8, socket: UdpSocket, next_peer: SocketAddr)->Window{
        Window {node_id, window_size, messages: HashMap::new(), final_times: VecDeque::new(),next_seq_num: 0, socket, next_peer}
    }

    /// Returns the smallest timeout of the messages in the window.
    fn smallest_timeout(&self)->Option<Duration>{
        smallest_timeout(&self.final_times)
    }

    /// Adds 1 to the next available id, if it is equal to u16
    /// u16::MAX-1 it is set to 0 to avoid overflow.
    fn increase_available_id(&mut self){
        if self.next_seq_num >= u16::MAX-1{
            self.next_seq_num = 0;
        }else{
            self.next_seq_num += 1;
        }
    }

    /// Returns the next available id, if it is already in use it increases it and returns the next one.
    fn next_available_id(&mut self) -> u16 {
        if self.messages.contains_key(&MsgId::new(self.node_id, self.next_seq_num)){
            self.increase_available_id();
            return self.next_available_id();
        }
        let available_id = self.next_seq_num;
        self.increase_available_id();
        available_id
    }

    /// Sends a message to the next peer, if the window is full it returns an error.
    fn send_to_next_peer(&mut self, msg: &mut RingMessage)->Result<(), TokenRingError>{
        if self.messages.len() >= self.window_size as usize{
            return Err(TokenRingError::ErrorWindowFull)
        }
        self.send_now(msg, true)
    }

    /// Receives a RingMessage, the Message is signed by the node that "owns" the window,
    /// it is sent via the socket to the next peer and the final time is set if it is not already in the window.
    fn send_now(&mut self, msg: &mut RingMessage, change_msg_id: bool)->Result<(), TokenRingError>{
        if change_msg_id{
            if msg.header.id.node_id == u16::MAX{
                msg.header.id.node_id = self.node_id;
            }
            if msg.header.is_from_node(self.node_id) && !self.messages.contains_key(&msg.header.id){
                msg.header.id.seq_num = self.next_available_id();
            }
        }

        msg.sign(self.node_id);
        print(format!("El next peer {:?} El mensaje a enviar en el sender es de tipo {:?}, {:?}, tiene las firmas {:?}", self.next_peer,msg.header.msg_type, msg.header.id, msg.signatures));
        self.socket.send_to(&msg.to_bytes(), self.next_peer).map_err(|_| TokenRingError::ErrorSendingMsg)?;
        if !self.messages.contains_key(&msg.header.id){
            self.final_times.push_back(FinalTime::from_timeout(PEER_TIMEOUT_MS, msg.header.id));
        }
        self.messages.insert(msg.header.id, msg.clone());
        Ok(())
    }

    /// Receives a RingMessage and it removes it from the window if it is in it.
    fn acknowledge_sent_msg(&mut self, msg: RingMessageHeader){
        if self.messages.remove(&msg.id).is_some(){
            self.remove_final_time(&msg.id)
        }
    }

    /// Receives a MsgId and it removes the final time associated to that MsgId.
    fn remove_final_time(&mut self, id: &MsgId){
        let position = self.final_times.iter().position(|time| time.id == *id);
        if let Some(position) = position{
            self.final_times.remove(position);
        }
    }

    /// Checks the lowest final time, if it timed out it sends the message again and increases
    /// the amount of timeouts, also updating the timeout of the resent message.
    /// If it timed out too many times it returns an error.
    fn handle_timeout(&mut self)->Result<(), TokenRingError>{
        let mut final_time = match self.final_times.pop_front(){
            Some(final_time) => {
                if final_time.remaining_time() > 0{
                    self.final_times.push_front(final_time);
                    return Ok(());
                };
                final_time},
            None => return Ok(())
        };

        final_time.amount_of_timeouts +=1;
        if final_time.amount_of_timeouts >= MAX_PEER_TIMEOUT{
            self.final_times.push_front(final_time);

            return Err(TokenRingError::ErrorTimedOutTooManyTimes)
        }

        if let Some(msg) = self.messages.remove(&final_time.id){
            print(format!("Se reenvia por TO de Window el mensaje al next peer {:?} de tipo {:?}, {:?}, tiene las firmas {:?}", self.next_peer,msg.header.msg_type, msg.header.id, msg.signatures));
            self.socket.send_to(&msg.to_bytes(), self.next_peer).map_err(|_| TokenRingError::ErrorSendingMsg)?;
            final_time.time = Utc::now().timestamp_millis() + PEER_TIMEOUT_MS;
            self.final_times.push_back(final_time);
            self.messages.insert(msg.header.id, msg);
        }

        Ok(())
    }

    /// Receives a SocketAddr representing the next peer to send messages through the UDP Socket
    /// and it sends all the messages in the window to that peer.
    fn change_peer(&mut self, next_peer: SocketAddr) -> Result<(), TokenRingError>{
        self.next_peer = next_peer;
        self.resend_all()
    }

    /// Resends all the messages in the window.
    fn resend_all(&mut self) -> Result<(), TokenRingError>{
        let mut messages = Vec::new();

        while let Some(final_time) = self.final_times.pop_front(){
            if let Some(msg) = self.messages.remove(&final_time.id){
                messages.push(msg);
            }
        }

        for mut msg in messages{
            self.send_now(&mut msg, false)?
        }
        Ok(())
    }

    /// Resets the final times of the messages in the window.
    fn reset_final_times(&self)->Vec<FinalTime>{
        self.final_times.iter().map(|final_time| FinalTime::new(PEER_TIMEOUT_MS, final_time.id)).collect()
    }

    fn reset(&mut self, next_peer: SocketAddr){
        self.node_id = u16::MAX;
        self.messages =  HashMap::new();
        self.final_times =  VecDeque::new();
        self.next_seq_num = 0;
        self.next_peer = next_peer;
    }

}

/// Returns the smallest timeout of a queue of FinalTimes.
fn smallest_timeout(final_times: &VecDeque<FinalTime>)->Option<Duration>{
    if final_times.is_empty(){
        return None
    }

    let time_out = final_times[0].time - Utc::now().timestamp_millis();
    let time_out = time_out.max(0) as u64;
    Some(Duration::from_millis(time_out))
}

fn print(string: String){
    println!("{} {} \n-------------------------------------------------------------------","[TokenRingSender] ".blue(), string);

}

#[cfg(test)]
pub mod test {
    use std::{sync::{mpsc, Arc, Mutex}, str::FromStr, net::IpAddr};
    use std::sync::mpsc::Sender;
    use super::*;

    pub fn create_topology(amount_of_peers: u16, port: u16)->(SharedRingTopology, SharedRingAddresses){
        let mut ring_topology = VecDeque::new();
        let mut ring_addresses = HashMap::new();
        for i in 0..amount_of_peers{
            ring_topology.push_back(i+1);
            let addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port+i+1);
            ring_addresses.insert(i+1, addr);
        }
        let addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port);
        ring_topology.push_back(0);
        ring_addresses.insert(0, addr);

        let ring_topology = Arc::new(Mutex::from(ring_topology));
        let ring_addresses = Arc::new(Mutex::from(ring_addresses));
        (ring_topology, ring_addresses)
    }

    fn create_token_ring_sender(amount_of_peers: u16, port: u16)->(TokenRingSender, Sender<RingMessage>){
        let (tx, rx) = mpsc::channel();

        let (ring_topology, ring_addresses) = create_topology(amount_of_peers, port);

        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port)).unwrap();
        let mut initial_next_peer_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port);
        if amount_of_peers != 0{
            initial_next_peer_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port+1);
        }
        let created_messages = SharedCreatedMessages::new();
        let token_ring_sender = TokenRingSender::new(0, rx, ring_topology, ring_addresses,created_messages, socket, initial_next_peer_addr);
        (token_ring_sender, tx)
    }

    fn create_n_messages_from(amount_of_msg: u8, node_id: u16) -> Vec<RingMessage>{
        create_n_messages_with_addr_from(amount_of_msg, node_id, "")
    }


    fn create_n_messages_with_addr_from(amount_of_msg: u8, node_id: u16, addr: &str) -> Vec<RingMessage>{
        let mut messages: Vec<RingMessage> = vec![];
        let addr = SocketAddr::from_str(addr).ok();
        for _i in 0..amount_of_msg{
            let msg_header = RingMessageHeader::new(MsgId::new(node_id,u16::MAX), RingMessageType::CoordinateRequest, 0, 0);
            let mut msg = RingMessage::new(msg_header, vec![0,1,2,3,4,5]);
            msg.header.sender_addr = addr.clone();
            messages.push(msg)
        }

        messages
    }

    fn receive_udp_package_from(socket: &UdpSocket)->std::io::Result<(Vec<u8>, SocketAddr)>{
        let mut buffer = [0;65535];
        let (amount_of_bytes, addr) = socket.recv_from(&mut buffer)?;
        let (received_bytes, _) =  buffer.split_at(amount_of_bytes);
        Ok((Vec::from(received_bytes), addr))
    }

    #[test]
    fn test_1_when_sending_messages_they_are_added_to_the_window()->Result<(), TokenRingError>{
        let (mut tr_sender, _msg_sender) = create_token_ring_sender(3, 11000);
        let msg = create_n_messages_from(1,0).remove(0);

        assert_eq!(tr_sender.message_window.messages.len(), 0);
        tr_sender.send(msg)?;
        assert_eq!(tr_sender.message_window.messages.len(), 1);
        Ok(())
    }

    #[test]
    fn test_2_send_message_to_next_peer()->Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,12000);
        let mut msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:12001").unwrap();

        tr_sender.send(msg.clone())?;
        let (mut received_msg_bytes, _) = receive_udp_package_from(&socket).unwrap();

        msg.sign(0);
        received_msg_bytes[2] = 255;
        received_msg_bytes[3] = 255;
        assert_eq!(received_msg_bytes, msg.to_bytes());

        Ok(())
    }

    #[test]
    fn test_3_sending_messages_with_a_full_window_does_not_send_it_and_adds_it_to_the_queue()->Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,13000);
        let msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:13001").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        for _i in 0..WINDOW_SIZE+1{
            tr_sender.send(msg.clone())?;
        }

        for _i in 0..WINDOW_SIZE{
            let (received_bytes, _addr) = receive_udp_package_from(&socket).unwrap();
            assert!(!received_bytes.is_empty());
        }
        assert!(receive_udp_package_from(&socket).is_err());

        assert_eq!(tr_sender.messages_to_send.len(), 1);
        Ok(())
    }

    #[test]
    fn test_4_sending_multiple_messages_increments_id()->Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3, 14000);
        let messages = create_n_messages_from(5,0);

        for msg in messages{
            tr_sender.send(msg)?;
        }

        for i in 0..5{
            assert!(tr_sender.message_window.messages.contains_key(&MsgId::new(0, i)));
        }

        Ok(())
    }

    #[test]
    fn test_5_acks_remove_from_window_and_sends_queued_messages() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,15000);
        let msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:15001").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        for _i in 0..WINDOW_SIZE+1{
            tr_sender.send(msg.clone())?;
        }

        let ack = RingMessage::new_ack(MsgId::new(0, 0),RingMessageType::ReceivedAck);
        tr_sender.process_msg(ack)?;

        assert_eq!(tr_sender.message_window.messages.len(), WINDOW_SIZE as usize);
        assert_eq!(tr_sender.message_window.final_times.len(), WINDOW_SIZE as usize);

        for i in 1..WINDOW_SIZE+1{
            let ack = RingMessage::new_ack(MsgId::new(0, i as u16), RingMessageType::ReceivedAck);
            tr_sender.process_msg(ack)?;
        }
        assert_eq!(tr_sender.message_window.messages.len(), 0);
        assert_eq!(tr_sender.message_window.final_times.len(), 0);

        for _i in 0..WINDOW_SIZE+1{
            receive_udp_package_from(&socket).unwrap();
        }
        Ok(())
    }

    #[test]
    fn test_6_when_a_message_does_a_peer_time_out_it_is_sent_again() -> Result<(), TokenRingError> {
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3, 16000);
        let mut msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:16001").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        tr_sender.send(msg.clone())?;
        tr_sender.send(msg.clone())?;
        receive_udp_package_from(&socket).unwrap();
        receive_udp_package_from(&socket).unwrap();

        tr_sender.rcv_and_process()?;
        tr_sender.rcv_and_process()?;
        let (received_bytes_msg1 ,_) = receive_udp_package_from(&socket).unwrap();
        let (received_bytes_msg2 ,_) = receive_udp_package_from(&socket).unwrap();

        msg.sign(0);
        let mut msg1 = msg.clone();
        let mut msg2 = msg;
        msg1.header.id.seq_num = 0;
        msg2.header.id.seq_num = 1;

        assert_eq!(msg1.to_bytes(),received_bytes_msg1);
        assert_eq!(msg2.to_bytes(),received_bytes_msg2);
        assert_eq!(tr_sender.message_window.final_times[0].amount_of_timeouts, 1);
        assert_eq!(tr_sender.message_window.final_times[1].amount_of_timeouts, 1);

        assert!(receive_udp_package_from(&socket).is_err());

        Ok(())

    }

    #[test]
    fn test_7_when_a_message_times_out_too_many_times_the_next_peer_is_changed_and_messages_resended() -> Result<(), TokenRingError> {
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(10, 17000);
        let msg = create_n_messages_from(1,0).remove(0);
        let first_socket = UdpSocket::bind("127.0.0.1:17001").unwrap();
        let second_socket = UdpSocket::bind("127.0.0.1:17002").unwrap();
        first_socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        second_socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        tr_sender.send(msg.clone())?;
        tr_sender.send(msg.clone())?;

        for i in 0..2*(MAX_PEER_TIMEOUT-1)+1{
            println!("{i}");
            tr_sender.rcv_and_process()?;
        }

        for _i in 0..2*MAX_PEER_TIMEOUT{
            receive_udp_package_from(&first_socket).unwrap();
        }

        assert!(receive_udp_package_from(&first_socket).is_err());

        for _i in 0..3{
            receive_udp_package_from(&second_socket).unwrap();
        }
        assert!(receive_udp_package_from(&second_socket).is_err());


        assert_eq!(tr_sender.message_window.next_peer, SocketAddr::from_str("127.0.0.1:17002").unwrap());
        Ok(())
    }



    #[test]
    fn test_8_send_message_adds_it_to_the_queue_of_created_messages() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,18000);
        let msg = create_n_messages_from(1,0).remove(0);

        for _i in 0..WINDOW_SIZE+1{
            tr_sender.send(msg.clone())?;
        }

        assert_eq!(tr_sender.created_messages.len()?, WINDOW_SIZE as usize);
        assert_eq!(tr_sender.final_times_created_messages.len(), WINDOW_SIZE as usize);

        Ok(())
    }

    #[test]

    fn test_9_resending_messages_doesnt_add_to_created_messages() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,19000);
        let msg = create_n_messages_from(1,2).remove(0);

        tr_sender.send(msg)?;

        assert!(tr_sender.created_messages.is_empty()?);
        assert!(tr_sender.final_times_created_messages.is_empty());
        assert!(!tr_sender.message_window.messages.is_empty());
        assert!(!tr_sender.message_window.final_times.is_empty());
        Ok(())
    }

    #[test]
    fn test_10_timeout_of_created_and_sent_msg() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,20000);
        let msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:20001").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        tr_sender.send(msg)?;
        tr_sender.acknowledge_sent_msg(RingMessage::new_ack(MsgId::new(0, 0),RingMessageType::ReceivedAck));

        tr_sender.rcv_and_process()?;
        receive_udp_package_from(&socket).unwrap();
        receive_udp_package_from(&socket).unwrap();
        assert!(receive_udp_package_from(&socket).is_err());
        Ok(())
    }
    #[test]
    fn test_11_receive_msg_from_myself_in_disconected_topology() -> Result<(), TokenRingError>{
        let (mut tr_sender,  msg_sender) = create_token_ring_sender(0,21000);
        let mut msg = create_n_messages_with_addr_from(1,0,"127.0.0.1:21000").remove(0);
        tr_sender.message_window.socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        tr_sender.send(msg.clone())?;

        msg.sign(0);
        msg.header.id.seq_num = 0;
        receive_udp_package_from(&tr_sender.message_window.socket).unwrap();
        let mut ack_msg = RingMessage::new_ack(MsgId::new(0, 0), RingMessageType::SendAck);
        ack_msg.header.sender_addr = Some(SocketAddr::from_str("127.0.0.1:21000").unwrap());
        msg_sender.send(ack_msg).unwrap();
        tr_sender.rcv_and_process()?;

        receive_udp_package_from(&tr_sender.message_window.socket).unwrap();

        assert!(receive_udp_package_from(&tr_sender.message_window.socket).is_err());
        Ok(())
    }

    #[test]
    fn test_12_create_disconected_topology_by_pointing_to_yourself()-> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(1,22000);
        let msg = create_n_messages_from(1,0).remove(0);
        tr_sender.message_window.socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        tr_sender.send(msg)?;

        for i in 0..MAX_PEER_TIMEOUT{
            println!("{i}");
            tr_sender.rcv_and_process()?;
        }

        println!("{:?}",tr_sender.message_window.next_peer);
        assert_eq!(tr_sender.message_window.next_peer,SocketAddr::from_str("127.0.0.1:22000").unwrap());
        receive_udp_package_from(&tr_sender.message_window.socket).unwrap();
        receive_udp_package_from(&tr_sender.message_window.socket).unwrap();
        assert!(receive_udp_package_from(&tr_sender.message_window.socket).is_err());
        Ok(())
    }

    #[test]
    fn test_13_resend_correct_msg_after_ring_timeout() -> Result<(), TokenRingError> {
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3, 23000);
        let mut msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:23001").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        tr_sender.send(msg.clone())?;
        tr_sender.send(msg.clone())?;

        let (received_bytes_msg1 ,_) = receive_udp_package_from(&socket).unwrap();
        let (received_bytes_msg2 ,_) = receive_udp_package_from(&socket).unwrap();
        msg.sign(0);
        let mut msg1 = msg.clone();
        let mut msg2 = msg;
        msg1.header.id.seq_num = 0;
        msg2.header.id.seq_num = 1;
        assert_eq!(msg1.to_bytes(),received_bytes_msg1);
        assert_eq!(msg2.to_bytes(),received_bytes_msg2);
        tr_sender.acknowledge_sent_msg(RingMessage::new_ack(MsgId::new(0, 0),RingMessageType::ReceivedAck));
        tr_sender.acknowledge_sent_msg(RingMessage::new_ack(MsgId::new(0, 1),RingMessageType::ReceivedAck));

        tr_sender.rcv_and_process()?;
        tr_sender.rcv_and_process()?;

        msg1.header.id.seq_num = 2;
        msg2.header.id.seq_num = 3;
        let (received_bytes_msg1 ,_) = receive_udp_package_from(&socket).unwrap();
        let (received_bytes_msg2 ,_) = receive_udp_package_from(&socket).unwrap();

        assert_eq!(msg1.to_bytes(),received_bytes_msg1);
        assert_eq!(msg2.to_bytes(),received_bytes_msg2);
        assert_eq!(tr_sender.message_window.final_times[0].amount_of_timeouts, 0);
        assert_eq!(tr_sender.message_window.final_times[1].amount_of_timeouts, 0);

        assert!(receive_udp_package_from(&socket).is_err());

        Ok(())

    }

    #[test]
    fn test_14_send_ack() -> Result<(), TokenRingError> {
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3, 24000);
        let mut msg = RingMessage::new_ack(MsgId::new(3,0), RingMessageType::SendAck);
        msg.header.sender_addr = Some(SocketAddr::from_str("127.0.0.1:24003").unwrap());
        let socket = UdpSocket::bind("127.0.0.1:24003").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        tr_sender.process_msg(msg)?;

        let (received_bytes_msg,_) = receive_udp_package_from(&socket).unwrap();
        assert_eq!(received_bytes_msg, RingMessage::new_ack(MsgId::new(3, 0), RingMessageType::SendAck).to_bytes());

        Ok(())
    }

    #[test]
    fn test_15_new_node_connection_is_forwarded_for_first_time() -> Result<(), TokenRingError> {
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(1, 25000);
        let new_node_addr = SocketAddr::from_str("127.0.0.1:25002").unwrap();
        let socket = UdpSocket::bind("127.0.0.1:25001").unwrap();
        let nc_msg = NodeConnection::start(new_node_addr);
        let mut msg = RingMessage::from_node_connection(nc_msg);

        tr_sender.process_msg(msg.clone())?;

        msg.header.id = MsgId::new(0,0);
        msg.sign(0);

        let (received_bytes_msg,_) = receive_udp_package_from(&socket).unwrap();
        let received_msg = RingMessage::from_bytes(&received_bytes_msg)?;
        assert_eq!(msg, received_msg);
        Ok(())
    }

    #[test]
    fn test_16_update_node_id_due_to_full_lap_of_node_connection() -> Result<(), TokenRingError> {
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(1, 26000);
        let new_node_addr = SocketAddr::from_str("127.0.0.1:26002").unwrap();
        let addr1 = SocketAddr::from_str("127.0.0.1:26001").unwrap();
        tr_sender.message_window.node_id = u16::MAX;

        {let mut topology = tr_sender.ring_topology.lock().unwrap();
        topology.pop_back();
        topology.push_back(2);}
        {let mut address = tr_sender.ring_addresses.lock().unwrap();
        address.remove(&0);
        address.insert(2, new_node_addr);}

        let socket = UdpSocket::bind("127.0.0.1:26001").unwrap();

        let nc_msg = NodeConnection::new(MsgId::new(1,0), vec![u16::MAX,1], 2, new_node_addr,VecDeque::from([1,2]), vec![addr1, new_node_addr] );
        let mut msg = RingMessage::from_node_connection(nc_msg);
        msg.header.sender_addr = SocketAddr::from_str("127.0.0.1:26001").ok();
        tr_sender.process_msg(msg.clone())?;
        msg.header.sender_addr = None;
        msg.sign(2);

        assert_eq!(tr_sender.message_window.node_id, 2);
        let (received_bytes_msg,_) = receive_udp_package_from(&socket).unwrap();
        let received_msg = RingMessage::from_bytes(&received_bytes_msg)?;
        assert_eq!(msg, received_msg);
        Ok(())
    }

    #[test]
    fn test_17_do_not_send_timeout_message_if_not_my_id() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(1, 27000);
        let addr = SocketAddr::from_str("127.0.0.1:27002").unwrap();
        let socket = UdpSocket::bind("127.0.0.1:27001").unwrap();
        socket.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let nc_msg = NodeConnection::start(addr);
        let msg = RingMessage::from_node_connection(nc_msg);
        tr_sender.created_messages.insert(MsgId::new(255, 0), msg)?;
        tr_sender.final_times_created_messages.push_back(FinalTime::from_timeout(0, MsgId::new(255, 0)));

        tr_sender.handle_timeout()?;

        assert!(tr_sender.created_messages.is_empty()?);
        assert!(tr_sender.final_times_created_messages.is_empty());
        assert!(receive_udp_package_from(&socket).is_err());
        Ok(())
    }

    #[test]
    fn test_18_setting_a_false_connection_status_resets_the_node() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(1, 28000);
        let socket = UdpSocket::bind("127.0.0.1:28001").unwrap();
        socket.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let msg = SetConnectionStatus::new(false, None);
        tr_sender.process_msg(RingMessage::from_set_connection_status(msg))?;

        let aux_msg = RingMessage::from_node_connection(NodeConnection::start(SocketAddr::from_str("127.0.0.1:28002").unwrap()));
        tr_sender.process_msg(aux_msg)?;
        let node_addr = SocketAddr::from_str("127.0.0.1:28000").unwrap();

        assert!(!tr_sender.connected);
        assert_eq!(tr_sender.message_window.node_id,u16::MAX);
        assert!(tr_sender.message_window.messages.is_empty());
        assert_eq!(tr_sender.message_window.next_peer, node_addr);
        assert!(tr_sender.created_messages.is_empty()?);
        assert!(tr_sender.final_times_created_messages.is_empty());
        assert_eq!(tr_sender.ring_topology.lock().unwrap()[0], u16::MAX);
        assert_eq!(tr_sender.ring_topology.lock().unwrap().len(), 1);
        assert_eq!(tr_sender.ring_addresses.lock().unwrap()[&u16::MAX], node_addr);
        assert_eq!(tr_sender.ring_addresses.lock().unwrap().len(), 1);
        
        let (received_bytes_msg,_) = receive_udp_package_from(&socket).unwrap();
        let received_msg = RingMessage::from_bytes(&received_bytes_msg)?;
        assert_eq!(received_msg.header.msg_type, RingMessageType::NodeDisconnected);
        assert!(receive_udp_package_from(&socket).is_err());
        
        Ok( ())
    }

    #[test]
    fn test_19_a_node_reconnects_to_itself_in_a_cero_peer_ring() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(0, 29000);
        tr_sender.connected = false;
        tr_sender.reset_node()?;

        let msg = SetConnectionStatus::new(true,None);
        tr_sender.process_msg(RingMessage::from_set_connection_status(msg))?;

        assert!(tr_sender.connected);
        assert_eq!(tr_sender.message_window.next_peer, SocketAddr::from_str("127.0.0.1:29000").unwrap());

        Ok(())
    }

    /*#[ test]
    fn test_20_connect_to_existant_token_ring() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(1, 30000);
        tr_sender.connected = false;
        tr_sender.reset_node()?;
        let socket = UdpSocket::bind("127.0.0.1:30001").unwrap();
        socket.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let msg = SetConnectionStatus::new(true, Some(SocketAddr::from_str("127.0.0.1:30001").unwrap()));
        tr_sender.process_msg(RingMessage::from_set_connection_status(msg))?;
        
        let addr1 = SocketAddr::from_str("127.0.0.1:30001").unwrap();
        let node_addr = SocketAddr::from_str("127.0.0.1:30000").unwrap();
        let aux_msg = RingMessage::from_node_connection( NodeConnection::new(MsgId::new(1,2), vec![u16::MAX,1], 2, node_addr, VecDeque::from([1,2]), vec![addr1, node_addr] ));
        tr_sender.process_msg(aux_msg)?;


        assert!(tr_sender.connected);
        assert_eq!(tr_sender.message_window.next_peer, SocketAddr::from_str("127.0.0.1:30001").unwrap());
        
        let (received_bytes_msg,_) = receive_udp_package_from(&socket).unwrap();
        let received_msg = RingMessage::from_bytes(&received_bytes_msg)?;
        assert_eq!(received_msg.header.msg_type, RingMessageType::NodeConnection);
        assert!(!receive_udp_package_from(&socket).is_err());
        
        Ok( ())
    } */
    
    
}