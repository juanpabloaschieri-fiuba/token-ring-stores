use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::process::id;
use std::sync::mpsc::{Sender, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap, BinaryHeap};
use std::time::{Instant, Duration};
use chrono::Utc;

use crate::messages;
use crate::token_ring_messages::{RingMessageType, RingMessage, RingMessageHeader, RingMessages, MsgId};

const WINDOW_SIZE: u8 = 15;
const PEER_TIMEOUT_MS: i64 = 100;
const MAX_PEER_TIMEOUT : u8 = 4;

type SharedRingAddresses = Arc<Mutex<HashMap<u16, SocketAddr>>>;
type SharedRingTopology = Arc<Mutex<VecDeque<u16>>>;

struct TokenRingNode{
    
}

struct TokenRingReceiver{
    ring_addresses: SharedRingAddresses,
    ring_nodes: SharedRingTopology,
}

struct TokenRingSender{
    node_id: u16,
    message_window: Window,
    messages_to_send: VecDeque<RingMessage>,
    message_receiver: Receiver<RingMessage>,
    created_messages: HashMap<MsgId, RingMessage>,
    final_times_created_messages: VecDeque<FinalTime>,
    ring_topology: SharedRingTopology,
    ring_addresses: SharedRingAddresses 
}

#[derive(Debug, PartialEq, Eq)]
enum TokenRingError{
    ErrorTimeOut,
    ErrorDisconnected,
    ErrorWindowFull,
    ErrorSendingMsg,
    ErrorSharingData,
    ErrorTimedOutTooManyTimes
}

impl TokenRingSender {
    fn new(node_id: u16, rx: Receiver<RingMessage>, ring_topology: SharedRingTopology, ring_addresses: SharedRingAddresses,socket: UdpSocket, initial_next_peer_addr: SocketAddr) -> TokenRingSender {
        TokenRingSender {
            node_id,
            message_window: Window::new(node_id, WINDOW_SIZE,socket, initial_next_peer_addr),
            messages_to_send: VecDeque::new(),
            message_receiver: rx,
            created_messages: HashMap::new(),
            final_times_created_messages: VecDeque::new(),
            ring_topology,
            ring_addresses
        }
    }

    fn smallest_timeout(&self) -> Option<Duration> {
        let win_timeout = self.message_window.smallest_timeout();
        let creted_messages_timeout = smallest_timeout(&self.final_times_created_messages);
        if win_timeout.is_none() && creted_messages_timeout.is_none(){
            return None;
        }
        let win_timeout = win_timeout.unwrap_or(Duration::MAX);
        let created_messages_timeout = creted_messages_timeout.unwrap_or(Duration::MAX);
        Some(win_timeout.min(created_messages_timeout))
    }

    fn recv_until_smallest_timeout(&self) -> Result<RingMessage, TokenRingError>{
        //p obtener el minimo entre el timeout de la window y el de los mensajes a cargo 
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

    fn acknowledge_msg(&mut self, msg: RingMessage)-> Option<RingMessage>{
        self.message_window.acknowledge_msg(msg.header);
        self.messages_to_send.pop_front()
    }

    fn add_created_message(&mut self, msg: RingMessage) -> Result<(), TokenRingError>{
        if !msg.header.recently_created_by(self.node_id){
            return Ok(());
        }
        let nodes_in_ring= match self.ring_topology.lock(){
            Ok(topology) => topology.len() as i64,
            Err(_) => return Err(TokenRingError::ErrorSharingData),
        };
        self.final_times_created_messages.push_back(FinalTime::from_timeout(PEER_TIMEOUT_MS * nodes_in_ring * 3, msg.header.id));
        self.created_messages.insert(msg.header.id, msg);
        Ok(())
    }

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
    
    fn send(&mut self, msg: RingMessage) -> Result<(),TokenRingError>{
        self.send_with_priority(msg, false)
    }

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
        
        if let Some(msg) = self.created_messages.remove(&final_time.id){
            self.send_with_priority(msg, true)?;
        }

        Ok(())
    }

    fn handle_timeout(&mut self) -> Result<(),TokenRingError>{
    
        //p hay que fijarse cual de los dos timeouteo, y en funcion de eso, nadelear como corresponda.  
        if let Err(error) = self.message_window.handle_timeout(){
            match error{
                TokenRingError::ErrorTimedOutTooManyTimes => return self.handle_next_peer_disconection(),
                _=> return Err(error),
            }
        }
        
        self.handle_created_messages_timeout()
    }

    fn delete_peer_from_topology(&mut self) -> Result<SocketAddr, TokenRingError>{
        println!("entre al delete");
        let mut ring_topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut ring_addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        
        let removed_id = ring_topology.pop_front().ok_or(TokenRingError::ErrorDisconnected)?;
        ring_addresses.remove(&removed_id).ok_or(TokenRingError::ErrorDisconnected)?;

        let next_id = ring_topology.front().ok_or(TokenRingError::ErrorDisconnected)?;
        let addr = ring_addresses.get(next_id).ok_or(TokenRingError::ErrorDisconnected)?.clone();
        println!("al salir del delete{:?}", addr);
        Ok(addr)
    }

    fn handle_next_peer_disconection(&mut self) -> Result<(),TokenRingError>{
        let next_addr = self.delete_peer_from_topology()?;
        
        //crear mensaje de desconexion de peer WIP
        //self.send_with_priority(msg, true)
        self.message_window.change_peer(next_addr);
        Ok(())
    }

    fn remove_created_message(&mut self, msg_id: MsgId){
        self.created_messages.remove(&msg_id);
        let position = self.final_times_created_messages.iter().position(|time| time.id == msg_id);
        if let Some(position) = position{
            self.final_times_created_messages.remove(position);
        }
    }

    fn process_msg(&mut self, msg: RingMessage)-> Result<(),TokenRingError>{
        let msg = match msg.header.msg_type{
            RingMessageType::Ack => self.acknowledge_msg(msg),
            _ => Some(msg),
        };
        if let Some(msg) = msg{
            if msg.did_full_lap(self.node_id) {
                self.remove_created_message(msg.header.id);
            }else{
                self.send(msg)?;
            }
        };
        Ok(())
    }

    fn rcv_and_procces(&mut self)->Result<(), TokenRingError>{
        let msg = self.recv_until_smallest_timeout();
            //fijarse si termino (me mato)
        match msg{
            Ok(msg) => self.process_msg(msg)?,
            Err(error) => match error{
                TokenRingError::ErrorTimeOut => self.handle_timeout()?,
                _ => return Err(error),
            },
        };
        Ok(())
    }
}



fn run_sender(node_id: u16, rx: Receiver<RingMessage>, ring_topology: SharedRingTopology, ring_addr: SharedRingAddresses, socket: UdpSocket, initial_next_peer_addr: SocketAddr)->Result<(),TokenRingError>{
    let mut sender = TokenRingSender::new(node_id, rx, ring_topology, ring_addr, socket, initial_next_peer_addr);
    
    loop{
        sender.rcv_and_procces()?
    }
}

#[derive(Eq, PartialEq)]
struct FinalTime{
    time: i64,
    amount_of_timeouts: u8,
    id: MsgId
}

impl FinalTime{
    fn new(time:i64, id: MsgId)->FinalTime{
        FinalTime {time, amount_of_timeouts: 0,id}
    }

    fn from_timeout(timeout:i64, id:MsgId)->FinalTime{
        FinalTime::new(Utc::now().timestamp_millis() + timeout, id)
    }

    fn remaining_time(&self)->i64{
        (self.time - Utc::now().timestamp_millis()).max(0)
    }
}

impl Ord for FinalTime{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.time.cmp(&other.time){
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
        }
    }
}

impl PartialOrd for FinalTime{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if let Some(ordering) = self.time.partial_cmp(&other.time){
            let ordering = match ordering{
                std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
            };
            return Some(ordering)
        }
        None
    }
}


struct Window{
    node_id: u16,
    window_size: u8,
    messages: HashMap<MsgId,RingMessage>,
    final_times: VecDeque<FinalTime>,
    next_id: u8,
    socket: UdpSocket,
    next_peer: SocketAddr
}

impl Window{
    fn new(node_id: u16, window_size:u8, socket: UdpSocket, next_peer: SocketAddr)->Window{
        Window {node_id, window_size, messages: HashMap::new(), final_times: VecDeque::new(),next_id: 0, socket, next_peer}
    }

    fn smallest_timeout(&self)->Option<Duration>{
        smallest_timeout(&self.final_times)
    }

    fn increase_available_id(&mut self){
        if self.next_id == u8::MAX-1{
            self.next_id = 0;
        }else{
            self.next_id += 1;
        }
    }

    fn next_available_id(&mut self)->u8{
        if self.messages.contains_key(&MsgId::new(self.node_id, self.next_id)){
            self.increase_available_id();
            return self.next_available_id();
        }
        let available_id = self.next_id;
        self.increase_available_id();
        available_id
    }

    fn send_to_next_peer(&mut self, msg: &mut RingMessage)->Result<(), TokenRingError>{
        if self.messages.len() >= self.window_size as usize{
            return Err(TokenRingError::ErrorWindowFull)
        }
        if msg.header.is_from_node(self.node_id) && !self.messages.contains_key(&msg.header.id){
            msg.header.id.seq_num = self.next_available_id();
        }
        msg.sign(self.node_id);
        self.socket.send_to(&msg.to_bytes(), self.next_peer).map_err(|_| TokenRingError::ErrorSendingMsg)?;
        if !self.messages.contains_key(&msg.header.id){
            self.final_times.push_back(FinalTime::from_timeout(PEER_TIMEOUT_MS, msg.header.id));
        }
        self.messages.insert(msg.header.id, msg.clone());
        
        Ok(())
    }

    fn acknowledge_msg(&mut self, msg: RingMessageHeader){
        if self.messages.remove(&msg.id).is_some(){
            self.remove_final_time(&msg.id)
        }
    }

    fn remove_final_time(&mut self, id: &MsgId){
        let position = self.final_times.iter().position(|time| time.id == *id);
        if let Some(position) = position{
            self.final_times.remove(position);
        }
    }

    fn handle_timeout(&mut self)->Result<(), TokenRingError>{
        let mut final_time = match  self.final_times.pop_front(){
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
        
        if let Some(msg) = self.messages.get(&final_time.id){
            self.socket.send_to(&msg.to_bytes(), self.next_peer).map_err(|_| TokenRingError::ErrorSendingMsg)?;
            final_time.time = Utc::now().timestamp_millis() + PEER_TIMEOUT_MS;
            self.final_times.push_back(final_time);
        }
        
        Ok(())
    }
    
    fn change_peer(&mut self, next_peer: SocketAddr) -> Result<(), TokenRingError>{
        self.next_peer = next_peer;
        self.resend_all()
    }

    fn resend_all(&mut self) -> Result<(), TokenRingError>{
        let mut messages = Vec::new();
        
        while let Some(final_time) = self.final_times.pop_front(){
            if let Some(msg) = self.messages.remove(&final_time.id){
                messages.push(msg);
            }
        }

        for mut msg in messages{
            self.send_to_next_peer(&mut msg)?
        }
        Ok(())
    }
}

fn smallest_timeout(final_times: &VecDeque<FinalTime>)->Option<Duration>{
    if final_times.is_empty(){
        return None
    }
    
    let time_out = final_times[0].time - Utc::now().timestamp_millis();
    let time_out = time_out.max(0) as u64;
    Some(Duration::from_millis(time_out))
}

#[cfg(test)]
mod test {
    use std::{sync::mpsc, str::FromStr};

    use super::*;

    fn create_token_ring_sender(amount_of_peers: u16, port: u16)->(TokenRingSender, Sender<RingMessage>){
        let (tx, rx) = mpsc::channel();
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
        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port)).unwrap();
        let mut initial_next_peer_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port);
        if amount_of_peers != 0{
            initial_next_peer_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port+1);
        }
        let token_ring_sender = TokenRingSender::new(0, rx, ring_topology, ring_addresses, socket, initial_next_peer_addr);
        (token_ring_sender, tx)
    }

    fn create_n_messages_from(amount_of_msg: u8, node_id: u16) -> Vec<RingMessage>{
        let mut messages: Vec<RingMessage> = vec![];
        
        for i in 0..amount_of_msg{
            let msg_header = RingMessageHeader::new(MsgId::new(node_id,u8::MAX), RingMessageType::NewNode, 0);
            let mut msg = RingMessage::new(msg_header, vec![0,1,2,3,4,5]);
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
        let (mut tr_sender, mut msg_sender) = create_token_ring_sender(3, 11000);
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

        let ack = RingMessage::new_ack(MsgId::new(0, 0));
        tr_sender.process_msg(ack)?;

        assert_eq!(tr_sender.message_window.messages.len(), WINDOW_SIZE as usize);
        assert_eq!(tr_sender.message_window.final_times.len(), WINDOW_SIZE as usize);
        
        for i in 1..WINDOW_SIZE+1{
            let ack = RingMessage::new_ack(MsgId::new(0, i));
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
        let msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:16001").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        
        tr_sender.send(msg.clone())?;
        tr_sender.send(msg)?;
        tr_sender.rcv_and_procces()?;
        tr_sender.rcv_and_procces()?;

        assert_eq!(tr_sender.message_window.final_times[0].amount_of_timeouts, 1);
        assert_eq!(tr_sender.message_window.final_times[1].amount_of_timeouts, 1);
        
        receive_udp_package_from(&socket).unwrap();
        receive_udp_package_from(&socket).unwrap();
        receive_udp_package_from(&socket).unwrap();
        receive_udp_package_from(&socket).unwrap();
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
            tr_sender.rcv_and_procces()?;
        }
        
        for _i in 0..2*MAX_PEER_TIMEOUT{
            receive_udp_package_from(&first_socket).unwrap();
        }
        assert!(receive_udp_package_from(&first_socket).is_err());
        
        for _i in 0..2{
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

        assert_eq!(tr_sender.created_messages.len(), WINDOW_SIZE as usize);
        assert_eq!(tr_sender.final_times_created_messages.len(), WINDOW_SIZE as usize);

        Ok(())
    }

    #[test]

    fn test_9_resending_messages_doesnt_add_to_created_messages() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,19000);
        let msg = create_n_messages_from(1,2).remove(0);
        
        tr_sender.send(msg)?;

        assert!(tr_sender.created_messages.is_empty());
        assert!(tr_sender.final_times_created_messages.is_empty());
        assert!(!tr_sender.message_window.messages.is_empty());
        assert!(!tr_sender.message_window.final_times.is_empty());
        Ok(())
    }

    #[test]
    fn test_10_receive_own_messages_removes_them_from_the_queue() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,20000);
        let mut msg = create_n_messages_from(5,0);

        for i in 0..msg.len(){
            tr_sender.send(msg[i].clone())?;
        }

        for i in 0..msg.len(){
            msg[i].sign(0);
            msg[i].header.id.seq_num = i as u8;
            tr_sender.process_msg(msg[i].clone())?;
        }
        
        assert_eq!(tr_sender.created_messages.len(), 0);
        assert_eq!(tr_sender.final_times_created_messages.len(),0);
        
        Ok(())
    }
    
    #[test]
    fn test_11_timeout_of_created_and_sended_msg() -> Result<(), TokenRingError>{
        let (mut tr_sender,  _msg_sender) = create_token_ring_sender(3,21000);
        let msg = create_n_messages_from(1,0).remove(0);
        let socket = UdpSocket::bind("127.0.0.1:21001").unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        tr_sender.send(msg)?;
        tr_sender.acknowledge_msg(RingMessage::new_ack(MsgId::new(0, 0)));

        tr_sender.rcv_and_procces()?;
        receive_udp_package_from(&socket).unwrap();
        receive_udp_package_from(&socket).unwrap();
        assert!(receive_udp_package_from(&socket).is_err());
        Ok(())
    }
    #[test]
    fn test_12_receive_msg_from_myself_in_disconected_topology() -> Result<(), TokenRingError>{
        let (mut tr_sender,  msg_sender) = create_token_ring_sender(0,22000);
        let mut msg = create_n_messages_from(1,0).remove(0);
        tr_sender.message_window.socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        
        tr_sender.send(msg.clone())?;

        msg.sign(0);
        msg.header.id.seq_num = 0;
        msg_sender.send(RingMessage::new_ack(MsgId::new(0, 0))).unwrap();
        msg_sender.send(msg).unwrap();
        
        tr_sender.rcv_and_procces()?;
        tr_sender.rcv_and_procces()?;
        receive_udp_package_from(&tr_sender.message_window.socket).unwrap();
        assert!(receive_udp_package_from(&tr_sender.message_window.socket).is_err());
        Ok(())
    }
    
    #[test]
    fn test_13_create_disconected_topology_by_pointing_to_yourself()-> Result<(), TokenRingError>{
        let (mut tr_sender,  msg_sender) = create_token_ring_sender(1,23000);
        let mut msg = create_n_messages_from(1,0).remove(0);
        tr_sender.message_window.socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        tr_sender.send(msg)?;
        
        for i in 0..MAX_PEER_TIMEOUT{
            println!("{i}");
            tr_sender.rcv_and_procces()?;
        }
        
        println!("{:?}",tr_sender.message_window.next_peer);
        assert_eq!(tr_sender.message_window.next_peer,SocketAddr::from_str("127.0.0.1:23000").unwrap());
        receive_udp_package_from(&tr_sender.message_window.socket).unwrap();
        assert!(receive_udp_package_from(&tr_sender.message_window.socket).is_err());
        Ok(())
    }

    //test del mensaje reenviado por timeout desp de disconnect (ver cant de firmas)
}