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
//p los mensajes a cuidar compartidos entre el sender y el receiver

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
    ring_topology: SharedRingTopology,
    ring_addresses: SharedRingAddresses 
}

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
            ring_topology,
            ring_addresses}
    }

    fn recv_until_smallest_timeout(&self) -> Result<RingMessage, TokenRingError>{
        //p obtener el minimo entre el timeout de la window y el de los mensajes a cargo 
        match self.message_window.smallest_timeout(){
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

    fn send_with_priority(&mut self, msg: RingMessage, prioritice: bool)->Result<(),TokenRingError>{
        if let Err(error) = self.message_window.send_to_next_peer(msg.clone()){
            match error{
                TokenRingError::ErrorWindowFull => {
                    if prioritice{
                        self.messages_to_send.push_front(msg);
                    }else {
                        self.messages_to_send.push_back(msg);
                    }
                }
                _ => return Err(error),
            }
        }
        //p fijarse si es nuestro el mensaje, y agregarlo a mensajes a cuidar
        Ok(())
    }
    
    fn send(&mut self, msg: RingMessage)->Result<(),TokenRingError>{
        self.send_with_priority(msg, false)
    }

    fn handle_timeout(&mut self) -> Result<(),TokenRingError>{
        //p hay que fijarse cual de los dos timeouteo, y en funcion de eso, nadelear como corresponda.
        match self.message_window.handle_timeout(){
            Ok(()) => Ok(()),
            Err(error) => {
                match error{
                    TokenRingError::ErrorTimedOutTooManyTimes => self.handle_next_peer_disconection(),
                    _=> Err(error),
                }
            }
        }

    }

    fn delete_peer_from_topology(&mut self) -> Result<SocketAddr, TokenRingError>{
        let mut ring_topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut ring_addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        
        let removed_id = ring_topology.pop_front().ok_or(TokenRingError::ErrorSharingData)?;
        ring_addresses.remove(&removed_id).ok_or(TokenRingError::ErrorSharingData)?;

        let next_id = ring_topology.front().ok_or(TokenRingError::ErrorSharingData)?;
        let addr = ring_addresses.get(next_id).ok_or(TokenRingError::ErrorSharingData)?.clone();
        
        Ok(addr)

    }

    fn handle_next_peer_disconection(&mut self) -> Result<(),TokenRingError>{
        let next_addr = self.delete_peer_from_topology()?;
        //crear mensaje de desconexion de peer WIP
        //self.send_with_priority(msg, true)
        self.message_window.change_peer(next_addr);
        Ok(())
    }
}



fn run_sender(node_id: u16, rx: Receiver<RingMessage>, ring_topology: SharedRingTopology, ring_addr: SharedRingAddresses, socket: UdpSocket, initial_next_peer_addr: SocketAddr)->Result<(),TokenRingError>{
    let mut sender = TokenRingSender::new(node_id, rx, ring_topology, ring_addr, socket, initial_next_peer_addr);
    
    loop{
        let msg = sender.recv_until_smallest_timeout();
            //fijarse si termino (me mato)
            match msg{
                Ok(msg) => {
                    let msg = match msg.header.msg_type{
                        RingMessageType::Ack => sender.acknowledge_msg(msg),
                        _ => Some(msg),
                    };
                    if let Some(msg) = msg{
                        sender.send(msg)?;
                    }
                }
                Err(error) => match error{
                    TokenRingError::ErrorTimeOut => sender.handle_timeout()?,
                    _ => return Err(error),
                },
            }
            //si timeouteo decirle a la ventana

            //si recibio un mensaje a enviar agregarlo a messages_to_send o a la ventana si tiene lugar
            //si se recibio ack, sacarlo de la ventana
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


struct  Window{
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
        if self.final_times.is_empty(){
            return None
        }
        
        let time_out = self.final_times[0].time - Utc::now().timestamp_millis();
        let time_out = time_out.max(0) as u64;
        Some(Duration::from_millis(time_out))
    }

    fn send_to_next_peer(&mut self, mut msg: RingMessage)->Result<(), TokenRingError>{
        if self.messages.len() >= self.window_size as usize{
            return Err(TokenRingError::ErrorWindowFull)
        }
        if msg.header.is_from_node(self.node_id){
            msg.header.id.seq_num = self.next_id;
        }
        self.socket.send_to(&msg.to_bytes(), self.next_peer).map_err(|_| TokenRingError::ErrorSendingMsg)?;
        self.final_times.push_back(FinalTime::from_timeout(PEER_TIMEOUT_MS, msg.header.id));
        self.messages.insert(msg.header.id, msg);
        
        if self.next_id == u8::MAX{
            self.next_id = 0;
        }else{
            self.next_id += 1;
        }
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
        let mut final_time = match self.final_times.pop_front(){
            Some(final_time) => {
                if self.final_times[0].remaining_time() > 0{
                    return Ok(());
                };
                final_time},
            None => return Ok(())
        };
        
        final_time.amount_of_timeouts +=1;
        if final_time.amount_of_timeouts >= MAX_PEER_TIMEOUT{
            return Err(TokenRingError::ErrorTimedOutTooManyTimes)
        }
        
        if let Some(msg) = self.messages.get(&self.final_times[0].id){
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

        for msg in messages{
            self.send_to_next_peer(msg)?
        }
        Ok(())
    }
}