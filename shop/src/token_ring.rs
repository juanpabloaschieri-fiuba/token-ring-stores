use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::os::unix::net::SocketAddr;
use std::sync::mpsc::{Sender, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap, BinaryHeap};
use std::time::{Instant, Duration};
use chrono::Utc;

use crate::token_ring_messages::RingMessageType;

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
    message_window: Window,
    messages_to_send: VecDeque<Vec<u8>>,
    message_receiver: Receiver<(Vec<u8>,RingMessageType)>,
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
    fn new(rx: Receiver<(Vec<u8>, RingMessageType)>, ring_topology: SharedRingTopology, ring_addresses: SharedRingAddresses,socket: UdpSocket, initial_next_peer_addr: SocketAddr) -> TokenRingSender {
        TokenRingSender {
            message_window: Window::new(WINDOW_SIZE,socket, initial_next_peer_addr),
            messages_to_send: VecDeque::new(),
            message_receiver: rx,
            ring_topology,
            ring_addresses}
    }

    fn recv_until_smallest_timeout(&self)->Result<(Vec<u8>, RingMessageType), TokenRingError>{
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

    fn acknowledge_msg(&mut self, msg: Vec<u8>)-> Option<Vec<u8>>{
        self.message_window.acknowledge_msg(msg);
        self.messages_to_send.pop_front()
    }

    fn send(&mut self, msg: Vec<u8>)->Result<(),TokenRingError>{
        if let Err(error) = self.message_window.send_to_next_peer(msg){
            match error{
                TokenRingError::ErrorWindowFull => self.messages_to_send.push_back(msg), 
                _ => return Err(error),
            }
        }
        Ok(())
    }

    fn handle_timeout(&mut self) -> Result<(),TokenRingError>{
        if let Err(error) = self.message_window.handle_timeout(){
            match error{
                TokenRingError::ErrorTimedOutTooManyTimes => self.handle_next_peer_disconection(),
                _=> return Err(error),
            }
        }
        Ok(())
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
        self.delete_peer_from_topology()?;
        //buscar quien es el proximo peer
        //sacar el peer de la tabla
        //crear mensaje de desconexion de peer
        //mandarle a la ventana que tiene que cambiar de peer (cambio de IP), 
        // y reenviar todo priorizando el mensaje de desconexion (para entrar a la ventana)
    }
}



fn run_sender(rx: Receiver<(Vec<u8>,RingMessageType)>, ring_topology: SharedRingTopology, ring_addr: SharedRingAddresses, socket: UdpSocket, initial_next_peer_addr: SocketAddr)->Result<(),TokenRingError>{
    let mut sender = TokenRingSender::new(rx, ring_topology, ring_addr, socket, initial_next_peer_addr);
    //sender.message_receiver.recv_timeout(timeout);
    loop{
        let msg = sender.recv_until_smallest_timeout();
            //fijarse si termino (me mato)
            match msg{
                Ok((msg, msg_type)) => {
 
                    let msg = match msg_type{
                        RingMessageType::Ack => sender.acknowledge_msg(msg),
                        _ => Some(msg),
                    };
                    if let Some(msg) = msg{
                        sender.send(msg)?;
                    }
                }
                Err(error) => match error{
                    TokenRingError::ErrorTimeOut => sender.handle_timeout(),
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
    id: u8
}

impl FinalTime{
    fn new(time:i64, id: u8)->FinalTime{
        FinalTime {time: time, amount_of_timeouts: 0,id}
    }

    fn from_timeout(timeout:i64, id:u8)->FinalTime{
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
    window_size: u8,
    messages: HashMap<u8,Vec<u8>>,
    final_times: VecDeque<FinalTime>,
    next_id: u8,
    socket: UdpSocket,
    next_peer: SocketAddr
}

impl Window{
    fn new(window_size:u8, socket: UdpSocket, next_peer: SocketAddr)->Window{
        Window { window_size, messages: HashMap::new(), final_times: VecDeque::new(),next_id: 0, socket, next_peer}
    }

    fn smallest_timeout(&self)->Option<Duration>{
        if self.final_times.is_empty(){
            return None
        }
        
        let time_out = self.final_times[0].time - Utc::now().timestamp_millis();
        let time_out = time_out.max(0) as u64;
        Some(Duration::from_millis(time_out))
    }

    fn send_to_next_peer(&mut self, mut msg: Vec<u8>)->Result<(), TokenRingError>{
        if self.messages.len() >= self.window_size as usize{
            return Err(TokenRingError::ErrorWindowFull)
        }
        msg[0] = self.next_id;
        self.socket.send_to(&msg, self.next_peer).map_err(|_| TokenRingError::ErrorSendingMsg)?;
        self.messages.insert(self.next_id, msg);
        self.final_times.push_back(FinalTime::from_timeout(PEER_TIMEOUT_MS, self.next_id));
        if self.next_id == u8::MAX{
            self.next_id = 0;
        }else{
            self.next_id += 1;
        }
        Ok(())
    }

    fn acknowledge_msg(&mut self, msg: Vec<u8>){
        if self.messages.remove(&msg[0]).is_some(){
            self.remove_final_time(&msg[0])
        }
    }

    fn remove_final_time(&mut self, msg_id: &u8){
        let position = self.final_times.iter().position(|&time| time.id == *msg_id);
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
        let msg = self.messages[&self.final_times[0].id];
        self.socket.send_to(&msg, self.next_peer).map_err(|_| TokenRingError::ErrorSendingMsg)?;
        final_time.time = Utc::now().timestamp_millis() + PEER_TIMEOUT_MS;
        self.final_times.push_back(final_time);

        Ok(())
    }
}


//let (tx, rx): (std::sync::mpsc::Sender<Box<dyn MyTrait>>, std::sync::mpsc::Receiver<Box<dyn MyTrait>>) = std::sync::mpsc::channel();
/*
struct RingMessage{
    signatures: Vec<u16>,
    msg_id: u8,
    msg_type: RingMessageType,
}

pub enum RingMessageType {
    CoordinateRequest(CoordinateRequestMsg)
}
*/



