use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc::{Sender, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap, BinaryHeap};
use std::time::{Instant, Duration};
use chrono::Utc;

use crate::token_ring_messages::RingMessageType;

const WINDOW_SIZE: u8 = 15;
const PEER_TIMEOUT_MS: u64 = 100;

type SharedRingIps = Arc<Mutex<HashMap<u16, SocketAddr>>>;
type SharedRingTopology = Arc<Mutex<Vec<u16>>>;

struct TokenRingNode{
    
}

struct TokenRingReceiver{
    ring_ips: SharedRingIps,
    ring_nodes: SharedRingTopology,
}

struct TokenRingSender{
    message_window: Window,
    messages_to_send: VecDeque<Vec<u8>>,
    message_receiver: Receiver<(Vec<u8>,RingMessageType)>,
    ring_topology: SharedRingTopology,
    ring_ips: SharedRingIps
}

enum TokenRingError{
    ErrorTimeOut,
    ErrorDisconnected
}

impl TokenRingSender {
    fn new(rx: Receiver<(Vec<u8>, RingMessageType)>, ring_topology: SharedRingTopology, ring_ips: SharedRingIps) -> TokenRingSender {
        TokenRingSender {
            message_window: Window::new(WINDOW_SIZE),
            messages_to_send: VecDeque::new(),
            message_receiver: rx,
            ring_topology,
            ring_ips}
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

    fn acknowledge_msg(&mut self, msg: Vec<u8>){
        self.message_window.acknowledge_msg(msg);
        self.messages_to_send.pop_front();
    }
}



fn run_sender(rx: Receiver<(Vec<u8>,RingMessageType)>, ring_topology: SharedRingTopology, ring_ips: SharedRingIps)->Result<(),TokenRingError>{
    let sender = TokenRingSender::new(rx, ring_topology, ring_ips);
    //sender.message_receiver.recv_timeout(timeout);
    loop{
        let msg = sender.recv_until_smallest_timeout();
            //fijarse si termino (me mato)
            match msg{
                Ok((msg, msg_type)) => match msg_type{
                    RingMessageType::Ack => sender.acknowledge_msg(msg),
                    _ => sender.send(),
                }
                sender.message_window.handle_msg(msg),
                Err(error) => todo!(),
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
    final_times: Vec<FinalTime>,
}

impl Window{
    fn new(window_size:u8)->Window{
        Window { window_size, messages: HashMap::new(), final_times: Vec::new() }
    }

    fn smallest_timeout(&self)->Option<Duration>{
        if self.final_times.is_empty(){
            return None
        }
        
        let time_out = self.final_times[0].time - Utc::now().timestamp_millis();
        let time_out = time_out.max(0) as u64;
        Some(Duration::from_millis(time_out))
    }

    fn push(&mut self, msg: Vec<u8>){
        self.messages.insert(msg.[0], msg)
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



