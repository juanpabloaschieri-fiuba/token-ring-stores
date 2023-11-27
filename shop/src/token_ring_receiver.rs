use std::f32::consts::E;
use std::net::{UdpSocket, SocketAddr};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap, BinaryHeap};
use actix::{Addr, Message};
use rand::seq::IteratorRandom;
use colored::*;

const MAX_BUFFER_SIZE: usize = 65535;

use crate::ecomerce::Ecommerce;
use crate::messages::Request;
use crate::token_ring::*;
use crate::token_ring_messages::*;

/// # TokenRingReceiver
/// 
/// This struct is in charge of receiving messages from other nodes in the token ring.
/// 
/// ## Attributes
/// * `node_id`: The id of the node that owns this TokenRingReceiver
/// * `socket`: The socket used to receive messages
/// * `ecommerce_addr`: The address of the Ecommerce actor
/// * `msg_sender`: The sender used to send messages to the TokenRingSender
/// * `ring_addresses`: The addresses of all the nodes in the ring
/// * `ring_topology`: The ids of all the nodes in the ring
pub struct TokenRingReceiver{
    node_id: u16,
    socket: UdpSocket,              //WIP timeout in main
    ecommerce_addr: Addr<Ecommerce>,
    msg_sender: Sender<RingMessage>,
    ring_addresses: SharedRingAddresses,
    ring_topology: SharedRingTopology,
    created_messages: SharedCreatedMessages,
    connected: bool 
}

impl TokenRingReceiver{
    
    /// Creates a new TokenRingReceiver
    
    pub fn new(node_id: u16, msg_sender: Sender<RingMessage>, ring_nodes: SharedRingTopology,ring_addresses: SharedRingAddresses, created_messages: SharedCreatedMessages, socket: UdpSocket,ecommerce_addr: Addr<Ecommerce>) -> TokenRingReceiver{
        TokenRingReceiver {node_id, socket, ecommerce_addr, msg_sender, ring_addresses, ring_topology: ring_nodes, created_messages, connected: true}
    }

    /// Receives a message from the socket and generates a RingMessage from it.
    fn recv_from(&self) -> Result<Option<(RingMessage, SocketAddr)>,TokenRingError>{
        let mut buffer = [0;MAX_BUFFER_SIZE];
        let (amount_of_bytes, addr) = match self.socket.recv_from(&mut buffer){
            Ok((amount_of_bytes, addr)) => (amount_of_bytes, addr),
            Err(error) => match error.kind(){
                std::io::ErrorKind::WouldBlock => return Ok(None),
                std::io::ErrorKind::TimedOut => return Ok(None),
                _ => return Err(TokenRingError::ErrorReceivingMsg),
            },
        };
        let received_bytes =  Vec::from(buffer.split_at(amount_of_bytes).0);
        
        Ok(Some((RingMessage::from_bytes(&received_bytes)?,addr)))
    }
    
    /// Returns true if the message was sent from inside the topology
    fn message_sent_from_inside_topolgy(&self,msg: &RingMessage, addr: SocketAddr)->Result<bool,TokenRingError>{
        let addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        match addresses.get(&msg.last_sent_by()){
            Some(id_addr) => Ok(addr == *id_addr),
            None => Ok(false),
        }
    }

    /// Handles a CoordinateRequest message
    fn handle_coordinate_request(&mut self, msg: RingMessage)-> Result<(Vec<RingMessage>, EcommerceMsg),TokenRingError>{
        let coordinate_request = CoordinateRequest::from_ring_message(msg)?;
        let mut msg_to_send = Vec::new();
        let mut ecommerce_msg = EcommerceMsg::None;
        if coordinate_request.did_full_lap(self.node_id){
            if let Some(coordinate_request_result) = coordinate_request.get_result(self.node_id){
                if coordinate_request_result.winner_id != self.node_id{
                    print(format!("Request did full lap, i am not the winner"));
                    msg_to_send.push(RingMessage::from_coordinated_request_result(coordinate_request_result));
                } else {
                    ecommerce_msg = EcommerceMsg::Request(coordinate_request_result.request);
                    print(format!("Request did full lap, i am the winner"));
                }
            }
        } else {
            print(format!("Continuando la lap"));
            ecommerce_msg = EcommerceMsg::Coordinaterequest(coordinate_request);
        }
        
        Ok((msg_to_send, ecommerce_msg))
    }
    
    /// Handles a CoordinatedRequestResult message
    fn handle_coordinated_request_result(&mut self, msg:RingMessage)-> Result<(Vec<RingMessage>, EcommerceMsg),TokenRingError>{
        let coordinate_request_result = CoordinatedRequestResult::from_ring_message(msg.clone())?;
        let mut msg_to_send = Vec::new();
        let mut ecommerce_msg = EcommerceMsg::None;
        if coordinate_request_result.did_full_lap(self.node_id){
            if !coordinate_request_result.signatures.contains(&coordinate_request_result.winner_id){
                let recoordinate = CoordinateRequest::start(self.node_id, coordinate_request_result.request.clone(), coordinate_request_result.amount_of_diferent_products);
                ecommerce_msg = EcommerceMsg::Coordinaterequest(recoordinate);
            }
        } else {
            if coordinate_request_result.winner_id == self.node_id{
                ecommerce_msg = EcommerceMsg::Request(coordinate_request_result.request);
            }
            msg_to_send.push(msg);
        }
        Ok((msg_to_send, ecommerce_msg))
    }

    // Handles a message related to the ecommerce: given the message, it handles the request
    // and returns the message that should be sent to the Ecommerce actor
    fn handle_ecommerce_related_message(&mut self, msg:RingMessage)-> Result<Vec<RingMessage>,TokenRingError>{
        let (msg_to_send ,ecommerce_message) = match msg.header.msg_type{
            RingMessageType::CoordinateRequest => self.handle_coordinate_request(msg)?,
            RingMessageType::CoordinatedRequestResult => self.handle_coordinated_request_result(msg)?,
            _ => return Ok(Vec::new()),
        };
        print(format!("Enviandole al ecomerce {:?}", ecommerce_message));
        match ecommerce_message{
            EcommerceMsg::Request(msg) => self.ecommerce_addr.try_send(msg).map_err(|_| TokenRingError::ErrorSharingData)?,
            EcommerceMsg::Coordinaterequest(msg) => self.ecommerce_addr.try_send(msg).map_err(|_| TokenRingError::ErrorSharingData)?,
            EcommerceMsg::None =>{},
        };
        Ok(msg_to_send)
    }

    /// Handles a NodeDisconnected message: removes the node from the topology
    fn handle_node_disconection_message(&mut self, msg: RingMessage) -> Result<Vec<RingMessage>, TokenRingError>{
        let disconection_msg = NodeDisconnected::from_ring_message(msg.clone())?;
        let mut msg_to_send = Vec::new();
        if !disconection_msg.did_full_lap(self.node_id){
            self.remove_peer_from_topology(disconection_msg.disconected_node)?;
            msg_to_send.push(msg);
        };
        Ok(msg_to_send)
    }

    /// Initializes the node by receiving a NodeConnection message and sending it to the rest of the topology
    fn initialize_node(&mut self, msg: NodeConnection)-> Result<RingMessage, TokenRingError>{
        let msg_clone = msg.clone();
        let mut ring_topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut ring_addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        *ring_topology = msg.topology_ids;
        let mut addresses = HashMap::new();
        for (i, id) in ring_topology.iter().enumerate(){
            addresses.insert(*id, msg.topology_addresses[i]);
        }
        *ring_addresses = addresses;
        self.node_id = *ring_topology.back().ok_or(TokenRingError::ErrorDisconnected)?;

        let confirm_msg = ConfirmConnection::new(MsgId::from_node(self.node_id), Vec::new(), self.node_id, ring_addresses[&ring_topology[ring_topology.len()-1]]);
        
        print(format!("Node with id: {} is ready", self.node_id));
        Ok(RingMessage::from_confirm_connection(confirm_msg))
    }

    /// Handles a NodeConnection message
    fn handle_node_connection(&mut self, msg:RingMessage) -> Result<Vec<RingMessage>, TokenRingError>{
        let mut connection_msg = NodeConnection::from_ring_message(msg.clone())?;
        let mut messages_to_send = Vec::new();
        if !msg.did_full_lap(self.node_id) || (self.node_id == u16::MAX){
            messages_to_send.push(msg);
        };
        let local_address = self.socket.local_addr().map_err(|_| TokenRingError::ErrorSharingData)?;
        if  (local_address == connection_msg.new_node_addr) && (self.node_id == u16::MAX){
            messages_to_send.push(self.initialize_node(connection_msg)?);
            return Ok(messages_to_send);
        }


        let mut ring_topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut ring_addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        if ring_addresses.values().find(|addr| **addr == connection_msg.new_node_addr).is_some(){
            return Ok(messages_to_send);
        }

        if self.node_id == *ring_topology.iter().max().ok_or(TokenRingError::ErrorDisconnected)?{
            connection_msg.topology_ids = ring_topology.clone();
            for id in &connection_msg.topology_ids{
                connection_msg.topology_addresses.push(ring_addresses[id])
            }
            connection_msg.topology_ids.push_back(self.node_id+1);
            connection_msg.topology_addresses.push(connection_msg.new_node_addr.clone());
            connection_msg.amount_of_peers = connection_msg.topology_ids.len() as u16;
            ring_topology.push_front(self.node_id + 1);
            ring_addresses.insert(self.node_id + 1, connection_msg.new_node_addr.clone());
        }
        
        Ok(vec![RingMessage::from_node_connection(connection_msg)])
    }

    /// Handles a ConfirmConnection message
    fn handle_confirm_connection(&self, msg:RingMessage)-> Result<Vec<RingMessage>, TokenRingError>{
        let cc_msg = ConfirmConnection::from_ring_message(msg)?;
        
        let mut ring_topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut ring_addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        if !ring_addresses.contains_key(&cc_msg.node_id){
            let min = ring_topology.iter().enumerate().min_by(|a,b| a.1.cmp(b.1)).unwrap();
            let mut aux = -1;
            for i in min.0..ring_topology.len()-1{
                if ring_topology[i] > cc_msg.node_id && aux == -1{
                    aux=i as i32;
                    break;
                }
            }
            if aux == -1{
                for i in 0..min.0{
                    if ring_topology[i] > cc_msg.node_id{
                        aux=i as i32;
                        break;
                    }
                }
            }
            if aux == -1{
                aux = min.0 as i32;
            }
            ring_topology.insert(aux as usize, cc_msg.node_id);
            ring_addresses.insert(cc_msg.node_id, cc_msg.addr);
        }
        
        let msg = RingMessage::from_confirm_connection(cc_msg);
        let mut msg_to_send = Vec::new();
        if !msg.did_full_lap(self.node_id){
            msg_to_send.push(msg)
        }
        Ok(msg_to_send)
    }
    
    /// Receives a RingMesage and handles it accordingly.
    fn handle_message(&mut self, mut msg: RingMessage, addr: SocketAddr) -> Result<(),TokenRingError>{
        if msg.header.msg_type == RingMessageType::SetConnectionStatus{
            return self.handle_set_connection_status(msg);
        }
        if !self.connected{
            return Ok(())
        }
        print(format!("Before handeling msg of type: {:?} {:?}, received from {:?} con firmas {:?}", msg.header.msg_type, msg.header.id, addr, msg.signatures));
        print(format!("-----------------------------------------------------------------------------"));
        msg.header.sender_addr = Some(addr);
        if msg.header.msg_type != RingMessageType::SendAck {
            let mut received_ack = RingMessage::new_ack(msg.header.id, RingMessageType::SendAck);
            received_ack.header.sender_addr = Some(addr);
            self.msg_sender.send(received_ack).map_err(|_| TokenRingError::ErrorSharingData)?;
            
            if msg.header.is_from_node(self.node_id){
                if self.created_messages.remove(&msg.header.id)?.is_none(){
                    return Ok(())
                }
            }
        }
        
        let resend_msgs = match msg.header.msg_type {
            RingMessageType::CoordinateRequest => self.handle_ecommerce_related_message(msg)?,
            RingMessageType::CoordinatedRequestResult => self.handle_ecommerce_related_message(msg)?,
            RingMessageType::NodeConnection => self.handle_node_connection(msg)?,
            RingMessageType::NodeDisconnected => self.handle_node_disconection_message(msg)?,
            RingMessageType::ConfirmConnection => self.handle_confirm_connection(msg)?,
            RingMessageType::SendAck => {
                msg.header.msg_type = RingMessageType::ReceivedAck;
                return self.msg_sender.send(msg).map_err(|_| TokenRingError::ErrorSharingData)
            },
            _ => return Ok(()),
        };
        
        for msg in resend_msgs{
            self.msg_sender.send(msg).map_err(|_| TokenRingError::ErrorSharingData)?;
        }

        Ok(())
    }

    
    
    fn handle_set_connection_status(&mut self, msg: RingMessage)-> Result<(), TokenRingError>{
        if msg.header.msg_type != RingMessageType::SetConnectionStatus {
            return Ok(())
        }
        let msg = SetConnectionStatus::from_ring_message(msg)?;
        self.connected = msg.new_connection_status;
        if msg.new_connection_status == false{
            self.node_id = u16::MAX;
        }

        self.msg_sender.send(RingMessage::from_set_connection_status(msg)).map_err(|_| TokenRingError::ErrorSharingData)?;
        Ok(())
    }

    /// Receives a message from the socket and handles it
    pub fn recv_and_handle(&mut self)->Result<(), TokenRingError>{
        let (mut msg, addr) = match self.recv_from(){
            Ok(received) => match received{
                Some((msg, addr)) => {
                    match msg.header.msg_type {
                        RingMessageType::NodeDisconnected => {
                            if !self.message_sent_from_inside_topolgy(&msg,addr)?{
                                return Ok(())
                            }
                            (msg,addr)
                        },
                        RingMessageType::EndOfProgram => return Err(TokenRingError::EndOfProgram),
                        _ => (msg,addr),
                    }
                },
                None => return Ok(()),
            },
            Err(error) => match error{
                TokenRingError::ErrorInvalidByteSequence => return Ok(()),
                _ => return Err(error),
            },
        };

        self.handle_message(msg, addr)

    }

    /// Removes a peer from the topology
    fn remove_peer_from_topology(&mut self, node_id:u16)->Result<(), TokenRingError>{
        println!("entre al delete");
        let mut ring_topology = self.ring_topology.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
        let mut ring_addresses = self.ring_addresses.lock().map_err(|_| TokenRingError::ErrorSharingData)?;
            
        let _ = ring_addresses.remove(&node_id);
        if let Some(position) = ring_topology.iter().position(|id| node_id==*id){
            ring_topology.remove(position);
        }
        Ok(())
    }

    pub fn signal_end_of_program_to_sender(self){
        self.msg_sender.send(RingMessage::new_end_of_program());
    }
}

/// Enum used to send messages to the Ecommerce actor
#[derive(Debug)]
enum EcommerceMsg{
    Request(Request),
    Coordinaterequest(CoordinateRequest),
    None
}

fn print(string: String){
    println!("{} {}","[TokenRingReceiver]".yellow(), string);
}

#[cfg(test)]
mod test {
    use std::{sync::mpsc::{self, Receiver}, net::IpAddr, str::FromStr};
    use actix::Actor;

    use crate::{token_ring_sender::test::create_topology, shop::Shop, point::Point};
    use super::*;
    
    fn create_token_ring_receiver(amount_of_peers: u16, port: u16)->(TokenRingReceiver, Receiver<RingMessage>){
        let (tx, rx) = mpsc::channel();
        
        let (ring_topology, ring_addresses) = create_topology(amount_of_peers, port);
        let socket = UdpSocket::bind(SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), port)).unwrap();
        
        let (delivery_tx, _delivery_rx) = mpsc::channel();
        let shop_addr = Shop::new(HashMap::new(), Point::new(0, 0), delivery_tx).start();
        let ecommerce_addr = Ecommerce::new(shop_addr, tx.clone()).start();
        let created_messages = SharedCreatedMessages::new();
        let token_ring_receiver = TokenRingReceiver::new(0, tx, ring_topology, ring_addresses,created_messages,socket, ecommerce_addr);
        
        
        (token_ring_receiver, rx)
    }

    fn assert_recv_n_messages_of_type(msg_receiver: Receiver<RingMessage>, n:usize, mut msg_types: Vec<RingMessageType>){
        for _i in 0..n{
            let msg = msg_receiver.try_recv().expect("Not enough messages");
            let position = msg_types.iter().position(|msg_type| *msg_type == msg.header.msg_type).expect("Wrong message type");
            msg_types.remove(position);
        }
        assert!(msg_receiver.try_recv().is_err());
    }

    fn assert_n_messages_of_type(received_messages: Vec<RingMessage>, n: usize, mut msg_types: Vec<RingMessageType>){
        assert_eq!(received_messages.len(), n);
        for msg in received_messages{
            let position = msg_types.iter().position(|msg_type| *msg_type == msg.header.msg_type).expect("Wrong message type");
            msg_types.remove(position);
        }
    }

    fn assert_ecommerce_msg_is_coordinate_request(msg_for_ecommerce: EcommerceMsg){
        match msg_for_ecommerce{
            EcommerceMsg::Coordinaterequest(_) => {},
            _ => panic!("Incorrect response"),
        }
    }

    fn assert_ecommerce_msg_is_request(msg_for_ecommerce: EcommerceMsg){
        match msg_for_ecommerce{
            EcommerceMsg::Request(_) => {},
            _ => panic!("Incorrect response"),
        }
    }

    fn assert_no_ecommerce_msg(msg_for_ecommerce: EcommerceMsg){
        match msg_for_ecommerce{
            EcommerceMsg::None => {},
            _ => panic!("Incorrect response"),
        }
    }

    #[actix_rt::test]
    async fn test_1_when_receiving_ack_it_is_fowarded_to_the_sender()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10100);
        let sender_addr = SocketAddr::from_str("127.0.0.1:10103").unwrap();
        tr_receiver.handle_message(RingMessage::new_ack(MsgId::from_node(0), RingMessageType::SendAck), sender_addr)?;
        assert_recv_n_messages_of_type(msg_receiver, 1, vec![RingMessageType::ReceivedAck]);
        Ok(())
    }

    #[actix_rt::test]
    async fn test_2_when_receiving_unfinished_coordinate_request_it_is_given_to_ecommerce()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10200);
        let request =Request::new(vec![0,1], vec![30,45],Point::new(2,10));
        let coordinate_request = CoordinateRequest::new(MsgId::from_node(0), Vec::new(), request.amount_of_diferent_products() as u8, Vec::new(), request);
        let msg = RingMessage::from_coordinate_request(coordinate_request);
        let msg_for_ecommerce = tr_receiver.handle_coordinate_request(msg)?.1;
        assert_ecommerce_msg_is_coordinate_request(msg_for_ecommerce);
        Ok(())
    }

    #[actix_rt::test]
    async fn test_3_when_receiving_finished_coordinate_request_and_i_am_not_the_winner_the_result_is_sent()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10300);
        let request =Request::new(vec![0,1], vec![30,45],Point::new(2,10));
        let coordinate_request = CoordinateRequest::new(MsgId::from_node(0), vec![0,1,2,3], request.amount_of_diferent_products() as u8, vec![5,-1,3,4], request);
        let msg = RingMessage::from_coordinate_request(coordinate_request);
        let (msg_to_send ,msg_for_ecommerce) = tr_receiver.handle_coordinate_request(msg)?;
        
        assert_no_ecommerce_msg(msg_for_ecommerce);
        assert_n_messages_of_type(msg_to_send, 1, vec![RingMessageType::CoordinatedRequestResult]);
        
        Ok(())
    }
    
    #[actix_rt::test]
    async fn test_4_when_receiving_finished_coordinate_request_and_i_am_the_winner_the_result_is_not_sent()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10400);
        let request =Request::new(vec![0,1], vec![30,45],Point::new(2,10));
        let coordinate_request = CoordinateRequest::new(MsgId::from_node(0), vec![0,1,2,3], request.amount_of_diferent_products() as u8, vec![0,-1,3,4], request);
        let msg = RingMessage::from_coordinate_request(coordinate_request);
        let (msg_to_send, msg_for_ecommerce) = tr_receiver.handle_coordinate_request(msg)?;
        assert_ecommerce_msg_is_request(msg_for_ecommerce);
        assert_n_messages_of_type(msg_to_send, 0, vec![]);
        Ok(())
    }
    
    #[actix_rt::test]
    async fn test_5_when_no_one_wins_no_result_is_sent()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10500);
        let request =Request::new(vec![0,1], vec![30,45],Point::new(2,10));
        let coordinate_request = CoordinateRequest::new(MsgId::from_node(0), vec![0,1,2,3], request.amount_of_diferent_products() as u8, vec![-1,-1,-1,-1], request);
        let msg = RingMessage::from_coordinate_request(coordinate_request);
        let (msg_to_send, msg_for_ecommerce) = tr_receiver.handle_coordinate_request(msg)?;
        assert_no_ecommerce_msg(msg_for_ecommerce);
        assert_n_messages_of_type(msg_to_send, 0, vec![]);
        Ok(())
    }

    #[actix_rt::test]
    async fn test_6_when_result_is_received_and_i_havent_won_it_is_fowarded()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10600);
        let request =Request::new(vec![0,1], vec![30,45],Point::new(2,10));
        let coordinated_request_result = CoordinatedRequestResult::new(MsgId::new(2, 0), vec![2,3], 1, request.amount_of_diferent_products() as u8, request);
        let msg = RingMessage::from_coordinated_request_result(coordinated_request_result);
        let (msg_to_send, msg_for_ecommerce) = tr_receiver.handle_coordinated_request_result(msg)?;
        assert_no_ecommerce_msg(msg_for_ecommerce);
        assert_n_messages_of_type(msg_to_send, 1, vec![RingMessageType::CoordinatedRequestResult]);
        Ok(())
    }

    #[actix_rt::test]
    async fn test_7_when_result_is_received_and_i_have_won_it_is_fowarded_and_sent_to_eccomerce()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10700);
        let request =Request::new(vec![0,1], vec![30,45],Point::new(2,10));
        let coordinated_request_result = CoordinatedRequestResult::new(MsgId::new(2, 0), vec![2,3], 0, request.amount_of_diferent_products() as u8, request);
        let msg = RingMessage::from_coordinated_request_result(coordinated_request_result);
        let (msg_to_send, msg_for_ecommerce) = tr_receiver.handle_coordinated_request_result(msg)?;
        assert_ecommerce_msg_is_request(msg_for_ecommerce);
        assert_n_messages_of_type(msg_to_send, 1, vec![RingMessageType::CoordinatedRequestResult]);
        Ok(())
    }

    #[actix_rt::test]
    async fn test_8_result_did_full_lap_but_winner_didnt_sign_so_new_coordinate_request_starts()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10800);
        let request =Request::new(vec![0,1], vec![30,45],Point::new(2,10));
        let coordinated_request_result = CoordinatedRequestResult::new(MsgId::new(0, 0), vec![0,2,3], 1, request.amount_of_diferent_products() as u8, request);
        let msg = RingMessage::from_coordinated_request_result(coordinated_request_result);
        let (msg_to_send, msg_for_ecommerce) = tr_receiver.handle_coordinated_request_result(msg)?;
        assert_ecommerce_msg_is_coordinate_request(msg_for_ecommerce);
        assert_n_messages_of_type(msg_to_send, 0, vec![]);
        Ok(())
    }

    
    #[actix_rt::test]
    async fn test_9_can_delete_peer_from_topology()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(3,10900);
        let node_disconected = NodeDisconnected::new(MsgId::new(2, 0), vec![2], 3);
        
        let msg = RingMessage::from_node_disconected(node_disconected);
        let msg_to_send = tr_receiver.handle_node_disconection_message(msg)?;
        assert_n_messages_of_type(msg_to_send, 1, vec![RingMessageType::NodeDisconnected]);
        
        let ring_topology = tr_receiver.ring_topology.lock().unwrap();
        let ring_addresses = tr_receiver.ring_addresses.lock().unwrap();
        assert_eq!(ring_topology.iter().map(|id| *id).collect::<Vec<u16>>(), vec![1,2,0]);
        assert!(ring_addresses.contains_key(&1));
        assert!(ring_addresses.contains_key(&2));
        assert!(!ring_addresses.contains_key(&3));
        Ok(())
    }
    
    #[actix_rt::test]
    async fn test_10_receiving_node_connection_when_not_the_biggest_node_just_resends_it()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(2,11100);
        let addr3 = SocketAddr::from_str("127.0.0.1:11103").unwrap();
        let sender_addr = SocketAddr::from_str("127.0.0.1:11102").unwrap();
        let nc_msg = NodeConnection::new(MsgId::new(1, 0), vec![], 0, addr3, VecDeque::new(), vec![]);
        let msg = RingMessage::from_node_connection(nc_msg);
        tr_receiver.handle_message(msg, sender_addr)?;

        assert_recv_n_messages_of_type(msg_receiver, 2, vec![RingMessageType::NodeConnection, RingMessageType::SendAck]);
        assert_eq!(tr_receiver.ring_topology.lock().unwrap().len(), 3);
        Ok(())
    }

    #[actix_rt::test]
    async fn test_11_receiving_node_connection_when_the_biggest_sets_the_topology()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(2,11200);
        tr_receiver.node_id = 3;
        tr_receiver.ring_topology.lock().unwrap()[2] = 3;
        let removed = tr_receiver.ring_addresses.lock().unwrap().remove(&0).unwrap();
        tr_receiver.ring_addresses.lock().unwrap().insert(3, removed);
        let addr3 = SocketAddr::from_str("127.0.0.1:11103").unwrap();
        let sender_addr = SocketAddr::from_str("127.0.0.1:11102").unwrap();
        let mut nc_msg = NodeConnection::new(MsgId::new(1, 0), vec![1,2], 0, addr3, VecDeque::new(), vec![]);
        let msg = RingMessage::from_node_connection(nc_msg.clone());
        tr_receiver.handle_message(msg, sender_addr)?;
        
        nc_msg.amount_of_peers = 4;
        nc_msg.topology_ids = VecDeque::from([1,2,3,4]);
        let addresses = tr_receiver.ring_addresses.lock().unwrap();
        nc_msg.topology_addresses = vec![addresses[&1], addresses[&2], addresses[&3], addresses[&4]];
        let mut msg = RingMessage::from_node_connection(nc_msg.clone());
        msg.header.sender_addr = Some(sender_addr);
        msg_receiver.recv().unwrap();
        assert_eq!(msg, msg_receiver.recv().unwrap());
        assert_eq!(tr_receiver.ring_topology.lock().unwrap().len(), 4);
        Ok(())
    }


    #[actix_rt::test]
    async fn test_12_initialize_new_node_by_receiving_node_connection()->Result<(), TokenRingError>{
        let addr0 = SocketAddr::from_str("127.0.0.1:11300").unwrap();
        let addr1 = SocketAddr::from_str("127.0.0.1:11301").unwrap();
        let addr2 = SocketAddr::from_str("127.0.0.1:11302").unwrap();
        let addr3 = SocketAddr::from_str("127.0.0.1:11303").unwrap();
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(0,11300);
        tr_receiver.node_id = u16::MAX;
        tr_receiver.ring_topology.lock().unwrap().extend([0,1]);
        {let mut ring_addresses = tr_receiver.ring_addresses.lock().unwrap();
            ring_addresses.insert(0, addr1);
            ring_addresses.insert(u16::MAX, addr0);}
        let all_addr = vec![addr1,addr2,addr3,addr0];
        let nc_msg = NodeConnection::new(MsgId::new(1, 0), vec![1,2,3], 4, addr0, VecDeque::from([1,2,3,4]), all_addr);
        let msg_to_send = tr_receiver.handle_node_connection(RingMessage::from_node_connection(nc_msg))?;
        let final_ring_addresses = tr_receiver.ring_addresses.lock().unwrap();
        let final_ring_topology = tr_receiver.ring_topology.lock().unwrap();
        assert_eq!(tr_receiver.node_id, 4);
        assert_eq!(final_ring_addresses.get(&1).unwrap(),&addr1);
        assert_eq!(final_ring_addresses.get(&2).unwrap(),&addr2);
        assert_eq!(final_ring_addresses.get(&3).unwrap(),&addr3);
        assert_eq!(final_ring_addresses.get(&4).unwrap(),&addr0);
        assert_eq!(final_ring_addresses.len(),4);
        assert_eq!(*final_ring_topology,vec![1,2,3,4]);
        assert_n_messages_of_type(msg_to_send, 2, vec![RingMessageType::NodeConnection, RingMessageType::ConfirmConnection]);
        Ok(())
    }
    
    #[actix_rt::test]
    async fn test_13_confirm_connection_id_not_in_topology_is_then_added()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(1,11400);
        let addr0 = SocketAddr::from_str("127.0.0.1:11400").unwrap();
        let addr1 = SocketAddr::from_str("127.0.0.1:11401").unwrap();
        let addr2 = SocketAddr::from_str("127.0.0.1:11402").unwrap();
        let cc_message = ConfirmConnection::new(MsgId::new(2, 0), vec![2], 2, addr2);
        let msg = RingMessage::from_confirm_connection(cc_message);
        tr_receiver.handle_message(msg, addr2)?;
        let final_ring_addresses = tr_receiver.ring_addresses.lock().unwrap();
        let final_ring_topology = tr_receiver.ring_topology.lock().unwrap();
        assert_eq!(final_ring_addresses.get(&0).unwrap(),&addr0);
        assert_eq!(final_ring_addresses.get(&1).unwrap(),&addr1);
        assert_eq!(final_ring_addresses.get(&2).unwrap(),&addr2);
        assert_eq!(*final_ring_topology, VecDeque::from([1,2,0]));
        assert_recv_n_messages_of_type(msg_receiver, 2, vec![RingMessageType::ConfirmConnection, RingMessageType::SendAck]);
        Ok(())
    }
    
    #[actix_rt::test]
    async fn test_14_confirm_connection_id_in_topology_is_just_fowarded()->Result<(), TokenRingError>{
        let (mut tr_receiver, msg_receiver) = create_token_ring_receiver(2,11500);
        let addr0 = SocketAddr::from_str("127.0.0.1:11500").unwrap();
        let addr1 = SocketAddr::from_str("127.0.0.1:11501").unwrap();
        let addr2 = SocketAddr::from_str("127.0.0.1:11502").unwrap();
        let cc_message = ConfirmConnection::new(MsgId::new(2, 0), vec![2], 2, addr2);
        let msg = RingMessage::from_confirm_connection(cc_message);
        tr_receiver.handle_message(msg, addr2)?;
        let final_ring_addresses = tr_receiver.ring_addresses.lock().unwrap();
        let final_ring_topology = tr_receiver.ring_topology.lock().unwrap();
        assert_eq!(final_ring_addresses.get(&0).unwrap(),&addr0);
        assert_eq!(final_ring_addresses.get(&1).unwrap(),&addr1);
        assert_eq!(final_ring_addresses.get(&2).unwrap(),&addr2);
        assert_eq!(*final_ring_topology, VecDeque::from([1,2,0]));
        assert_recv_n_messages_of_type(msg_receiver, 2, vec![RingMessageType::ConfirmConnection, RingMessageType::SendAck]);
        Ok(())
    }    

    #[actix_rt::test]
    async fn test_15_receive_own_messages_removes_them_from_the_queue() -> Result<(), TokenRingError>{
        let (mut tr_receiver,  _msg_receiver) = create_token_ring_receiver(2,11600);
        let sender_addr = SocketAddr::from_str("127.0.0.1:11602").unwrap();
        let mut messages = Vec::new();
        for i in 0..5{
            let addr = SocketAddr::from_str("127.0.0.1:11603").unwrap();
            let msg = ConfirmConnection::new(MsgId::new(0,i), vec![0], 3, addr);
            let mut msg = RingMessage::from_confirm_connection(msg);
            tr_receiver.created_messages.insert(msg.header.id, msg.clone());
            msg.sign(1);
            msg.sign(2);
            messages.push(msg);
        }
        
        for msg in messages{
            tr_receiver.handle_message(msg, sender_addr)?;
        }
        
        assert!(tr_receiver.created_messages.is_empty()?);
        
        Ok(())
    }
    
    #[actix_rt::test]
    async fn test_16_when_receiving_a_disconection_message_the_receiver_stops_handling_messages() -> Result<(), TokenRingError> {
        let (mut tr_receiver,  msg_receiver) = create_token_ring_receiver(2,11700);
        let msg = SetConnectionStatus::new(false, None);
        tr_receiver.handle_set_connection_status(RingMessage::from_set_connection_status(msg))?;
        let ack_msg = RingMessage::new_ack(MsgId::new(0,0), RingMessageType::SendAck);
        tr_receiver.handle_message(ack_msg, SocketAddr::from_str("127.0.0.1:11702").unwrap())?;
        
        assert_recv_n_messages_of_type(msg_receiver, 1, vec![RingMessageType::SetConnectionStatus]);
        assert!(!tr_receiver.connected);
        assert_eq!(tr_receiver.node_id, u16::MAX);
        Ok(())
    }

    #[actix_rt::test]
    async fn test_17_when_receiving_a_reconnection_message_the_receiver_begins_to_handle_messages() -> Result<(), TokenRingError> {
        let (mut tr_receiver,  msg_receiver) = create_token_ring_receiver(2,11800);
        tr_receiver.connected = false;
        let msg = SetConnectionStatus::new(true, None);
        tr_receiver.handle_set_connection_status(RingMessage::from_set_connection_status(msg))?;
        let ack_msg = RingMessage::new_ack(MsgId::new(0,0), RingMessageType::SendAck);
        tr_receiver.handle_message(ack_msg, SocketAddr::from_str("127.0.0.1:11802").unwrap())?;
        
        assert_recv_n_messages_of_type(msg_receiver, 2, vec![RingMessageType::SetConnectionStatus, RingMessageType::ReceivedAck]);
        assert!(tr_receiver.connected);
        
        
        Ok(())
    }

}