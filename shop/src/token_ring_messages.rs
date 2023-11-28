//use std::ops::IndexMut;

use std::{
    collections::VecDeque,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use actix::Message;

use crate::{messages::Request, token_ring::TokenRingError};

const MSG_ID_SIZE: usize = 4;
const FINAL_MSG_TYPE_POSITION: usize = MSG_ID_SIZE + 1;
const FINAL_AMOUNT_OF_SIGS_POSITION: usize = FINAL_MSG_TYPE_POSITION + 2;
const HEADER_SIZE: usize = FINAL_AMOUNT_OF_SIGS_POSITION + 2;

/// ByteConvertable is a trait that allows to convert a type to a byte sequence and viceversa
/// It is implemented for all the types that are used in the messages of the token ring
pub trait ByteConvertable {
    type Type;
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, TokenRingError>;
}

/// RingMessageType is an enum that represents the type of message that is being sent
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RingMessageType {
    CoordinateRequest,
    CoordinatedRequestResult,
    NodeConnection,
    NodeDisconnected,
    ConfirmConnection,
    SendAck,
    ReceivedAck,
    SetConnectionStatus,
    EndOfProgram,
}

impl ByteConvertable for RingMessageType {
    type Type = RingMessageType;

    fn to_bytes(&self) -> Vec<u8> {
        vec![self.clone() as u8]
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, TokenRingError> {
        if bytes.len() != 1 {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        match bytes[0] {
            0 => Ok(RingMessageType::CoordinateRequest),
            1 => Ok(RingMessageType::CoordinatedRequestResult),
            2 => Ok(RingMessageType::NodeConnection),
            3 => Ok(RingMessageType::NodeDisconnected),
            4 => Ok(RingMessageType::ConfirmConnection),
            5 => Ok(RingMessageType::SendAck),
            6 => Ok(RingMessageType::ReceivedAck),
            7 => Ok(RingMessageType::SetConnectionStatus),
            8 => Ok(RingMessageType::EndOfProgram),
            _ => Err(TokenRingError::ErrorInvalidByteSequence),
        }
    }
}

/// RingMessageHeader is a struct that represents the header of a message
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingMessageHeader {
    pub id: MsgId,
    pub msg_type: RingMessageType,
    pub ammount_of_signatures: u16,
    pub payload_size: u16,
    pub sender_addr: Option<SocketAddr>,
}

impl RingMessageHeader {
    /// Creates a new RingMessageHeader given the id of the message, the type of message,
    /// the ammount of signatures that the message has and the size of the payload

    pub fn new(
        id: MsgId,
        msg_type: RingMessageType,
        ammount_of_signatures: u16,
        payload_size: u16,
    ) -> Self {
        Self::_new(id, msg_type, ammount_of_signatures, payload_size, None)
    }

    pub fn _new(
        id: MsgId,
        msg_type: RingMessageType,
        ammount_of_signatures: u16,
        payload_size: u16,
        sender_addr: Option<SocketAddr>,
    ) -> Self {
        RingMessageHeader {
            id,
            msg_type,
            ammount_of_signatures,
            payload_size,
            sender_addr,
        }
    }

    /// Returns if the message is from the node with the given id
    pub fn is_from_node(&self, node_id: u16) -> bool {
        self.id.is_from_node(node_id)
    }

    /// Returns if the message was recently created by the node with the given id
    pub fn recently_created_by(&self, node_id: u16) -> bool {
        self.is_from_node(node_id) && self.ammount_of_signatures == 1
    }
}

impl Default for RingMessageHeader {
    fn default() -> Self {
        Self {
            id: Default::default(),
            msg_type: RingMessageType::SetConnectionStatus,
            ammount_of_signatures: Default::default(),
            payload_size: Default::default(),
            sender_addr: Default::default(),
        }
    }
}

impl ByteConvertable for RingMessageHeader {
    type Type = RingMessageHeader;

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.id.to_bytes();
        bytes.push(self.msg_type.clone() as u8);
        bytes.extend(self.ammount_of_signatures.to_be_bytes());
        bytes.extend(self.payload_size.to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, TokenRingError> {
        if bytes.len() != HEADER_SIZE {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let id = MsgId::from_bytes(&bytes[..MSG_ID_SIZE])?;
        let msg_type = RingMessageType::from_bytes(&bytes[MSG_ID_SIZE..FINAL_MSG_TYPE_POSITION])?;
        let ammount_of_signatures = u16::from_be_bytes([
            bytes[FINAL_MSG_TYPE_POSITION],
            bytes[FINAL_MSG_TYPE_POSITION + 1],
        ]);
        let payload_size =
            u16::from_be_bytes([bytes[FINAL_AMOUNT_OF_SIGS_POSITION], bytes[HEADER_SIZE - 1]]);

        Ok(RingMessageHeader::new(
            id,
            msg_type,
            ammount_of_signatures,
            payload_size,
        ))
    }
}

/// MsgId is a struct that represents the id of a message
///
/// ## Attributes
///
/// * `node_id` - The id of the node that created the message
/// * `seq_num` - The sequence number of the message
#[derive(Eq, PartialEq, Hash, Clone, Copy, Debug)]
pub struct MsgId {
    pub node_id: u16,
    pub seq_num: u16,
}

impl MsgId {
    /// Creates a new MsgId given the id of the node that created the message and the sequence number
    /// of the message
    pub fn new(node_id: u16, seq_num: u16) -> Self {
        MsgId { node_id, seq_num }
    }

    /// Creates a new MsgId given the id of the node that created the message
    pub fn from_node(node_id: u16) -> Self {
        MsgId::new(node_id, u16::MAX)
    }

    /// Returns if the message is from the node with the given id
    pub fn is_from_node(&self, node_id: u16) -> bool {
        node_id == self.node_id
    }
}

impl Default for MsgId {
    fn default() -> Self {
        MsgId::new(u16::MAX, u16::MAX)
    }
}

impl ByteConvertable for MsgId {
    type Type = MsgId;

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::from(self.node_id.to_be_bytes());
        bytes.extend(Vec::from(self.seq_num.to_be_bytes()));
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, TokenRingError> {
        if bytes.len() != MSG_ID_SIZE {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let node_id = u16::from_be_bytes([bytes[0], bytes[1]]);
        let seq_num = u16::from_be_bytes([bytes[2], bytes[3]]);
        Ok(MsgId::new(node_id, seq_num))
    }
}

/// RingMessage is a struct that represents a message that is sent through the token ring
///
/// ## Attributes
///
/// * `header` - The header of the message
/// * `signatures` - The signatures of the message, that is, the ids of the nodes that have sent the message
/// * `payload` - The payload of the message
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingMessage {
    pub header: RingMessageHeader,
    pub signatures: Vec<u16>,
    pub payload: Vec<u8>,
}

impl ByteConvertable for RingMessage {
    type Type = RingMessage;

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.header.to_bytes();
        for signature in &self.signatures {
            bytes.extend(signature.to_be_bytes());
        }
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, TokenRingError> {
        let header = RingMessageHeader::from_bytes(&bytes[..HEADER_SIZE])?;
        if bytes.len() - HEADER_SIZE
            != (2 * header.ammount_of_signatures + header.payload_size) as usize
        {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let mut signatures = Vec::new();
        for i in 0..header.ammount_of_signatures as usize {
            let signature =
                u16::from_be_bytes([bytes[HEADER_SIZE + 2 * i], bytes[HEADER_SIZE + 1 + 2 * i]]);
            signatures.push(signature);
        }
        let payload = bytes[HEADER_SIZE + 2 * signatures.len()..].to_vec();

        Ok(RingMessage {
            header,
            signatures,
            payload,
        })
    }
}

impl RingMessage {
    /// Creates a new RingMessage given the header of the message and the payload
    pub fn new(header: RingMessageHeader, payload: Vec<u8>) -> Self {
        RingMessage {
            header,
            signatures: Vec::new(),
            payload,
        }
    }

    /// Creates a new RingMessage that serves as an ack for the message with the given id
    pub fn new_ack(id: MsgId, msg_type: RingMessageType) -> Self {
        let header = RingMessageHeader::new(id, msg_type, 0, 0);
        RingMessage {
            header,
            signatures: Vec::new(),
            payload: Vec::new(),
        }
    }

    pub fn new_end_of_program() -> RingMessage {
        let header = RingMessageHeader::new(MsgId::default(), RingMessageType::EndOfProgram, 0, 0);
        RingMessage {
            header,
            signatures: Vec::new(),
            payload: Vec::new(),
        }
    }

    /// Creates a new RingMessage from a CoordinateRequest
    pub fn from_coordinate_request(msg: CoordinateRequest) -> RingMessage {
        let mut payload: Vec<u8> = vec![msg.amount_of_diferent_products];
        payload.extend(
            msg.distances
                .into_iter()
                .flat_map(|distance| distance.to_be_bytes()),
        );
        payload.extend(msg.request.to_bytes());
        let header = RingMessageHeader::_new(
            msg.msg_id,
            RingMessageType::CoordinateRequest,
            msg.signatures.len() as u16,
            payload.len() as u16,
            msg.sender_addr,
        );
        RingMessage {
            header,
            signatures: msg.signatures,
            payload,
        }
    }

    /// Creates a new RingMessage from a CoordinatedRequestResult
    pub fn from_coordinated_request_result(msg: CoordinatedRequestResult) -> RingMessage {
        let mut payload: Vec<u8> = Vec::from(msg.winner_id.to_be_bytes());
        payload.push(msg.amount_of_diferent_products);
        payload.extend(msg.request.to_bytes());
        let header = RingMessageHeader::_new(
            MsgId::from_node(msg.msg_id.node_id),
            RingMessageType::CoordinatedRequestResult,
            msg.signatures.len() as u16,
            payload.len() as u16,
            msg.sender_addr,
        );
        RingMessage {
            header,
            signatures: msg.signatures,
            payload,
        }
    }

    /// Creates a new RingMessage from a NodeDisconnected
    pub fn from_node_disconected(msg: NodeDisconnected) -> RingMessage {
        let header = RingMessageHeader::_new(
            msg.msg_id,
            RingMessageType::NodeDisconnected,
            msg.signatures.len() as u16,
            2,
            msg.sender_addr,
        );
        let payload: Vec<u8> = msg.disconected_node.to_be_bytes().to_vec();
        RingMessage {
            header,
            signatures: msg.signatures,
            payload,
        }
    }

    /// Creates a new RingMessage from a NodeConnection
    pub fn from_node_connection(msg: NodeConnection) -> RingMessage {
        let mut payload = msg.amount_of_peers.to_be_bytes().to_vec();
        payload.extend(msg.new_node_addr.to_bytes());
        payload.extend(msg.topology_ids.into_iter().flat_map(|id| id.to_be_bytes()));
        payload.extend(
            msg.topology_addresses
                .into_iter()
                .flat_map(|addr| addr.to_bytes()),
        );
        let header = RingMessageHeader::_new(
            msg.msg_id,
            RingMessageType::NodeConnection,
            msg.signatures.len() as u16,
            payload.len() as u16,
            msg.sender_addr,
        );
        RingMessage {
            header,
            signatures: msg.signatures,
            payload,
        }
    }

    /// Creates a new RingMessage from a ConfirmConnection
    pub fn from_confirm_connection(msg: ConfirmConnection) -> RingMessage {
        let mut payload = msg.node_id.to_be_bytes().to_vec();
        payload.extend(msg.addr.to_bytes());
        let header = RingMessageHeader::_new(
            msg.msg_id,
            RingMessageType::ConfirmConnection,
            msg.signatures.len() as u16,
            payload.len() as u16,
            msg.sender_addr,
        );

        RingMessage {
            header,
            signatures: msg.signatures,
            payload,
        }
    }

    /// Creates a new RingMessage from a SetConnectionStatus
    pub fn from_set_connection_status(msg: SetConnectionStatus) -> RingMessage {
        let mut payload = vec![if !msg.new_connection_status { 0 } else { 1 }];

        if let Some(connect_to) = msg.connect_to {
            payload.extend(connect_to.to_bytes())
        }
        let header = RingMessageHeader {
            payload_size: payload.len() as u16,
            ..Default::default()
        };
        RingMessage::new(header, payload)
    }

    /// Returns if the message did a full lap, that is, if the node with the given id is in the signatures
    pub fn did_full_lap(&self, node_id: u16) -> bool {
        self.signatures.contains(&node_id)
    }

    /// Given a node id, it signs the message if it has not been signed by the node with the given id
    pub fn sign(&mut self, node_id: u16) {
        if let Some(last_signature) = self.signatures.last() {
            if *last_signature == node_id {
                return;
            }
        }
        self.signatures.push(node_id);
        self.header.ammount_of_signatures += 1;
    }

    /// Returns the id of the node that last sent the message
    pub fn last_sent_by(&self) -> u16 {
        match self.signatures.last() {
            Some(last_signature) => *last_signature,
            None => self.header.id.node_id,
        }
    }
}

/// CoordinateRequest is a struct that represents a CoordinateRequest message
///
/// A CoordinateRequest message is sent by a node when it wants to coordinate a request
/// with the other nodes in the ring
///
/// ## Attributes
///
/// * `msg_id` - The id of the message
/// * `signatures` - The signatures of the message
/// * `amount_of_diferent_products` - The amount of diferent products that the request has
/// * `distances` - The distances of the node to the destination of the request
/// * `request` - The request that is being coordinated
#[derive(Message, Debug, PartialEq, Eq, Clone)]
#[rtype(result = "()")]
pub struct CoordinateRequest {
    pub msg_id: MsgId,
    pub signatures: Vec<u16>,
    pub amount_of_diferent_products: u8,
    pub distances: Vec<i32>,
    pub request: Request,
    sender_addr: Option<SocketAddr>,
}

impl CoordinateRequest {
    /// Creates a new CoordinateRequest given the id of the message, the signatures of the message,
    /// the amount of diferent products that the request has, the distances of the node to the destination
    /// of the request and the request that is being coordinated
    pub fn new(
        msg_id: MsgId,
        signatures: Vec<u16>,
        amount_of_diferent_products: u8,
        distances: Vec<i32>,
        request: Request,
    ) -> CoordinateRequest {
        CoordinateRequest {
            msg_id,
            signatures,
            amount_of_diferent_products,
            distances,
            request,
            sender_addr: None,
        }
    }

    /// Creates a new CoordinateRequest given the id of the node that created the message, the request
    /// that is being coordinated and the amount of diferent products that the request has
    pub fn start(
        node_id: u16,
        request: Request,
        amount_of_diferent_products: u8,
    ) -> CoordinateRequest {
        CoordinateRequest::new(
            MsgId::new(node_id, 255),
            Vec::new(),
            amount_of_diferent_products,
            Vec::new(),
            request,
        )
    }

    /// Creates a new CoordinateRequest from a RingMessage
    pub fn from_ring_message(msg: RingMessage) -> Result<CoordinateRequest, TokenRingError> {
        if msg.payload.is_empty() || msg.header.msg_type != RingMessageType::CoordinateRequest {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }

        let (amount_of_diferent_products, mut payload) = msg.payload.split_at(1);
        let amount_of_diferent_products = amount_of_diferent_products[0];
        if msg.payload.len()
            != 1 + msg.signatures.len() * 4 + 2 * 4 * (amount_of_diferent_products as usize) + 8
        {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let mut distances = Vec::new();
        let mut aux;
        for _i in 0..msg.signatures.len() {
            (aux, payload) = payload.split_at(4);
            let distance: [u8; 4] = aux
                .try_into()
                .map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
            distances.push(i32::from_be_bytes(distance));
        }

        let request = Request::from_bytes(payload.to_vec(), amount_of_diferent_products)?;
        Ok(CoordinateRequest {
            msg_id: msg.header.id,
            signatures: msg.signatures,
            amount_of_diferent_products,
            distances,
            request,
            sender_addr: msg.header.sender_addr,
        })
    }

    /// Decides the winner of the coordination, that is, the node that is closest to the destination
    /// of the request
    pub fn decide_winner(&self) -> Option<u16> {
        let (mut index, mut min_aux) = (0, -1);
        for (i, distance) in self.distances.iter().enumerate() {
            if min_aux == -1 || (*distance >= 0 && *distance < min_aux) {
                (index, min_aux) = (i, *distance)
            }
        }

        if min_aux == -1 {
            return None;
        }
        Some(self.signatures[index])
    }

    /// Returns if the message did a full lap, that is, if the node with the given id is in the signatures
    pub fn did_full_lap(&self, node_id: u16) -> bool {
        self.signatures.contains(&node_id)
    }

    /// Returns the result of the coordination, that is, a CoordinatedRequestResult
    pub fn get_result(&self, node_id: u16) -> Option<CoordinatedRequestResult> {
        let winner_id = self.decide_winner()?;
        Some(CoordinatedRequestResult::new(
            MsgId::from_node(node_id),
            Vec::new(),
            winner_id,
            self.request.amount_of_diferent_products(),
            self.request.clone(),
        ))
    }
}

/// CoordinatedRequestResult is a struct that represents a CoordinatedRequestResult message
///
/// A CoordinatedRequestResult message is sent by a node when it wants to share the result of the
/// coordination with the other nodes in the ring
///
/// ## Attributes
///
/// * `msg_id` - The id of the message
/// * `signatures` - The signatures of the message
/// * `winner_id` - The id of the node that won the coordination
/// * `amount_of_diferent_products` - The amount of diferent products that the request has
/// * `request` - The request that was coordinated
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CoordinatedRequestResult {
    pub msg_id: MsgId,
    pub signatures: Vec<u16>,
    pub winner_id: u16,
    pub amount_of_diferent_products: u8,
    pub request: Request,
    sender_addr: Option<SocketAddr>,
}

impl CoordinatedRequestResult {
    /// Creates a new CoordinatedRequestResult given the id of the message, the signatures of the message,
    /// the id of the node that won the coordination, the amount of diferent products that the request has
    /// and the request that was coordinated
    pub fn new(
        msg_id: MsgId,
        signatures: Vec<u16>,
        winner_id: u16,
        amount_of_diferent_products: u8,
        request: Request,
    ) -> CoordinatedRequestResult {
        CoordinatedRequestResult {
            msg_id,
            signatures,
            winner_id,
            amount_of_diferent_products,
            request,
            sender_addr: None,
        }
    }

    /// Creates a new CoordinatedRequestResult from a RingMessage
    pub fn from_ring_message(msg: RingMessage) -> Result<CoordinatedRequestResult, TokenRingError> {
        if msg.payload.len() < 3 || msg.header.msg_type != RingMessageType::CoordinatedRequestResult
        {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let aux: [u8; 2] = msg.payload[..2]
            .try_into()
            .map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
        let winner_id = u16::from_be_bytes(aux);

        let amount_of_diferent_products = msg.payload[2];

        let request = Request::from_bytes(msg.payload[3..].to_vec(), amount_of_diferent_products)?;
        Ok(CoordinatedRequestResult {
            msg_id: msg.header.id,
            signatures: msg.signatures,
            winner_id,
            amount_of_diferent_products,
            request,
            sender_addr: msg.header.sender_addr,
        })
    }

    /// Returns if the message did a full lap, that is, if the node with the given id is in the signatures
    pub fn did_full_lap(&self, node_id: u16) -> bool {
        self.signatures.contains(&node_id)
    }
}

/// NodeConnection is a struct that represents a NodeConnection message
///
/// A NodeConnection message is sent by a node when it wants to connect to the ring
///
/// ## Attributes
///
/// * `msg_id` - The id of the message
/// * `signatures` - The signatures of the message
/// * `amount_of_peers` - The amount of peers that the node has
/// * `new_node_addr` - The address of the node that wants to connect to the ring
/// * `topology_ids` - The ids of the nodes in the ring
/// * `topology_addresses` - The addresses of the nodes in the ring
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NodeConnection {
    pub msg_id: MsgId,
    pub signatures: Vec<u16>,
    pub amount_of_peers: u16,
    pub new_node_addr: SocketAddr,
    pub topology_ids: VecDeque<u16>,
    pub topology_addresses: Vec<SocketAddr>,
    sender_addr: Option<SocketAddr>,
}

impl NodeConnection {
    /// Creates a new NodeConnection given the id of the message, the signatures of the message,
    /// the amount of peers that the node has, the address of the node that wants to connect to the ring,
    /// the ids of the nodes in the ring and the addresses of the nodes in the ring
    pub fn new(
        msg_id: MsgId,
        signatures: Vec<u16>,
        amount_of_peers: u16,
        new_node_addr: SocketAddr,
        topology_ids: VecDeque<u16>,
        topology_addresses: Vec<SocketAddr>,
    ) -> NodeConnection {
        NodeConnection {
            msg_id,
            signatures,
            amount_of_peers,
            new_node_addr,
            topology_ids,
            topology_addresses,
            sender_addr: None,
        }
    }

    /// Creates a new NodeConnection given the address of the node that wants to connect to the ring
    pub fn start(new_node_addr: SocketAddr) -> NodeConnection {
        NodeConnection::new(
            MsgId::default(),
            Vec::new(),
            0,
            new_node_addr,
            VecDeque::new(),
            Vec::new(),
        )
    }

    /// Creates a new NodeConnection from a RingMessage
    pub fn from_ring_message(msg: RingMessage) -> Result<NodeConnection, TokenRingError> {
        if msg.payload.len() < 8 || msg.header.msg_type != RingMessageType::NodeConnection {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }

        let (amount_of_peers_bytes, payload) = msg.payload.split_at(2);
        let amount_of_peers =
            u16::from_be_bytes([amount_of_peers_bytes[0], amount_of_peers_bytes[1]]);
        let (new_addr_bytes, mut payload) = payload.split_at(6);
        let new_node_addr = SocketAddr::from_bytes(new_addr_bytes)?;

        if payload.len() != amount_of_peers as usize * (2 + 6) {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }

        let mut topology_ids = VecDeque::new();
        let mut aux;
        for _i in 0..amount_of_peers {
            (aux, payload) = payload.split_at(2);
            let id: [u8; 2] = aux
                .try_into()
                .map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
            topology_ids.push_back(u16::from_be_bytes(id));
        }

        let mut topology_addresses = Vec::new();
        for _i in 0..amount_of_peers {
            (aux, payload) = payload.split_at(6);
            topology_addresses.push(
                SocketAddr::from_bytes(aux)
                    .map_err(|_| TokenRingError::ErrorInvalidByteSequence)?,
            );
        }
        Ok(NodeConnection {
            msg_id: msg.header.id,
            signatures: msg.signatures,
            amount_of_peers,
            new_node_addr,
            topology_ids,
            topology_addresses,
            sender_addr: msg.header.sender_addr,
        })
    }
}

/// ConfirmConnection is a struct that represents a ConfirmConnection message
///
/// A ConfirmConnection message is sent by a node when it wants to confirm that it has connected to the ring
///
/// ## Attributes
///
/// * `msg_id` - The id of the message
/// * `signatures` - The signatures of the message
/// * `node_id` - The id of the node that has connected to the ring
/// * `addr` - The address of the node that has connected to the ring
pub struct ConfirmConnection {
    msg_id: MsgId,
    signatures: Vec<u16>,
    pub node_id: u16,
    pub addr: SocketAddr,
    sender_addr: Option<SocketAddr>,
}

impl ConfirmConnection {
    /// Creates a new ConfirmConnection given the id of the message, the signatures of the message,
    /// the id of the node that has connected to the ring and the address of the node that has connected
    /// to the ring
    pub fn new(
        msg_id: MsgId,
        signatures: Vec<u16>,
        node_id: u16,
        addr: SocketAddr,
    ) -> ConfirmConnection {
        ConfirmConnection {
            msg_id,
            signatures,
            node_id,
            addr,
            sender_addr: None,
        }
    }

    /// Creates a new ConfirmConnection from a RingMessage
    pub fn from_ring_message(msg: RingMessage) -> Result<ConfirmConnection, TokenRingError> {
        if msg.payload.len() != 8 || msg.header.msg_type != RingMessageType::ConfirmConnection {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let (node_id_bytes, addr_bytes) = msg.payload.split_at(2);
        let node_id = u16::from_be_bytes([node_id_bytes[0], node_id_bytes[1]]);
        let addr = SocketAddr::from_bytes(addr_bytes)?;
        Ok(ConfirmConnection {
            msg_id: msg.header.id,
            signatures: msg.signatures,
            node_id,
            addr,
            sender_addr: msg.header.sender_addr,
        })
    }
}

/// NodeDisconnected is a struct that represents a NodeDisconnected message
///
/// A NodeDisconnected message is sent by a node when it wants to notify that a node has disconnected
/// from the ring
///
/// ## Attributes
///
/// * `msg_id` - The id of the message
/// * `signatures` - The signatures of the message
/// * `disconected_node` - The id of the node that has disconnected from the ring
pub struct NodeDisconnected {
    msg_id: MsgId,
    signatures: Vec<u16>,
    pub disconected_node: u16,
    sender_addr: Option<SocketAddr>,
}

impl NodeDisconnected {
    /// Creates a new NodeDisconnected given the id of the message, the signatures of the message
    /// and the id of the node that has disconnected from the ring
    pub fn new(msg_id: MsgId, signatures: Vec<u16>, disconected_node: u16) -> NodeDisconnected {
        NodeDisconnected {
            msg_id,
            signatures,
            disconected_node,
            sender_addr: None,
        }
    }

    /// Creates a new NodeDisconnected from a RingMessage
    pub fn from_ring_message(msg: RingMessage) -> Result<NodeDisconnected, TokenRingError> {
        if msg.payload.len() != 2 || msg.header.msg_type != RingMessageType::NodeDisconnected {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }

        let disconected_node = u16::from_be_bytes([msg.payload[0], msg.payload[1]]);
        Ok(NodeDisconnected {
            msg_id: msg.header.id,
            signatures: msg.signatures,
            disconected_node,
            sender_addr: msg.header.sender_addr,
        })
    }

    /// Returns if the message did a full lap, that is, if the node with the given id is in the signatures
    pub fn did_full_lap(&self, node_id: u16) -> bool {
        self.signatures.contains(&node_id)
    }
}

pub struct SetConnectionStatus {
    pub new_connection_status: bool,
    pub connect_to: Option<SocketAddr>,
}

impl SetConnectionStatus {
    pub fn new(connected: bool, connect_to: Option<SocketAddr>) -> SetConnectionStatus {
        SetConnectionStatus {
            new_connection_status: connected,
            connect_to,
        }
    }

    pub fn from_ring_message(msg: RingMessage) -> Result<SetConnectionStatus, TokenRingError> {
        if !Self::valid_length(msg.payload.len())
            || msg.header.msg_type != RingMessageType::SetConnectionStatus
        {
            return Err(TokenRingError::ErrorInvalidByteSequence);
        }
        let status = msg.payload[0] != 0;
        let mut connected_to = None;

        if msg.payload.len() == 7 {
            connected_to = Some(SocketAddr::from_bytes(&msg.payload[1..])?);
        }

        Ok(SetConnectionStatus::new(status, connected_to))
    }

    fn valid_length(n: usize) -> bool {
        n == 1 || n == 7
    }
}

impl ByteConvertable for SocketAddr {
    type Type = SocketAddr;

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.ip().to_bytes();
        bytes.extend(self.port().to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, TokenRingError> {
        if bytes.len() == 6 {
            let (ip_bytes, port_bytes) = bytes.split_at(4);
            let ip = IpAddr::from_bytes(ip_bytes)?;
            let port = u16::from_be_bytes([port_bytes[0], port_bytes[1]]);
            return Ok(SocketAddr::from((ip, port)));
        }
        if bytes.len() == 18 {
            let (ip_bytes, port_bytes) = bytes.split_at(16);
            let ip = IpAddr::from_bytes(ip_bytes)?;
            let port = u16::from_be_bytes([port_bytes[0], port_bytes[1]]);
            return Ok(SocketAddr::from((ip, port)));
        }
        Err(TokenRingError::ErrorInvalidByteSequence)
    }
}

impl ByteConvertable for IpAddr {
    type Type = IpAddr;

    fn to_bytes(&self) -> Vec<u8> {
        match self {
            IpAddr::V4(ip) => ip.octets().to_vec(),
            IpAddr::V6(ip) => ip.octets().to_vec(),
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, TokenRingError> {
        if bytes.len() == 4 {
            let bytes: [u8; 4] = bytes
                .try_into()
                .map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
            return Ok(IpAddr::V4(Ipv4Addr::from(bytes)));
        } else if bytes.len() == 16 {
            let bytes: [u8; 16] = bytes
                .try_into()
                .map_err(|_| TokenRingError::ErrorInvalidByteSequence)?;
            return Ok(IpAddr::V6(Ipv6Addr::from(bytes)));
        }
        Err(TokenRingError::ErrorInvalidByteSequence)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::point::Point;

    use super::*;

    #[test]
    fn test_1_to_bytes() {
        let msg_header = RingMessageHeader::new(
            MsgId::new(5, 7),
            RingMessageType::CoordinatedRequestResult,
            0,
            5,
        );
        let mut msg = RingMessage::new(msg_header, vec![0, 1, 2, 3, 4]);
        msg.sign(0);
        msg.sign(1);

        let mut bytes = (5 as u16).to_be_bytes().to_vec();
        bytes.extend((7 as u16).to_be_bytes());
        bytes.push(1);
        bytes.extend((2 as u16).to_be_bytes());
        bytes.extend((5 as u16).to_be_bytes());
        bytes.extend(
            [(0 as u16).to_be_bytes(), (1 as u16).to_be_bytes()]
                .iter()
                .flatten(),
        );
        bytes.extend([0, 1, 2, 3, 4]);

        assert_eq!(bytes, msg.to_bytes())
    }

    #[test]
    fn test_2_from_bytes() {
        let msg_header = RingMessageHeader::new(
            MsgId::new(5, 7),
            RingMessageType::CoordinatedRequestResult,
            0,
            5,
        );
        let mut msg = RingMessage::new(msg_header, vec![0, 1, 2, 3, 4]);
        msg.sign(0);
        msg.sign(1);

        let msg_from_bytes = RingMessage::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(msg_from_bytes, msg)
    }

    #[test]
    fn test_3_coordinate_request_from_ring_message() -> Result<(), TokenRingError> {
        let msg_header =
            RingMessageHeader::new(MsgId::new(5, 7), RingMessageType::CoordinateRequest, 0, 5);
        let mut msg = RingMessage::new(
            msg_header,
            vec![
                2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 30, 0, 0, 0, 45, 0, 0,
                0, 2, 0, 0, 0, 10,
            ],
        );
        msg.sign(0);
        msg.sign(1);

        let coordinate_request = CoordinateRequest::from_ring_message(msg.clone())?;
        let request = Request::new(vec![0, 1], vec![30, 45], Point::new(2, 10));
        let expected_coordinate_request =
            CoordinateRequest::new(msg.header.id, msg.signatures, 2, vec![0, 5], request);

        assert_eq!(coordinate_request, expected_coordinate_request);
        Ok(())
    }

    #[test]
    fn test_4_coordinate_request_result_from_ring_message() -> Result<(), TokenRingError> {
        let msg_header = RingMessageHeader::new(
            MsgId::new(5, 7),
            RingMessageType::CoordinatedRequestResult,
            0,
            5,
        );
        let mut msg = RingMessage::new(
            msg_header,
            vec![
                0, 10, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 30, 0, 0, 0, 45, 0, 0, 0, 2, 0, 0, 0, 10,
            ],
        );
        msg.sign(0);
        msg.sign(1);

        let coordinate_request_result = CoordinatedRequestResult::from_ring_message(msg.clone())?;
        let request = Request::new(vec![0, 1], vec![30, 45], Point::new(2, 10));
        let expected_coordinate_reques_tresult =
            CoordinatedRequestResult::new(msg.header.id, msg.signatures, 10, 2, request);

        assert_eq!(
            coordinate_request_result,
            expected_coordinate_reques_tresult
        );
        Ok(())
    }
    #[test]
    fn test_5_ring_message_from_coordinate_request() -> Result<(), TokenRingError> {
        let request = Request::new(vec![0, 1], vec![30, 45], Point::new(2, 10));
        let coordinate_request =
            CoordinateRequest::new(MsgId::new(5, 7), vec![0, 1], 2, vec![3, 5], request);
        let obtained_ring_message =
            RingMessage::from_coordinate_request(coordinate_request.clone());
        assert_eq!(
            CoordinateRequest::from_ring_message(obtained_ring_message)?,
            coordinate_request
        );
        Ok(())
    }

    #[test]
    fn test_6_ring_message_from_coordinate_request_result() -> Result<(), TokenRingError> {
        let request = Request::new(vec![0, 1], vec![30, 45], Point::new(2, 10));
        let coordinated_request_result =
            CoordinatedRequestResult::new(MsgId::from_node(5), vec![0, 1], 10, 2, request);
        let obtained_ring_message =
            RingMessage::from_coordinated_request_result(coordinated_request_result.clone());
        assert_eq!(
            CoordinatedRequestResult::from_ring_message(obtained_ring_message)?,
            coordinated_request_result
        );
        Ok(())
    }

    #[test]
    fn test_7_ring_message_from_node_connection() -> Result<(), TokenRingError> {
        let addr1 = SocketAddr::from_str("127.0.0.1:10001").unwrap();
        let addr2 = SocketAddr::from_str("127.0.0.1:10002").unwrap();
        let addr3 = SocketAddr::from_str("127.0.0.1:10003").unwrap();
        let all_node_address = vec![addr1, addr2, addr3];
        let node_connection = NodeConnection::new(
            MsgId::new(2, 0),
            vec![2],
            3,
            addr3,
            VecDeque::from([1, 2, 3]),
            all_node_address.clone(),
        );

        let mut payload = vec![0, 3];
        payload.extend(all_node_address[2].to_bytes());
        payload.extend([0, 1, 0, 2, 0, 3]);
        payload.extend(
            all_node_address
                .iter()
                .map(|addr| addr.to_bytes())
                .flatten(),
        );
        let header = RingMessageHeader::new(
            MsgId::new(2, 0),
            RingMessageType::NodeConnection,
            1,
            payload.len() as u16,
        );
        let expected_ring_msg = RingMessage {
            header,
            signatures: vec![2],
            payload,
        };
        assert_eq!(
            RingMessage::from_node_connection(node_connection),
            expected_ring_msg
        );
        Ok(())
    }

    #[test]
    fn test_8_node_connection_from_ring_message() -> Result<(), TokenRingError> {
        let addr1 = SocketAddr::from_str("127.0.0.1:10001").unwrap();
        let addr2 = SocketAddr::from_str("127.0.0.1:10002").unwrap();
        let addr3 = SocketAddr::from_str("127.0.0.1:10003").unwrap();
        let all_node_address = vec![addr1, addr2, addr3];
        let node_connection = NodeConnection::new(
            MsgId::new(2, 0),
            vec![2],
            3,
            addr3,
            VecDeque::from([1, 2, 3]),
            all_node_address.clone(),
        );
        let ring_msg = RingMessage::from_node_connection(node_connection.clone());
        assert_eq!(
            NodeConnection::from_ring_message(ring_msg).unwrap(),
            node_connection
        );

        Ok(())
    }
}
