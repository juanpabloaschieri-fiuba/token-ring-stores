//use std::ops::IndexMut;

pub trait RingMessages{
    fn to_bytes(&self)-> Vec<u8>;
    fn from_bytes(&self) -> Self;
}

#[derive(Clone)]
pub enum RingMessageType {
    CoordinateRequest,
    RequestHandledBy,
    NewNode,
    NodeDisconnected,
    Ack,
}

#[derive(Clone)]
pub struct RingMessageHeader {
    pub id: MsgId,
    pub msg_type: RingMessageType,
    ammount_of_signatures: u16,
    payload_size: u16,
}

impl RingMessageHeader{

    pub fn new(id: MsgId, msg_type: RingMessageType, payload_size: u16) -> Self {
        RingMessageHeader {
            id,
            msg_type,
            ammount_of_signatures: 0,
            payload_size,
        }
    }

    pub fn is_from_node(&self, node_id:u16)-> bool{
        self.id.is_from_node(node_id)
    }

    pub fn recently_created_by(&self, node_id:u16) -> bool {
        self.is_from_node(node_id) && self.ammount_of_signatures == 1
    } 
}



#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct MsgId{
    pub node_id: u16,
    pub seq_num: u8,
}

impl MsgId{
    
    pub fn new(node_id: u16, seq_num: u8) -> Self {
        MsgId {
            node_id,
            seq_num,
        }
    }
    
    pub fn is_from_node(&self, node_id:u16)-> bool{
        node_id == self.node_id
    }

    fn to_bytes(&self)->Vec<u8>{
        let mut bytes = Vec::from(self.node_id.to_be_bytes());
        bytes.push(self.seq_num);
        bytes
    }
}

impl RingMessages for RingMessageHeader{
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.id.to_bytes();
        bytes.push(self.msg_type.clone() as u8);
        bytes.extend(self.ammount_of_signatures.to_be_bytes());
        bytes.extend(self.payload_size.to_be_bytes());  
        bytes
    }

    fn from_bytes(&self) -> Self {
        todo!()
    }
}

#[derive(Clone)]
pub struct RingMessage{
    pub header: RingMessageHeader,
    signatures: Vec<u16>,
    payload: Vec<u8>,
}

impl RingMessages for RingMessage{
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.header.to_bytes();
        for signature in &self.signatures{
            bytes.extend(signature.to_be_bytes());    
        }
        bytes.extend(&self.payload);
        bytes
    }

    fn from_bytes(&self) -> Self {
        todo!()
    }
}

impl RingMessage {
    pub fn new(header: RingMessageHeader, payload: Vec<u8>) -> Self {
        RingMessage {
            header,
            signatures: Vec::new(),
            payload,
        }
    }

    pub fn new_ack(id:MsgId)-> Self{
        let header = RingMessageHeader::new(id, RingMessageType::Ack, 0);
        RingMessage { header, signatures: Vec::new(), payload: Vec::new() }
    }

    pub fn did_full_lap(&self, node_id: u16)-> bool{
        self.signatures.contains(&node_id)
    }

    pub fn sign(&mut self,node_id: u16){
        if let Some(last_signature) = self.signatures.last(){
            if *last_signature == node_id{
                return
            }
        }
        self.signatures.push(node_id);
        self.header.ammount_of_signatures +=1;
    }
}