
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
    pub fn is_from_node(&self, node_id:u16)-> bool{
        self.id.is_from_node(node_id)
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct MsgId{
    pub node_id: u16,
    pub seq_num: u8,
}

impl MsgId{
    pub fn is_from_node(&self, node_id:u16)-> bool{
        node_id == self.node_id
    }
}

#[derive(Clone)]
pub struct RingMessage{
    pub header: RingMessageHeader,
    signatures: Vec<u16>,
    payload: Vec<u8>,
}

impl RingMessages for RingMessage{
    fn to_bytes(&self)-> Vec<u8> {
        todo!()
    }

    fn from_bytes(&self) -> Self {
        todo!()
    }
}