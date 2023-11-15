
pub trait RingMessages{
    fn to_bytes(&self)-> Vec<u8>;
    fn from_bytes(&self) -> Self;
}

pub enum RingMessageType {
    CoordinateRequest,
    RequestHandledBy,
    NewNode,
    NodeDisconnected,
    Ack,
}

struct RingMessageHeader {
    msg_id: u8,
    msg_type: RingMessageType,
    ammount_of_signatures: u16,
    payload_size: u16,
}

pub struct RingMessageSignatures{
    signatures: Vec<u8>
}