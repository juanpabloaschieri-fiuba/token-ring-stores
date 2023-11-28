use std::fs::File;
use std::io::{BufReader, BufRead};
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;
use std::thread;
use std::thread::JoinHandle;
use rand::Rng;
use std::net::UdpSocket;
use std::time::Duration;
use actix::prelude::{Addr};
use colored::*;
use crate::shop::{*, self};
use crate::messages::*;
use crate::point::*;
use crate::token_ring_messages::{CoordinateRequest, MsgId, RingMessage, ByteConvertable};
use std::sync::mpsc::{Sender, Receiver, self};
///13 //tiempo hasta pedido
///ida,5;idb7,8;
/// 
const PHYSICAL_REQUEST_RATIO: f32 = 0.0;
const PORT:u16 = 20000;

/// Struct that represents a physical request
#[derive(Debug,PartialEq)]
struct SimulatedRequest{
    delay: Duration,
    request: Request,
}

impl SimulatedRequest{

    /// Creates a new PhysicalRequest
    fn new(delay: Duration, product_ids: Vec<u32>, product_amount: Vec<u32>, shop_location: Point)-> SimulatedRequest{
        SimulatedRequest{delay, request: Request::new(product_ids, product_amount, shop_location)}
    }

    fn set_physical_request_randomly(&mut self)->bool{
        let shop_location = self.request.destination;
        if !(rand::thread_rng().gen_range(0.0, 1.0) < PHYSICAL_REQUEST_RATIO){
            self.request.destination = Point::new(rand::thread_rng().gen_range(-15, 15), rand::thread_rng().gen_range(-15, 15));
            while shop_location == self.request.destination{
                self.request.destination = Point::new(rand::thread_rng().gen_range(-15, 15), rand::thread_rng().gen_range(-15, 15));
            }
            return false;
        }
        true
    }

    fn handle(mut self, shop_addr: &Addr<Shop>, socket: &UdpSocket, ecommerce_socket_addr: &SocketAddr, finish_receiver: &Receiver<bool>)->Result<(),SimulationError>{
        if let Ok(_) = finish_receiver.recv_timeout(self.delay){
            return Err(SimulationError::EndOfSimulation)
        }
        if self.set_physical_request_randomly(){
            print(format!("Generating a new phisical request "));
            shop_addr.try_send(self.request).map_err(|_| SimulationError::ErrorInCommunications)?;
        } else {
            print(format!("Generating a new online request "));
            let c_request = CoordinateRequest::new(MsgId::default(), vec![], self.request.amount_of_diferent_products(), vec![], self.request);
            let bytes = RingMessage::from_coordinate_request(c_request).to_bytes();
            socket.send_to(&bytes, ecommerce_socket_addr).map_err(|_| SimulationError::ErrorInCommunications)?;
        }
        Ok(())
    }
}

/// Enum that represents the errors that can occur when generating physical requests
#[derive(Debug)]
pub enum SimulationError {
    FileError(FileError),
    ErrorParsingData,
    ErrorInCommunications,
    ErrorSettingAddr,
    EndOfSimulation,
    ErrorJoiningSimulationThread
}

pub struct Simulator{
    handle: JoinHandle<Result<(), SimulationError>>,
    finish_sender: Sender<bool>
}

impl Simulator{
    pub fn new(path:String, n: u16,shop_addr: Addr<Shop>, shop_location: Point, ecommerce_socket_addr: SocketAddr,)-> Simulator{
        let (finish_sender, finish_receiver) = mpsc::channel();
        let handle = thread::spawn(move || simulate(path, n, shop_addr, shop_location, ecommerce_socket_addr, finish_receiver));
        print("Creating new simulator".to_string());
        Simulator{handle, finish_sender}
    }

    pub fn stop(self) -> Result<(), SimulationError>{
        let _ = self.finish_sender.send(true).map_err(|_| SimulationError::ErrorInCommunications);
        let res = self.handle.join().map_err(|_| SimulationError::ErrorJoiningSimulationThread)?;
        print(format!("Simulator finished: {:?}", res));
        Ok(())
    }

    pub fn is_finished(&self) -> bool{
        self.handle.is_finished()
    }
}

pub fn simulate(path:String, n: u16,shop_addr: Addr<Shop>, shop_location: Point, ecommerce_socket_addr: SocketAddr, finish_receiver: Receiver<bool>) -> Result<(), SimulationError> {
    let orders = File::open(path).map_err(|_| SimulationError::FileError(FileError::ErrorOpeningFile))?;
    let ip = IpAddr::from_str("127.0.0.2").map_err(|_| SimulationError::ErrorSettingAddr)?;
    let simulation_addr = SocketAddr::new(ip, PORT + n);
    let socket = UdpSocket::bind(simulation_addr).map_err(|_| SimulationError::ErrorInCommunications)?;
    let mut reader = BufReader::new(orders);
    loop {
        match read_request(&mut reader, shop_location){
            Ok(s_request) => {
                if let Err(error) = s_request.handle(&shop_addr, &socket, &ecommerce_socket_addr, &finish_receiver){
                    match error{
                        SimulationError::EndOfSimulation => return Ok(()),
                        _ => return Err(error),
                    }
                }
            },
            Err(error) => {
                if let SimulationError::FileError(FileError::ErrorEof) = error{
                    return Ok(())
                }
                return Err(error)
            }
        }
    }
}

/// Reads the delay part of the physical request from the file
fn read_delay(reader: &mut BufReader<File>) -> Result<Duration, SimulationError> {
    let delay = read_line(reader).map_err(|file_error| SimulationError::FileError(file_error))?;
    let delay: u32 = delay.parse().map_err(|_| SimulationError::ErrorParsingData)?;
    Ok(Duration::from_secs(delay as u64))
}

/// Reads the order part of the physical request from the file
fn read_order(reader: &mut BufReader<File>) -> Result<(Vec<u32>, Vec<u32>), SimulationError> {
    let mut request_products: Vec<u32> = Vec::new();
    let mut request_amounts: Vec<u32> = Vec::new();

    let buf = read_line(reader).map_err(|file_error| SimulationError::FileError(file_error))?;
    let products: Vec<&str> = buf.split(';').collect();
    
    for product in products{
        let product: Vec<&str> = product.split(',').collect();
        let id: u32 = product[0].parse().map_err(|_| SimulationError::ErrorParsingData)?;
        let amount: u32 = product[1].parse().map_err(|_| SimulationError::ErrorParsingData)?;
        request_products.push(id);
        request_amounts.push(amount);
    }
    Ok((request_products,request_amounts))
}

/// Reads the physical request from the file
fn read_request(reader: &mut BufReader<File>,shop_location: Point) -> Result<SimulatedRequest,SimulationError>{
    let delay = read_delay(reader)?;
    let (request_products, request_amounts) = read_order(reader)?;
    
    Ok(SimulatedRequest::new(delay,request_products,request_amounts, shop_location))
}

/// Enum that represents the errors that can occur when reading from a file
#[derive(Debug)]
pub enum FileError{
    ErrorReadingLine,
    ErrorOpeningFile,
    ErrorEof
}

/// Reads a line from a file
fn read_line(reader: &mut BufReader<File>)->Result<String, FileError>{
    let mut buf = String::new();
    if reader.read_line(&mut buf).map_err(|_| FileError::ErrorReadingLine)? == 0{
        return Err(FileError::ErrorEof);
    };
    if buf.ends_with('\n'){
        buf.pop();
    }
    Ok(buf)
}

fn print(string: String){
    println!("{} {}","[SIMULATOR]".green(), string);
}

#[cfg(test)]

mod test {
    use super::*;

    fn get_requests()-> (SimulatedRequest, SimulatedRequest){
        let p1 = SimulatedRequest::new(Duration::from_secs(2), Vec::from([1,2]), Vec::from([2,1]), Point::new(0,0));
        let p2 = SimulatedRequest::new(Duration::from_secs(3), Vec::from([1,3]), Vec::from([1,1]), Point::new(0, 0));
        (p1,p2)
    }

    #[tokio::test]
    async fn test_1_read_request(){
        let path = String::from("test_files/requests_test_1.txt");
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);
        let request1 = read_request(&mut reader, Point::new(0,0)).unwrap();
        let request2 = read_request(&mut reader, Point::new(0,0)).unwrap();
        let (p1, p2) = get_requests();
        assert_eq!(request1, p1);
        assert_eq!(request2, p2);
    }
}