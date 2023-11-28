use actix_rt::time::sleep;
use rand::{SeedableRng, Rng};
use shop::delivery::DeliveryHandler;
use shop::ecomerce::Ecommerce;
use shop::request_simulator::{Simulator, simulate};
use shop::shop::ShopError;
use shop::token_ring::{TokenRing, TokenRingError};
use shop::token_ring_messages::SetConnectionStatus;
use shop::{shop::Shop, point::Point};
use shop::messages::EcommerceAddrMsg;
use tokio::net::UdpSocket;
use std::collections::VecDeque;
use std::io::BufRead;
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use actix::{Actor, Addr,System, MailboxError};
use std::sync::mpsc;
use tokio::io::{self, BufReader, AsyncBufReadExt};
use colored::*;

const SEED : u64 = 43;
const PORT : u16 = 10000;

#[derive(Debug)]
enum Error{
    ShopError(ShopError),
    TokenRingError(TokenRingError),
    ErrorInitializingEcommerce,
    ErrorJoiningThreads
}

async fn load_ecommerce_addr(shop_addr: Addr<Shop>,ecommerce_addr: Addr<Ecommerce>)->Result<(), Error>{
    shop_addr.send(EcommerceAddrMsg::new(ecommerce_addr)).await.map_err(|_| Error::ErrorInitializingEcommerce)
}

fn procces_args()->Option<(u16, SocketAddr, Option<SocketAddr>)>{
    let args: Vec<String> = std::env::args().collect();
    if args.len() !=2 && args.len() !=3{
        return None
    }
    let n:u16 = args[1].parse().ok()?;
    let node_addr = SocketAddr::from((IpAddr::from_str("127.0.0.1").ok()?,PORT+n));
    let mut ring_node_addr = None;
    if args.len() == 3{
        ring_node_addr = Some(SocketAddr::from_str(&args[2]).ok()?);
    }
    Some((n,  node_addr, ring_node_addr))
}

async fn initialize(n: u16, node_addr: SocketAddr, ring_node_addr: Option<SocketAddr>)->Result<(TokenRing, DeliveryHandler, Addr<Shop>, Point), Error>{
    let stock_file_name = format!("shop_stocks/shop_stock_{}.txt", n);

    let (tx_delivery,rx_delivery) = mpsc::channel();
    let (tx_ring_messages,rx_ring_messages) = mpsc::channel();
    let shop = Shop::from(stock_file_name.to_string(), tx_delivery.clone()).map_err(|error| Error::ShopError(error))?;
    let (shop_location, shop_addr) = (shop.location, shop.start());
        
    let ecommerce_addr: Addr<Ecommerce> = Ecommerce::new(shop_addr.clone(), tx_ring_messages.clone()).start();
    load_ecommerce_addr(shop_addr.clone(),ecommerce_addr.clone()).await?;
    
    let delivery_handler = DeliveryHandler::new(shop_addr.clone(), rx_delivery, tx_delivery, shop_location);
    
    let tr = TokenRing::new(node_addr, ring_node_addr, ecommerce_addr, tx_ring_messages, rx_ring_messages).map_err(|error| Error::TokenRingError(error))?;

    Ok((tr, delivery_handler, shop_addr, shop_location))
}

fn handle_connect_command(mut words: VecDeque<&str>, tr: &TokenRing)->Result<(), Error>{
    let res = match words.pop_front(){
        Some(second_word) => {
            match SocketAddr::from_str(second_word){
                Ok(addr) => tr.connect(Some(addr)),
                Err(_) => {
                    Ok(print("Invalid addr, not connecting".to_string()))
                }
            }
        },
        None => tr.connect(None),
    };
    res.map_err(|error| Error::TokenRingError(error))
}

fn start_simulator(simulator: Option<Simulator> ,n: u16, shop_addr: &Addr<Shop>, shop_location:Point, node_addr: SocketAddr)->Simulator{
    match simulator{
        Some(simulator) => if simulator.is_finished(){
                if simulator.stop().is_err(){
                    print("Error joining simulator".to_string())
                };
                start_simulator(None, n, shop_addr, shop_location, node_addr)
            } else {
                print("Simulator already running".to_string());
                simulator
            },
        None => {
            let requests_path = format!("test_files/requests_test_{}.txt", n);
            Simulator::new(requests_path, n, shop_addr.clone(), shop_location, node_addr)
        },
    }        
}

async fn receive_commands(tr: &TokenRing, n:u16, shop_addr: Addr<Shop>, shop_location:Point, node_addr: SocketAddr)->Result<Option<Simulator>, Error>{
    let stdin = io::stdin();
    let mut simulator = None;
    let mut lines = BufReader::new(stdin).lines();
    while let Ok(Some(line)) = lines.next_line().await{
        let mut words: VecDeque<&str> = line.trim().split(" ").collect();
        if let Some(first_word) = words.pop_front(){
            let res = match first_word {
                "exit" => break,
                "simulate" => simulator = Some(start_simulator(simulator, n, &shop_addr, shop_location, node_addr)),
                "topology" => tr.print_topology().map_err(Error::TokenRingError)?,
                "disconnect" => tr.disconnect().map_err(|error| Error::TokenRingError(error))?,
                "connect" => handle_connect_command(words, &tr)?,
                _ => continue,
            };
        }
    };
    Ok(simulator)
}

fn finish(tr: TokenRing, simulator: Option<Simulator>, delivery_handler: DeliveryHandler)-> Result<(), Error>{
    let mut res = Ok(());

    if tr.stop().is_err(){
        res = Err(Error::ErrorJoiningThreads);
    }
    if delivery_handler.stop().is_err(){
        res = Err(Error::ErrorJoiningThreads);
    };
    if let Some(simulator) = simulator{
        if simulator.stop().is_err(){
            res = Err(Error::ErrorJoiningThreads);
        };
    }   
    System::current().stop();
    res
}


fn print(string: String){
    println!("{} {}","[Main]".bright_blue(), string);
}

#[actix_rt::main]
async fn main(){
    let (n, node_addr, ring_node_addr) = match procces_args(){
        Some(args) => args,
        None => return print("Invalid args".to_string()),
    };

    let (tr, delivery_handler,shop_addr,shop_location) = match initialize(n, node_addr, ring_node_addr).await{
        Ok(handles) => handles,
        Err(error) => return print(format!("An error ocurred initializing the node: {:?}",error)),
    };
    
    let simulators = match receive_commands(&tr,n,shop_addr,shop_location,node_addr).await{
        Ok(simulators) => simulators,
        Err(error) =>  return print(format!("Error handling command: {:?}",error)),
    };

    match finish(tr, simulators, delivery_handler){
        Ok(_) =>(),
        Err(error) => print(format!("An error ocurred terminating the program: {:?}",error)),
    };
}
