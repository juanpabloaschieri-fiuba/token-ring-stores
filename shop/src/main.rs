use actix_rt::time::sleep;
use rand::{SeedableRng, Rng};
use shop::delivery::deliver;
use shop::ecomerce::Ecommerce;
use shop::request_simulator::{Simulator, simulate};
use shop::token_ring::TokenRing;
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

const SEED : u64 = 43;
const PORT : u16 = 10000;

async fn load_ecommerce_addr(shop_addr: Addr<Shop>,ecommerce_addr: Addr<Ecommerce>)->Result<(), MailboxError>{
    shop_addr.send(EcommerceAddrMsg::new(ecommerce_addr)).await
}

fn procces_args()->Option<(u16, SocketAddr, Option<SocketAddr>)>{
    let args: Vec<String> = std::env::args().collect();
    //let args = vec!["nombre", "1", "127.0.0.1:10000"];
    //let args = vec!["nombre", "0"];
    print!("{:?}", args);
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

#[actix_rt::main]
async fn main() {
    let (n, node_addr, ring_node_addr) = match procces_args(){
        Some(args) => args,
        None => return println!("Invalid amount of args"),
    };
    let stock_file_name = format!("shop_stocks/shop_stock_{}.txt", n);

    let (tx_delivery,rx_delivery) = mpsc::channel();
    let (tx_ring_messages,rx_ring_messages) = mpsc::channel();
    let (shop_location ,shop_addr) = match Shop::from(stock_file_name.to_string(), tx_delivery){
        Ok(shop) => (shop.location, shop.start()),
        Err(error) => return println!("{:?}", error),  
    };
    let ecommerce_addr: Addr<Ecommerce> = Ecommerce::new(shop_addr.clone(), tx_ring_messages.clone()).start();
    if load_ecommerce_addr(shop_addr.clone(),ecommerce_addr.clone()).await.is_err(){
        return println!("tengo miedo");   //WIP
    };
    let deliveries_shop_addr = shop_addr.clone();
    let delivery_handler = thread::spawn(move|| deliver(deliveries_shop_addr, rx_delivery, shop_location));
    
    let tr = match TokenRing::new(node_addr, ring_node_addr, ecommerce_addr, tx_ring_messages, rx_ring_messages){
        Ok(tr_res) => tr_res,
        Err(error) => return println!("Failed to create Token_ring: {:?}", error),
    };

    let mut simulator = None;
    if n == 1{
        simulator = Some(Simulator::new("test_files/physical_requests_test_2.txt".to_string(), n, shop_addr, shop_location, node_addr));
    }

    //actix_rt::time::sleep(Duration::from_secs(1000)).await;
    
    let stdin = io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    println!("llegue aca");
    while let Ok(Some(line)) = lines.   next_line().await{
        println!("leyendo linea {line}");
        let mut words: VecDeque<&str> = line.trim().split(" ").collect();
        if let Some(first_word) = words.pop_front(){
            let res = match first_word {
                "exit" => todo!(),
                "disconnect" => tr.disconnect(),
                "connect" => {
                    match words.pop_front(){
                        Some(second_word) => {
                            match SocketAddr::from_str(second_word){
                                Ok(addr) => tr.connect(Some(addr)),
                                Err(_) => {
                                    println!("Invalid addr");
                                    continue;
                                }
                            }
                        },
                        None => tr.connect(None),
                    }
                    
                },
                _ => continue,
            };
            if let Err(error) = res{
                println!("{:?}", error)
            }
        }
    }
    //thread::sleep(Duration::from_secs(1000));
    System::current().stop();
    if let Some(simulator) = simulator{
        simulator.stop().unwrap();
    }
    
}
