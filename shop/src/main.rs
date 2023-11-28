use actix::{Actor, Addr, System};
use shop::delivery::DeliveryHandler;
use shop::ecomerce::Ecommerce;
use shop::messages::EcommerceAddrMsg;
use shop::request_simulator::Simulator;
use shop::terminal_commands::*;
use shop::token_ring::TokenRingNode;
use shop::{point::Point, shop::Shop};
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::mpsc;

const PORT: u16 = 10000;


fn procces_args() -> Option<(u16, SocketAddr, Option<SocketAddr>)> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 && args.len() != 3 {
        return None;
    }
    let n: u16 = args[1].parse().ok()?;
    let node_addr = SocketAddr::from((IpAddr::from_str("127.0.0.1").ok()?, PORT + n));
    let mut ring_node_addr = None;
    if args.len() == 3 {
        ring_node_addr = Some(SocketAddr::from_str(&args[2]).ok()?);
    }
    Some((n, node_addr, ring_node_addr))
}

async fn load_ecommerce_addr(
    shop_addr: Addr<Shop>,
    ecommerce_addr: Addr<Ecommerce>,
) -> Result<(), Error> {
    shop_addr
        .send(EcommerceAddrMsg::new(ecommerce_addr))
        .await
        .map_err(|_| Error::ErrorInitializingEcommerce)
}

async fn initialize(
    n: u16,
    node_addr: SocketAddr,
    ring_node_addr: Option<SocketAddr>,
) -> Result<(TokenRingNode, DeliveryHandler, Addr<Shop>, Point), Error> {
    let stock_file_name = format!("shop_stocks/shop_stock_{}.txt", n);

    let (tx_delivery, rx_delivery) = mpsc::channel();
    let (tx_ring_messages, rx_ring_messages) = mpsc::channel();
    let shop = Shop::from(stock_file_name.to_string(), tx_delivery.clone()).map_err(Error::Shop)?;
    let (shop_location, shop_addr) = (shop.location, shop.start());

    let ecommerce_addr: Addr<Ecommerce> =
        Ecommerce::new(shop_addr.clone(), tx_ring_messages.clone()).start();
    load_ecommerce_addr(shop_addr.clone(), ecommerce_addr.clone()).await?;

    let delivery_handler =
        DeliveryHandler::new(shop_addr.clone(), rx_delivery, tx_delivery, shop_location);

    let tr = TokenRingNode::new(
        node_addr,
        ring_node_addr,
        ecommerce_addr,
        tx_ring_messages,
        rx_ring_messages,
    )
    .map_err(Error::TokenRing)?;

    Ok((tr, delivery_handler, shop_addr, shop_location))
}

fn finish(
    tr: TokenRingNode,
    simulator: Option<Simulator>,
    delivery_handler: DeliveryHandler,
) -> Result<(), Error> {
    let mut res = Ok(());

    if tr.stop().is_err() {
        res = Err(Error::ErrorJoiningThreads);
    }
    if delivery_handler.stop().is_err() {
        res = Err(Error::ErrorJoiningThreads);
    };
    if let Some(simulator) = simulator {
        if simulator.stop().is_err() {
            res = Err(Error::ErrorJoiningThreads);
        };
    }
    System::current().stop();
    res
}

#[actix_rt::main]
async fn main() {
    let (n, node_addr, ring_node_addr) = match procces_args() {
        Some(args) => args,
        None => return print("Invalid args".to_string()),
    };

    let (tr, delivery_handler, shop_addr, shop_location) =
        match initialize(n, node_addr, ring_node_addr).await {
            Ok(handles) => handles,
            Err(error) => {
                return print(format!(
                    "An error ocurred initializing the node: {:?}",
                    error
                ))
            }
        };

    let simulators = match receive_commands(&tr, n, shop_addr, shop_location, node_addr).await {
        Ok(simulators) => simulators,
        Err(error) => return print(format!("Error handling command: {:?}", error)),
    };

    match finish(tr, simulators, delivery_handler) {
        Ok(_) => (),
        Err(error) => print(format!(
            "An error ocurred terminating the program: {:?}",
            error
        )),
    };
}
