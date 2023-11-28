use actix::Addr;
use colored::*;
use crate::request_simulator::Simulator;
use crate::shop::ShopError;
use crate::token_ring::{TokenRingNode, TokenRingError};
use crate::{point::Point, shop::Shop};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::io::{self, AsyncBufReadExt, BufReader};

#[derive(Debug)]
pub enum Error {
    Shop(ShopError),
    TokenRing(TokenRingError),
    ErrorInitializingEcommerce,
    ErrorJoiningThreads,
}

fn handle_connect_command(mut words: VecDeque<&str>, tr: &TokenRingNode) -> Result<(), Error> {
    let res = match words.pop_front() {
        Some(second_word) => match SocketAddr::from_str(second_word) {
            Ok(addr) => tr.connect(Some(addr)),
            Err(_) => {
                print("Invalid addr, not connecting".to_string());
                Ok(())
            }
        },
        None => tr.connect(None),
    };
    res.map_err(Error::TokenRing)
}

fn start_simulator(
    simulator: Option<Simulator>,
    n: u16,
    shop_addr: &Addr<Shop>,
    shop_location: Point,
    node_addr: SocketAddr,
) -> Simulator {
    match simulator {
        Some(simulator) => {
            if simulator.is_finished() {
                if simulator.stop().is_err() {
                    print("Error joining simulator".to_string())
                };
                start_simulator(None, n, shop_addr, shop_location, node_addr)
            } else {
                print("Simulator already running".to_string());
                simulator
            }
        }
        None => {
            let requests_path = format!("simulation_files/simulation_{}.txt", n);
            Simulator::new(
                requests_path,
                n,
                shop_addr.clone(),
                shop_location,
                node_addr,
            )
        }
    }
}

fn print_commands(){
    print("Commands:
    - exit           : Finish Execution
    - simulate       : Start simulation if one is not already running
    - topology       : Display topology
    - disconnect     : Disconnect from topology
    - connect        : Connect in a new ring
    - connect <addr> : Attempt to connect to a ring through <addr>
    - help           : Display this message
    ".to_string())
}

pub async fn receive_commands(
    tr: &TokenRingNode,
    n: u16,
    shop_addr: Addr<Shop>,
    shop_location: Point,
    node_addr: SocketAddr,
) -> Result<Option<Simulator>, Error> {
    let stdin = io::stdin();
    let mut simulator = None;
    let mut lines = BufReader::new(stdin).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut words: VecDeque<&str> = line.trim().split(' ').collect();
        if let Some(first_word) = words.pop_front() {
            match first_word {
                "exit" => break,
                "help" => print_commands(),
                "simulate" => {
                    simulator = Some(start_simulator(
                        simulator,
                        n,
                        &shop_addr,
                        shop_location,
                        node_addr,
                    ))
                }
                "disconnect" => tr.disconnect().map_err(Error::TokenRing)?,
                "connect" => handle_connect_command(words, tr)?,
                "topology" => tr.print_topology().map_err(Error::TokenRing)?,
                _ => print("Invalid command. Type \"help\" for commands".to_string()),
            };
        }
    }
    Ok(simulator)
}

pub fn print(string: String) {
    println!("{} {}", "[Main]".bright_blue(), string);
}