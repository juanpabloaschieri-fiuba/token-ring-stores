# Token-ring store network

A chain of stores that share incoming online orders **without a central coordinator**. Every store runs as an independent peer on a UDP ring. When an order arrives, the ring votes, and the nearest store that has the stock ships it.

Built in Rust for *Técnicas de Programación Concurrente I* (FIUBA, University of Buenos Aires), second semester 2023.

## How it works

- **Stores and e-commerce as actors** (actix). Each store owns its stock, split into available and reserved, and its location on a plane.
- **Voting on the ring.** The store that receives an online order sends a vote around the ring. Every peer with enough stock adds its distance to the destination, and the originator then announces the winner, which reserves the stock and ships.
- **Delivery simulation.** Each store's delivery thread keeps pending orders in a min-heap by arrival time. It sleeps until the next one arrives or a new order comes in, so many deliveries run in parallel.
- **In-store orders** are served immediately from local stock. A simulator thread replays a file of timed orders.

## Reliability over UDP

- **Acks with timeouts:** every message is acknowledged by ID (node ID + sequence number). After repeated timeouts, the next peer is declared dead, removed from the topology, and the ring routes around it.
- **Sliding window:** a node can have several messages in flight, and the rest queue up.
- **Lost-message recovery:** a node resends its own message if it doesn't come back around the ring in time.
- **No zombie messages:** each peer signs messages as it forwards them, so a message whose author died stops circulating. Duplicates are discarded.
- **Dynamic membership:** nodes can join a running ring, several at a time, and leave it gracefully.

## Running it

```sh
cd shop
cargo run 0                    # start a new ring (node 0, port 10000)
cargo run 1 127.0.0.1:10000    # start node 1 and join the ring through node 0
```

Commands while running: `simulate`, `topology`, `connect [addr]`, `disconnect`, `exit`.

Design notes in Spanish are in [`shop/README.md`](shop/README.md).

## My part

I wrote 27 of the 36 commits: the store and its order handling, the delivery system, and the token ring itself (sender, timeouts, acks, disconnection handling and tests).
