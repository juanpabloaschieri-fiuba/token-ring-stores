use crate::messages::*;
use crate::{ecomerce::Ecommerce, point::*};
use actix::{Actor, Addr, Context, Handler};
use colored::*;
use std::sync::mpsc::Sender;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

// Positions of the coordinates in the header of the file
const X_POS: usize = 0;
const Y_POS: usize = 1;

// Positions of the data in the body of the file
const ID_POS: usize = 1;
const AMOUNT_POS: usize = 2;

#[derive(Debug)]
pub struct Shop {
    stock: HashMap<u32, (u32, u32)>,
    pub location: Point,
    ecomerce_addr: Option<Addr<Ecommerce>>,
    delivery_sender: Sender<Option<Request>>,
}

impl Actor for Shop {
    type Context = Context<Self>;
}

impl Shop {
    /// Private constructor for Shop
    pub fn new(
        stock: HashMap<u32, (u32, u32)>,
        location: Point,
        delivery_sender: Sender<Option<Request>>,
    ) -> Shop {
        Shop {
            stock,
            location,
            ecomerce_addr: None,
            delivery_sender,
        }
    }

    /// Public constructor for Shop. Receives a path to a file with the shop stock and returns a Shop.
    pub fn from(path: String, delivery_sender: Sender<Option<Request>>) -> Result<Shop, ShopError> {
        let file = File::open(path);
        let file = file.map_err(|_| ShopError::ErrorCreatingShop)?;
        let mut reader = BufReader::new(file);

        let (x, y) = obtain_coordinates(&mut reader)?;

        let point = Point::new(x, y);

        let stock = charge_stock_from_file(&mut reader)?;

        Ok(Shop::new(stock, point, delivery_sender))
    }

    /// Given a request, returns if the shop has enough stock to take the order. The request can
    /// have more than one product, so it checks if the shop has enough stock of each product.
    fn verify_enough_stock(&self, request: &Request) -> bool {
        for (index, product) in request.product_ids.iter().enumerate() {
            match self.stock.get(product) {
                Some(amount) => {
                    if request.product_amount[index] > amount.0 {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }

    /// Given a request, returns true if the request is a physical request to the shop.
    fn is_physical_request(&self, request: &Request) -> bool {
        self.location == request.destination
    }

    /// Given a request, the shop checks if it has enough stock to take the order. If it does, it
    /// reduces the stock of the shop by the amount of the products in the request. If it isn't a
    /// physical request, it reserves the stock of the requested products. Returns an error if the
    /// shop doesn't have enough stock.
    fn process_request(
        &mut self,
        request: &Request,
        physical_request: bool,
    ) -> Result<(), ShopError> {
        if self.verify_enough_stock(request) {
            for (i, product) in request.product_ids.iter().enumerate() {
                if let Some(product) = self.stock.get_mut(product) {
                    product.0 -= request.product_amount[i];
                    if !physical_request {
                        product.1 += request.product_amount[i];
                    }
                }
            }
            print(format!("Received {}", request));
            self.print_stock();
            return Ok(());
        }
        print("Not enough Stock".to_string());

        Err(ShopError::ErrorNotEnoughStock)
    }

    /// This function is called when an online order is delivered. It receives a FinishedDelivery
    /// and, if it arrived in time, it reduces the amount of the reserved products, otherwise the
    /// reserved products are added back to the shop's stock
    fn handled_finished_delivery(&mut self, arrived_delivery: FinishedDelivery) {
        print(format!("Handling finished delivery {:?}", arrived_delivery));
        let (arrived_in_time, request) = match arrived_delivery {
            FinishedDelivery::ArrivedInTime(request) => (true, request),
            FinishedDelivery::ArrivedLate(request) => (false, request),
        };

        for (i, product) in request.product_ids.iter().enumerate() {
            if let Some(product) = self.stock.get_mut(product) {
                product.1 -= request.product_amount[i];
                if !arrived_in_time {
                    product.0 += request.product_amount[i];
                }
            }
        }

        self.print_stock();
    }

    /// Given a request made to the online shop, it processes the request and sends it to the
    /// delivery system. Could return an error if the shop can't deliver the package.
    fn handle_online_request(&mut self, request: &Request) -> Result<(), ShopError> {
        self.process_request(request, false)?;
        self.delivery_sender
            .send(Some(request.clone()))
            .map_err(|_| ShopError::ErrorCannotDeliverPackage)?;

        Ok(())
    }

    fn print_stock(&self) {
        let mut string = format!(
            "{}{}\n",
            "Stock:\n|id      |reserved|in_stock|\n",
            "-".repeat(3 * 9 + 1)
        );
        for (item_id, (in_stock, reserved)) in self.stock.iter() {
            let mut item_id = format!("{}", item_id);
            item_id = format!("|{}{}|", item_id, " ".repeat(8 - item_id.len()));
            let mut reserved = format!("{}", reserved);
            reserved = format!("{}{}|", reserved, " ".repeat(8 - reserved.len()));
            let mut in_stock = format!("{}", in_stock);
            in_stock = format!("{}{}|", in_stock, " ".repeat(8 - in_stock.len()));
            string = format!("{}{}{}{}\n", string, item_id, reserved, in_stock);
        }
        print(string);
    }
}

/// Enum that represents the errors that can occur when a message is sent to a shop.
#[derive(Debug, PartialEq)]
pub enum ShopError {
    ErrorCreatingShop,
    ErrorReadingFile,
    ErrorParsingData,
    ErrorNotEnoughStock,
    ErrorCantCommunicateWithEcommerce,
    ErrorCannotDeliverPackage,
}

impl Handler<Request> for Shop {
    type Result = ();

    /// Implementation for the Request message. If the request is a physical request, it processes
    /// the request and, if it is an online request, it sends it to the delivery system.
    /// Could return an error if the shop hasn't enough stock or if it can't deliver the package.
    fn handle(&mut self, _msg: Request, _ctx: &mut Context<Self>) -> Self::Result {
        if self.is_physical_request(&_msg) {
            print("Received Physical Request".to_string());
            let res = self
                .process_request(&_msg, true)
                .map_err(MessageError::ShopError);
            if let Err(error) = res {
                print(format!("{:?}", error));
            };
        } else if let Err(_error) = self.handle_online_request(&_msg) {
            if let Some(ecommerce_addr) = &self.ecomerce_addr {
                let res = ecommerce_addr
                    .try_send(CantHandleRequestMsg::new(_msg))
                    .map_err(|_| {
                        MessageError::ShopError(ShopError::ErrorCantCommunicateWithEcommerce)
                    });
                if let Err(error) = res {
                    print(format!("{:?}", error));
                };
            }
        };
    }
}

impl Handler<FinishedDelivery> for Shop {
    type Result = ();

    /// Implementation for the FinishedDelivery message. Used to handle the delivery of an online
    /// order.
    fn handle(&mut self, msg: FinishedDelivery, _ctx: &mut Self::Context) -> Self::Result {
        self.handled_finished_delivery(msg)
    }
}

impl Handler<EcommerceAddrMsg> for Shop {
    type Result = ();

    /// Binds the address of the Ecommerce to the shop.
    fn handle(&mut self, _msg: EcommerceAddrMsg, _ctx: &mut Context<Self>) -> Self::Result {
        self.ecomerce_addr = Some(_msg.ecommerce_addr)
    }
}

impl Handler<EnoughStockMsg> for Shop {
    type Result = ();

    /// Implementation for the EnoughStock message. Utilized to check if the shop has enough stock to
    /// take an order, if it has a Ecommerce address, it sends a message to the Ecommerce with the
    /// result of the verification.
    fn handle(&mut self, _msg: EnoughStockMsg, _ctx: &mut Context<Self>) -> Self::Result {
        let stock_availability = if self.verify_enough_stock(&_msg.request) {
            StockAvailabilityMsg::EnoughStock(
                _msg.req_id,
                _msg.request.destination.calculate_distance(&self.location),
            )
        } else {
            StockAvailabilityMsg::NotEnoughStock(_msg.req_id)
        };
        if let Some(ecommerce_addr) = &self.ecomerce_addr {
            print(format!("{}", stock_availability));
            if ecommerce_addr.try_send(stock_availability).is_err() {
                print("Failed to communicate with Ecommerce".to_string()) //WIP
            }
        }
    }
}

/// Charges the stock of the shop from the shop stock file. Receives a BufReader with the file
/// and returns a HashMap with the stock: the keys being the ID of the product and the value being
/// the quantity of each.
fn charge_stock_from_file(
    reader: &mut BufReader<File>,
) -> Result<HashMap<u32, (u32, u32)>, ShopError> {
    let mut stock = HashMap::new();

    for res_line in reader.lines() {
        //cargar stock
        let line = res_line.map_err(|_| ShopError::ErrorReadingFile)?;
        let vec: Vec<&str> = line.split(',').collect();

        let id: u32 = vec[ID_POS]
            .parse()
            .map_err(|_| ShopError::ErrorParsingData)?;
        let amount: u32 = vec[AMOUNT_POS]
            .parse()
            .map_err(|_| ShopError::ErrorParsingData)?;
        stock.insert(id, (amount, 0));
    }
    Ok(stock)
}

/// Obtains the coordinates of the shop from the shop stock file, reading only the header of the file.
/// Receives a BufReader with the file and returns a tuple with the coordinates.
fn obtain_coordinates(reader: &mut BufReader<File>) -> Result<(i32, i32), ShopError> {
    let mut buf = String::new();
    reader
        .read_line(&mut buf)
        .map_err(|_| ShopError::ErrorReadingFile)?;
    buf.pop(); // remove \n
    let vec: Vec<&str> = buf.split(',').collect();
    let x: i32 = vec[X_POS]
        .parse()
        .map_err(|_| ShopError::ErrorParsingData)?;
    let y: i32 = vec[Y_POS]
        .parse()
        .map_err(|_| ShopError::ErrorParsingData)?;
    Ok((x, y))
}

fn print(string: String) {
    println!("{} {}", "[Shop]".red(), string);
}

#[cfg(test)]
mod test {
    use super::*;

    fn create_test_shop() -> Shop {
        let (tx, _rx) = std::sync::mpsc::channel();
        Shop::from("test_files/shop_test_1.txt".to_string(), tx).unwrap()
    }

    #[test]
    fn shop_test_1_new_from_file() {
        let shop = create_test_shop();
        let stock = HashMap::from([(1, (5, 0)), (2, (4, 0)), (3, (6, 0))]);
        let location = Point::new(2, 3);
        assert_eq!(shop.stock, stock);
        assert_eq!(shop.location, location);
    }

    #[test]
    fn shop_test_2_enoungh_stock() {
        let shop = create_test_shop();
        let invalid_request = Request::new(vec![1], vec![100], Point::new(0, 0));
        let valid_request = Request::new(vec![1, 2], vec![1, 1], Point::new(0, 0));
        assert!(!shop.verify_enough_stock(&invalid_request));
        assert!(shop.verify_enough_stock(&valid_request));
    }

    #[test]
    fn shop_test_3_can_reduce_stock() -> Result<(), ShopError> {
        let mut shop = create_test_shop();
        let request = Request::new(vec![1, 2, 3], vec![2, 4, 2], Point::new(0, 0));
        shop.process_request(&request, true)?;
        let remaind_of_product_1 = shop.stock[&1];
        let remaind_of_product_2 = shop.stock[&2];
        let remaind_of_product_3 = shop.stock[&3];
        assert_eq!(remaind_of_product_1, (3, 0));
        assert_eq!(remaind_of_product_2, (0, 0));
        assert_eq!(remaind_of_product_3, (4, 0));
        Ok(())
    }

    #[test]
    fn shop_test_4_can_reserve_stock() -> Result<(), ShopError> {
        let mut shop = create_test_shop();
        let request = Request::new(vec![1, 2, 3], vec![2, 4, 2], Point::new(0, 0));
        shop.process_request(&request, false)?;
        let remaind_of_product_1 = shop.stock[&1];
        let remaind_of_product_2 = shop.stock[&2];
        let remaind_of_product_3 = shop.stock[&3];
        assert_eq!(remaind_of_product_1, (3, 2));
        assert_eq!(remaind_of_product_2, (0, 4));
        assert_eq!(remaind_of_product_3, (4, 2));
        Ok(())
    }

    #[test]
    fn shop_test_5_can_not_reserve_stock() -> Result<(), ShopError> {
        let mut shop = create_test_shop();
        let request = Request::new(vec![1, 2, 3], vec![45, 1000, 2], Point::new(0, 0));

        assert_eq!(
            Err(ShopError::ErrorNotEnoughStock),
            shop.process_request(&request, false)
        );
        let remaind_of_product_1 = shop.stock[&1];
        let remaind_of_product_2 = shop.stock[&2];
        let remaind_of_product_3 = shop.stock[&3];
        assert_eq!(remaind_of_product_1, (5, 0));
        assert_eq!(remaind_of_product_2, (4, 0));
        assert_eq!(remaind_of_product_3, (6, 0));
        Ok(())
    }

    #[test]
    fn shop_test_6_sends_requests_to_deliver() -> Result<(), ShopError> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut shop = Shop::from("test_files/shop_test_1.txt".to_string(), tx).unwrap();
        let request = Request::new(vec![1, 2, 3], vec![2, 4, 2], Point::new(0, 0));
        shop.handle_online_request(&request)?;
        assert_eq!(request, rx.recv().unwrap().unwrap());
        Ok(())
    }
}
