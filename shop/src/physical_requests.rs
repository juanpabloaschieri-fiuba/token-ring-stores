use std::thread;
use std::thread::JoinHandle;
use tokio::fs::File;
use tokio::io::{BufReader,AsyncBufReadExt};
use std::time::Duration;
use actix::prelude::*;
use crate::shop::*;
///13 //tiempo hasta pedido
///ida,5;idb7,8;
/// 

#[derive(Debug)]
pub enum PhysicalRequestError {
    FileError(FileError),
    ErrorParsingData
}

pub async fn simulate_physical_requests(path:String, shop_addr: Addr<Shop>)-> Result<(), PhysicalRequestError>{
    let orders = File::open(path).await.map_err(|_| PhysicalRequestError::FileError(FileError::ErrorOpeningFile))?;
    let mut reader = BufReader::new(orders);
    loop {
        match read_request(&mut reader).await {
            Ok(request) => {
                print!("WIP: Voy a dormir por: {:?}",request.delay);
                tokio::time::sleep(request.delay).await;
                let res = shop_addr.send(request).await;
                print!("WIP: El resultado fue: {:?}", res);
            },
            Err(error) => {
                if let PhysicalRequestError::FileError(FileError::ErrorEof) = error{
                    return Ok(())
                }
                return Err(error)
            }
        }
    }
}

async fn read_delay(reader: &mut BufReader<File>) -> Result<Duration, PhysicalRequestError> {
    let delay = read_line(reader).await.map_err(|file_error| PhysicalRequestError::FileError(file_error))?;
    let delay: u32 = delay.parse().map_err(|_| PhysicalRequestError::ErrorParsingData)?;
    Ok(Duration::from_secs(delay as u64))
}


async fn read_order(reader: &mut BufReader<File>) -> Result<(Vec<u32>, Vec<u32>), PhysicalRequestError> {
    let mut request_products: Vec<u32> = Vec::new();
    let mut request_amounts: Vec<u32> = Vec::new();

    let buf = read_line(reader).await.map_err(|file_error| PhysicalRequestError::FileError(file_error))?;
    let products: Vec<&str> = buf.split(';').collect();
    
    for product in products{
        let product: Vec<&str> = product.split(',').collect();
        let id: u32 = product[0].parse().map_err(|_| PhysicalRequestError::ErrorParsingData)?;
        let amount: u32 = product[1].parse().map_err(|_| PhysicalRequestError::ErrorParsingData)?;
        request_products.push(id);
        request_amounts.push(amount);
    }
    Ok((request_products,request_amounts))
}

async fn read_request(reader: &mut BufReader<File>) -> Result<PhysicalRequest,PhysicalRequestError>{
    println!("hola");
    let delay = read_delay(reader).await?;
    println!("hola2");
    let (request_products, request_amounts) = read_order(reader).await?;
    println!("hola3");
    Ok(PhysicalRequest::new(delay,request_products,request_amounts))
}

#[derive(Debug)]
pub enum FileError{
    ErrorReadingLine,
    ErrorOpeningFile,
    ErrorEof
}

async fn read_line(reader: &mut BufReader<File>)->Result<String, FileError>{
    let mut buf = String::new();
    if reader.read_line(&mut buf).await.map_err(|_| FileError::ErrorReadingLine)? == 0{
        return Err(FileError::ErrorEof);
    };
    if buf.ends_with('\n'){
        buf.pop();
    }
    Ok(buf)
}

#[cfg(test)]

mod test {
    use super::*;

    fn get_physical_requests_test_1_requests()-> (PhysicalRequest, PhysicalRequest){
        let p1 = PhysicalRequest::new(Duration::from_secs(2), Vec::from([1,2]), Vec::from([2,1]));
        let p2 = PhysicalRequest::new(Duration::from_secs(3), Vec::from([1,3]), Vec::from([1,1]));
        (p1,p2)
    }

    #[tokio::test]
    async fn test_1_read_request(){
        let path = String::from("test_files/physical_requests_test_1.txt");
        let file = File::open(path).await.unwrap();
        let mut reader = BufReader::new(file);
        let request1 = read_request(&mut reader).await.unwrap();
        let request2 = read_request(&mut reader).await.unwrap();
        let (p1, p2) = get_physical_requests_test_1_requests();
        assert_eq!(request1, p1);
        assert_eq!(request2, p2);
    }
}