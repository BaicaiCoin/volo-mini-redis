#![feature(impl_trait_in_assoc_type)]

use std::{net::SocketAddr, sync::Mutex, collections::HashMap};

use volo_mini_redis::S;

#[volo::main]
async fn main() {
    let addr: SocketAddr = "[::]:8080".parse().unwrap();
    let addr = volo::net::Address::from(addr);

    let aof_path = "redis.aof".to_string();

    let s = S {
        map: Mutex::new(HashMap::<String, String>::new()),
        aof_path: aof_path.clone(),
    };

    if let Err(err) = rebuild_data_from_aof(&s) {
        tracing::error!("Failed to rebuild data from AOF file: {:?}", err);
    }

    volo_gen::mini::redis::RedisServiceServer::new(s)
        .run(addr)
        .await
        .unwrap();
}

fn rebuild_data_from_aof(s: &S) -> Result<(), std::io::Error> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let file = File::open(&s.aof_path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let operation_str = line?;
        let req = parse_redis_operation(&operation_str);
        match req.request_type {
            volo_gen::mini::redis::RequestType::Set => {
                let _ = s.map.lock().unwrap().insert(req.key.unwrap().get(0).unwrap().to_string(), req.value.unwrap().to_string(),);
            }
            volo_gen::mini::redis::RequestType::Del => {
                for i in req.key.unwrap() {
                    if let Some(_) = s.map.lock().unwrap().remove(&i.to_string()) {
                    }
                }
            }
            _ => {}
        }
    }

    std::result::Result::Ok(())
}

fn parse_redis_operation(operation_str: &String) -> volo_gen::mini::redis::RedisRequest {
    let string_vec: Vec<String> = operation_str.split(' ').map(|str| str.to_string()).collect();
    let mut req = volo_gen::mini::redis::RedisRequest {
        key: None,
        value: None,
        request_type: volo_gen::mini::redis::RequestType::Illegal,
    };
    if string_vec[0] == "SET" && string_vec.len() == 3 {
        req = volo_gen::mini::redis::RedisRequest {
            key: Some(vec![string_vec.get(1).unwrap().clone().into()]),
            value: Some(string_vec.get(2).unwrap().clone().into()),
            request_type: volo_gen::mini::redis::RequestType::Set,
        }
    }
    else if  string_vec[0] == "DEL" {
        let mut tmp = vec![];
        for i in 1..string_vec.len() {
            tmp.push(string_vec.get(i).unwrap().clone().into());
        }
        req = volo_gen::mini::redis::RedisRequest {
            key: Some(tmp),
            value: None,
            request_type: volo_gen::mini::redis::RequestType::Del,
        }
    }
    req
}
