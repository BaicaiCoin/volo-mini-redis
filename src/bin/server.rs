#![feature(impl_trait_in_assoc_type)]

use std::{net::SocketAddr, sync::Mutex, collections::HashMap, vec::Vec};
use config::{Config, FileFormat};
use serde::Deserialize;
use volo_mini_redis::S;
use tokio::spawn;

#[derive(Debug, Deserialize)]
struct RedisConfig {
    main_node: NodeConfig,
    slave_nodes: Vec<NodeConfig>,
}

#[derive(Debug, Deserialize)]
struct NodeConfig {
    address: SocketAddr,
    is_main: bool,
}

impl NodeConfig {
    fn is_main(&self) -> bool {
        self.is_main
    }
}

#[volo::main]
async fn main() {
    let mut settings = Config::new();
    settings.merge(config::File::new("config", FileFormat::Toml).required(true)).unwrap();
    let redis_config: RedisConfig = settings.try_into().unwrap();
    let main_node = redis_config.main_node;
    let slave_nodes = redis_config.slave_nodes;
    let mut vec = Vec::new();
    vec.push(spawn(start_redis_node(main_node)));
    for slave_node in slave_nodes {
        vec.push(spawn(start_redis_node(slave_node)));
    }
    for element in vec {
        element.await.unwrap();
    }
}

async fn start_redis_node(config: NodeConfig) {
    let addr = volo::net::Address::from(config.address);
    let aof_path = "redis.aof".to_string();

    let s = S {
        map: Mutex::new(HashMap::<String, String>::new()),
        aof_path: aof_path.clone(),
        is_main: config.is_main(),
    };

    if let Err(err) = rebuild_data_from_aof(&s) {
        tracing::error!("Failed to rebuild data from AOF file: {:?}", err);
    }

    if config.is_main() {
        println!("Main_node start! addr: {}", config.address);
    } else {
        println!("Slave_node start! addr: {}", config.address);
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
