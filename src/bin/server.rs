#![feature(impl_trait_in_assoc_type)]

use std::{net::SocketAddr, sync::Mutex, collections::HashMap};

use volo_mini_redis::S;

#[volo::main]
async fn main() {
    let addr: SocketAddr = "[::]:8080".parse().unwrap();
    let addr = volo::net::Address::from(addr);

    volo_gen::mini::redis::RedisServiceServer::new(S {
        map: Mutex::new(HashMap::<String, String>::new()),
    })
        .run(addr)
        .await
        .unwrap();
}
