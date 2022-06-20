#![allow(dead_code)]

mod config;
mod index;
mod log;
mod store;

pub mod proto {
    tonic::include_proto!("log.v1");
}
