#![allow(dead_code)]

mod config;
mod index;
mod segment;
mod store;

mod pb_log_v1 {
    tonic::include_proto!("log.v1");
}
