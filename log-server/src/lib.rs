#![allow(dead_code)]

mod config;
mod index;
mod segment;
mod store;

mod pb {
    pub mod log {
        pub mod v1 {
            tonic::include_proto!("log.v1");
        }
    }
}
