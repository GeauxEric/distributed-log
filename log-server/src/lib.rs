mod log;
mod store;

pub mod proto {
    tonic::include_proto!("log.v1");
}
