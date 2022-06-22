#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Record {
    #[prost(bytes="vec", tag="1")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag="2")]
    pub offset: u64,
}
