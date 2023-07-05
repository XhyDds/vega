use std::sync::Arc;

use crate::context::Context;
use crate::rdd::Rdd;
use crate::serializable_traits::{Data, SerFunc};
use crate::SerArc;

#[cfg(feature="hdrs_valid")]
mod hdfs_file_reader;
mod local_file_reader;
#[cfg(feature="hdrs_valid")]
pub use hdfs_file_reader::{HdfsReader, HdfsReaderConfig};
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O;
}
