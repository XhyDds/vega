use std::sync::Arc;

use crate::context::Context;
use crate::rdd::Rdd;
use crate::serializable_traits::{Data, SerFunc};
use crate::SerArc;

mod local_file_reader;
mod hdfs_file_reader;
mod hdfs_file_writer;
//#[cfg(any(hdrs))]
pub use hdfs_file_reader::{HdfsReader, HdfsReaderConfig};
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};
pub use hdfs_file_writer::HdfsIO;

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O;
}
