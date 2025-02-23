use std::sync::Arc;

use crate::context::Context;
use crate::rdd::Rdd;
use crate::serializable_traits::{Data, SerFunc};
use crate::SerArc;

#[cfg(feature = "hdrs_valid")]
mod hdfs_io;

mod local_file_reader;

mod decoders;

mod local_fs_io;

#[cfg(feature = "hdrs_valid")]
pub use hdfs_io::HdfsIO;

pub use decoders::Decoders;

pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};

pub use local_fs_io::LocalFsIO;

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O;
}
