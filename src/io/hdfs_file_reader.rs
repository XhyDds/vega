use core::panic;
use hdrs::{Client, OpenOptions};
use std::fs;
use std::io::{BufReader, Read};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::context::Context;
use crate::dependency::Dependency;
use crate::error::{Error, Result};
use crate::hosts::Hosts;
use crate::io::*;
use crate::rdd::{MapPartitionsRdd, MapperRdd, Rdd, RddBase};
use crate::serializable_traits::{AnyData, Data, SerFunc};
use crate::split::Split;
use crate::Fn;
use log::debug;
use rand::prelude::*;
use serde_derive::{Deserialize, Serialize};

pub struct HdfsReaderConfig {
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    namenode: String,
    dir_path: PathBuf,
    executor_partitions: Option<u64>,
}

impl HdfsReaderConfig {
    /// Read all the files from a directory or a path.
    pub fn new<T: Into<PathBuf>>(path: T) -> HdfsReaderConfig {
        let nn = match Hosts::get() {
            //namenode默认是master
            //Ok(hosts) => hosts.master.to_string(),
            Ok(_) => String::from("192.168.179.129:9000"),
            Err(_) => String::from("localhost:9000"),
        };

        HdfsReaderConfig {
            filter_ext: None,
            expect_dir: true,
            namenode: nn, //这里namenode的ip可能有误（不知道socketAddr转为string带不带端口号）
            dir_path: path.into(),
            executor_partitions: None,
        }
    }

    /// Only will read files with a given extension.
    pub fn filter_extension<T: Into<String>>(&mut self, extension: T) {
        self.filter_ext = Some(extension.into().into()); //?
    }

    /// Set the namenode.
    pub fn set_namenode<T: Into<String>>(&mut self, namenode: T) {
        //这里之后可能得做一下保护，只能设置一次namenode
        self.namenode = namenode.into();
    }

    /// Default behaviour is to expect the directory to exist in every node,
    /// if it doesn't the executor will panic.
    pub fn expect_directory(mut self, should_exist: bool) -> Self {
        self.expect_dir = should_exist;
        self
    }

    /// Number of partitions to use per executor to perform the load tasks.
    /// One executor must be used per host with as many partitions as CPUs available (ideally).
    pub fn num_partitions_per_executor(mut self, num: u64) -> Self {
        self.executor_partitions = Some(num);
        self
    }
}

impl ReaderConfiguration<Vec<u8>> for HdfsReaderConfig {
    fn make_reader<F, U>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Vec<u8>) -> U,
        U: Data,
    {
        let reader = HdfsReader::<BytesReader>::new(self, context);
        let read_files = Fn!(
            |_part: usize, readers: Box<dyn Iterator<Item = BytesReader>>| {
                Box::new(readers.into_iter().map(|file| file.into_iter()).flatten())
                    as Box<dyn Iterator<Item = _>>
            }
        );
        let files_per_executor = Arc::new(
            MapPartitionsRdd::new(Arc::new(reader) as Arc<dyn Rdd<Item = _>>, read_files).pin(),
        );
        let decoder = MapperRdd::new(files_per_executor, decoder).pin();
        decoder.register_op_name("hdfs_reader<bytes>");
        SerArc::new(decoder)
    }
}

impl ReaderConfiguration<PathBuf> for HdfsReaderConfig {
    fn make_reader<F, U>(self, context: Arc<Context>, decoder: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(PathBuf) -> U,
        U: Data,
    {
        let reader = HdfsReader::<FileReader>::new(self, context);
        let read_files = Fn!(
            |_part: usize, readers: Box<dyn Iterator<Item = FileReader>>| {
                Box::new(readers.map(|reader| reader.into_iter()).flatten())
                    as Box<dyn Iterator<Item = _>>
            }
        );
        let files_per_executor = Arc::new(
            MapPartitionsRdd::new(Arc::new(reader) as Arc<dyn Rdd<Item = _>>, read_files).pin(),
        );
        let decoder = MapperRdd::new(files_per_executor, decoder).pin();
        decoder.register_op_name("hdfs_reader<files>");
        SerArc::new(decoder)
    }
}

/// Reads all files specified in a given directory from the local directory
/// on all executors on every worker node.
#[derive(Clone, Serialize, Deserialize)]
pub struct HdfsReader<T> {
    id: usize,
    namenode: String,
    path: PathBuf,
    is_single_file: bool,
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    executor_partitions: Option<u64>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Arc<Context>,
    // explicitly copy the address map as the map under context is not
    // deserialized in tasks and this is required:
    splits: Vec<SocketAddrV4>,
    _marker_reader_data: PhantomData<T>,
}

impl<T: Data> HdfsReader<T> {
    fn new(config: HdfsReaderConfig, context: Arc<Context>) -> Self {
        let HdfsReaderConfig {
            dir_path,
            expect_dir,
            namenode,
            filter_ext,
            executor_partitions,
        } = config;

        let is_single_file = {
            let path: &Path = dir_path.as_ref();
            let path = path.to_str().unwrap();
            let client = Client::connect(namenode.as_str());
            match client {
                Ok(client) => match client.metadata(path) {
                    Ok(metadata) => metadata.is_file(),
                    Err(e) => {
                        panic!("Failed to read metadata from HDFS: {}", e);
                    }
                },
                Err(e) => {
                    panic!("Failed to read metadata from HDFS: {}", e);
                }
            }
        };

        HdfsReader {
            id: context.new_rdd_id(),
            namenode,
            path: dir_path,
            is_single_file,
            filter_ext,
            expect_dir,
            executor_partitions,
            splits: context.address_map.clone(),
            context,
            _marker_reader_data: PhantomData,
        }
    }

    /// This function should be called once per host to come with the paralel workload.
    /// Is safe to recompute on failure though.
    fn load_hdfs_files(&self) -> Result<Vec<Vec<PathBuf>>> {
        let mut total_size = 0_u64;
        if self.is_single_file {
            let files = vec![vec![self.path.clone()]]; //按分区存，每个分区是一个vec，每个分区的vec里面是若干文件
            return Ok(files);
        }

        let mut num_partitions = self.get_executor_partitions();
        let mut files: Vec<(u64, PathBuf)> = vec![];
        // We compute std deviation incrementally to estimate a good breakpoint
        // of size per partition.
        let mut total_files = 0_u64;
        let mut k = 0;
        let mut ex = 0.0; //期望？
        let mut ex2 = 0.0; //平方的期望？

        let fs = Client::connect(self.namenode.as_str()).unwrap();

        for (i, entry) in fs
            .read_dir(&self.path.to_str().unwrap())
            .unwrap()
            .into_inner() //处理该路径下的每一个条目
            .enumerate()
        {
            let path = PathBuf::from(entry.path());
            if entry.is_file() {
                let is_proper_file = {
                    self.filter_ext.is_none()
                        || path.extension() == self.filter_ext.as_ref().map(|s| s.as_ref())
                    //根据filter对文件进行过滤
                };
                if !is_proper_file {
                    continue;
                }
                let size = entry.len();
                if i == 0 {
                    // assign first file size as reference sample
                    k = size;
                }
                // compute the necessary statistics
                let remain = size as f32 - k as f32;
                ex += remain;
                ex2 += remain.powf(2.0);
                total_size += size;
                total_files += 1;

                files.push((size, path));
            }
        }

        if total_files == 0 {
            return Err(Error::NoFilesFound);
        }

        let file_size_mean = (total_size / total_files) as u64;
        let std_dev = ((ex2 - ex.powf(2.0) / total_files as f32) / total_files as f32).sqrt(); //这个标准差算得有点奇怪

        if total_files < num_partitions {
            // Coerce the number of partitions to the number of files
            num_partitions = total_files;
        }

        let avg_partition_size = (total_size / num_partitions) as u64;

        let partitions = self.assign_files_to_partitions(
            //将文件分配到分区
            num_partitions,
            files,
            file_size_mean,
            avg_partition_size,
            std_dev,
        );

        Ok(partitions)
    }

    /// Assign files according to total avg partition size and file size.
    /// This should return a fairly balanced total partition size.
    fn assign_files_to_partitions(
        &self,
        num_partitions: u64,
        files: Vec<(u64, PathBuf)>,
        file_size_mean: u64, //平均大小
        avg_partition_size: u64,
        std_dev: f32, //文件大小的标准差
    ) -> Vec<Vec<PathBuf>> {
        // Accept ~ 0.25 std deviations top from the average partition size
        // when assigning a file to a partition.
        let high_part_size_bound = (avg_partition_size + (std_dev * 0.25) as u64) as u64;

        debug!(
            "the average part size is {} with a high bound of {}",
            avg_partition_size, high_part_size_bound
        );
        debug!(
            "assigning files from local fs to partitions, file size mean: {}; std_dev: {}",
            file_size_mean, std_dev
        );

        let mut partitions = Vec::with_capacity(num_partitions as usize);
        let mut partition = Vec::with_capacity(0); //这样分配？
        let mut curr_part_size = 0_u64;
        let mut rng = rand::thread_rng(); //RNG牛B

        for (size, file) in files.into_iter() {
            if partitions.len() as u64 == num_partitions - 1 {
                //最后一个分区，所有剩余文件都放里面
                partition.push(file);
                continue;
            }

            let new_part_size = curr_part_size + size;
            let larger_than_mean = rng.gen::<bool>(); //随机生成？？
            if (larger_than_mean && new_part_size < high_part_size_bound)
                || (!larger_than_mean && new_part_size <= avg_partition_size)
            {
                partition.push(file);
                curr_part_size = new_part_size;
            } else if size > avg_partition_size as u64 {
                if !partition.is_empty() {
                    partitions.push(partition);
                }
                partitions.push(vec![file]);
                partition = vec![];
                curr_part_size = 0;
            } else {
                if !partition.is_empty() {
                    partitions.push(partition);
                }
                partition = vec![file];
                curr_part_size = size;
            }
        }
        if !partition.is_empty() {
            partitions.push(partition);
        }

        let mut current_pos = partitions.len() - 1;
        while (partitions.len() as u64) < num_partitions {
            // If the number of specified partitions is relativelly equal to the number of files
            // or the file size of the last files is low skew can happen and there can be fewer
            // partitions than specified. This the number of partitions is actually the specified.
            if partitions.get(current_pos).unwrap().len() > 1 {
                // Only get elements from part as long as it has more than one element
                let last_part = partitions.get_mut(current_pos).unwrap().pop().unwrap(); //从当前位置的分区中取出一个文件，放到新的分区中
                partitions.push(vec![last_part])
            } else if current_pos > 0 {
                current_pos -= 1;
            } else {
                break;
            }
        }
        partitions
    }

    fn get_executor_partitions(&self) -> u64 {
        if let Some(num) = self.executor_partitions {
            num
        } else {
            num_cpus::get() as u64
        }
    }
}

macro_rules! impl_common_lfs_rddb_funcs {
    () => {
        fn get_rdd_id(&self) -> usize {
            self.id
        }

        fn get_context(&self) -> Arc<Context> {
            self.context.clone()
        }

        fn get_dependencies(&self) -> Vec<Dependency> {
            //？
            vec![]
        }

        fn is_pinned(&self) -> bool {
            true
        }

        default fn iterator_any(
            &self,
            split: Box<dyn Split>,
        ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
            Ok(Box::new(
                self.iterator(split)?
                    .map(|x| Box::new(x) as Box<dyn AnyData>),
            ))
        }
    };
}

impl RddBase for HdfsReader<BytesReader> {
    impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        // for a given split there is only one preferred location because this is pinned,
        // the preferred location is the host at which this split will be executed;
        let split = split.downcast_ref::<BytesReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::with_capacity(self.splits.len());
        for (idx, host) in self.splits.iter().enumerate() {
            splits.push(Box::new(BytesReader {
                idx,
                host: *host.ip(),
                files: Vec::new(),
                namenode: self.namenode.clone(),
            }) as Box<dyn Split>)
        }
        splits
    }
}

impl RddBase for HdfsReader<FileReader> {
    impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        let split = split.downcast_ref::<FileReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::with_capacity(self.splits.len());
        for (idx, host) in self.splits.iter().enumerate() {
            splits.push(Box::new(FileReader {
                idx,
                host: *host.ip(),
                files: Vec::new(),
            }) as Box<dyn Split>)
        }
        splits
    }
}

macro_rules! impl_common_lfs_rdd_funcs {
    () => {
        fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>
        where
            Self: Sized,
        {
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>
        }

        fn get_rdd_base(&self) -> Arc<dyn RddBase> {
            Arc::new(self.clone()) as Arc<dyn RddBase>
        }
    };
}

impl Rdd for HdfsReader<BytesReader> {
    type Item = BytesReader;

    impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<BytesReader>().unwrap();
        let files_by_part = self.load_hdfs_files()?;
        let idx = split.idx;
        let host = split.host;
        let namenode = self.namenode.clone();
        Ok(
            Box::new(files_by_part.into_iter().map(move |files| BytesReader {
                files,
                host,
                idx,
                namenode: namenode.clone(),
            })) as Box<dyn Iterator<Item = Self::Item>>,
        )
    }
}

impl Rdd for HdfsReader<FileReader> {
    type Item = FileReader;

    impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<FileReader>().unwrap();
        let files_by_part = self.load_hdfs_files()?;
        let idx = split.idx;
        let host = split.host;
        Ok(Box::new(
            files_by_part
                .into_iter()
                .map(move |files| FileReader { files, host, idx }),
        ) as Box<dyn Iterator<Item = Self::Item>>)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BytesReader {
    files: Vec<PathBuf>,
    idx: usize,
    host: Ipv4Addr,
    namenode: String,
}

impl Split for BytesReader {
    fn get_index(&self) -> usize {
        self.idx
    }
}

impl Iterator for BytesReader {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        let fs = Client::connect(self.namenode.as_str()).unwrap();
        if let Some(path) = self.files.pop() {
            let file = fs
                .open_file()
                .read(true)
                .open(path.to_str().unwrap())
                .unwrap();
            let mut content = vec![];
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut content).unwrap();
            Some(content)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileReader {
    files: Vec<PathBuf>,
    idx: usize,
    host: Ipv4Addr,
}

impl Split for FileReader {
    fn get_index(&self) -> usize {
        self.idx
    }
}

impl Iterator for FileReader {
    type Item = PathBuf;
    fn next(&mut self) -> Option<Self::Item> {
        self.files.pop()
    }
}
