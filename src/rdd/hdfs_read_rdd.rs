use hdrs::{Client, OpenOptions};

use crate::rdd::{Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::dependency::{Dependency, OneToOneDependency};

pub struct HdfsReadRdd<T: Data> {
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,

    nn: Mutex<String>,
    is_dir: AtomicBool,
    path: Mutex<String>,
    fs: Client,
    open_options: OpenOptions,
}

impl HdfsReadRdd {
    //hdfs_read_rdd的构造函数
    //接受三个参数，分别是namenode的地址，是否是目录，目录或文件的路径
    //返回一个Result，因为有可能连接失败
    pub fn new(prev: Arc<dyn Rdd<Item = T>>, nn: String, is_dir: bool, path: String) -> Result<Self, std::io::Error> {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let fs = match Client::connect(nn.as_str()) {
            Ok(fs) => fs,
            Err(e) => return Err(e),
        };
        // match fs.metadata(path) {//检查文件是否存在
        //     Ok(_) => (),
        //     Err(e) => return Err(e),
        // }
        let oo = fs.open_file();
        HdfsReadRdd {
            nn: Mutex::new(nn),
            is_dir: AtomicBool::new(is_dir),
            path: Mutex::new(path),
            fs,
            oo,
        }
    }
}