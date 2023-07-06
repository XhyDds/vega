use crate::error::{Error, Result};
use crate::io::*;
use crate::rdd::HdfsReadRdd;
use crate::*;
use hdrs::Client;
use std::io::{Read, Write};

pub struct HdfsIO {
    nn: String,
    fs: Client,
}

impl HdfsIO {
    pub fn new(nn: String) -> Result<Self> {
        let nn = nn + ":9000";
        let fs = Client::connect(nn.as_str());
        let fs = match fs {
            Ok(fs) => fs,
            Err(_) => {
                return Err(Error::HdfsConnect(nn));
            }
        };
        Ok(HdfsIO { nn, fs })
    }

    pub fn read_to_vec(&mut self, path: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut oo = self.fs.open_file();
        let file = oo.read(true).open(path);
        let mut file = match file {
            Ok(file) => file,
            Err(_) => {
                return Err(Error::HdfsFileOpen(self.nn.to_string()));
            }
        };
        let res = file.read_to_end(&mut buf);
        match res {
            Ok(_) => {}
            Err(_) => {
                return Err(Error::HdfsRead(self.nn.to_string()));
            }
        }
        Ok(buf)
    }

    pub fn read_to_rdd<U, F>(
        &mut self,
        path: &str,
        context: &Arc<Context>,
        num_slices: usize,
        f: F,
    ) -> Result<SerArc<dyn Rdd<Item = U>>>
    where
        F: SerFunc(Vec<u8>) -> U,
        U: Data,
    {
        let rdd = HdfsReadRdd::new(context.clone(), path.to_string(), num_slices);
        let rdd = rdd.map(f);
        Ok(rdd)
    }

    pub fn write_to_hdfs (&mut self, data: &[u8], path: &str, create: bool) -> Result<()> {
        let file = self.fs.open_file().create(create).write(true).open(path);
        let mut file = match file {
            Ok(file) => file,
            Err(_) => {
                return Err(Error::HdfsFileOpen(self.nn.clone()));
            },
        };
        let res = file.write(data);
        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::HdfsWrite(self.nn.to_string())),
        }
    }

}
