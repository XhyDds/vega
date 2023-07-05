use std::net::SocketAddr;
use std::path::Path;

use crate::error::{Error, Result};
use once_cell::sync::OnceCell;
use serde_derive::Deserialize;

static HOSTS: OnceCell<Hosts> = OnceCell::new();

/// Handles loading of the hosts configuration.
/// hosts.conf文件的导入与配置
#[derive(Debug, Deserialize)]
pub(crate) struct Hosts {
    pub master: SocketAddr,
    /// The slaves have the format "user@address", e.g. "worker@192.168.0.2"
    pub slaves: Vec<Slave>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Slave {
    pub ip: String,
    pub key: String,
}

//hosts.conf文件格式参见更新后的手册（相关库：std::net::SocketAddr，toml::from_str）
impl Hosts {
    pub fn get() -> Result<&'static Hosts> {
        HOSTS.get_or_try_init(Self::load)
    }

    fn load() -> Result<Self> {
        let home = std::env::home_dir().ok_or(Error::NoHome)?;
        println!("home:{:?}", home);
        Hosts::load_from(home.join("hosts.conf"))
    }

    fn load_from<P: AsRef<Path>>(path: P) -> Result<Self> {
        let s = std::fs::read_to_string(&path).map_err(|e| Error::LoadHosts {
            source: e,
            path: path.as_ref().into(),
        })?;
        // //TEST
        // println!("there1");
        toml::from_str(&s).map_err(|e| Error::ParseHosts {
            source: e,
            path: path.as_ref().into(),
        })
        // //TEST
        // ;
        // println!("there2");
        // result
    }
}

/// cargo test
/// 判断hosts.conf文件是否存在以及合法性
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    //是否存在
    #[test]
    fn test_missing_hosts_file() {
        match Hosts::load_from("/does_not_exist").unwrap_err() {
            Error::LoadHosts { .. } => {}
            _ => panic!("Expected Error::LoadHosts"),
        }
    }

    //合法性
    #[test]
    fn test_invalid_hosts_file() {
        let (mut file, path) = tempfile::NamedTempFile::new().unwrap().keep().unwrap();
        file.write_all("invalid data".as_ref()).unwrap();

        match Hosts::load_from(&path).unwrap_err() {
            Error::ParseHosts { .. } => {}
            _ => panic!("Expected Error::ParseHosts"),
        }
    }
}
