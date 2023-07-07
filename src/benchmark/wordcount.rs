use std::fs::{create_dir_all, File};
use std::io::prelude::*;
use std::sync::Arc;

use capnp::private::units::WORDS_PER_POINTER;
use once_cell::sync::Lazy;
use vega::io::*;
use vega::partitioner::HashPartitioner;
use vega::*;


#[allow(unused_must_use)]
fn set_up(file_name: &str) {
    let WORK_DIR=std::env::current_dir().unwrap();
    const TEST_DIR: &str = "tests/testdoc";
    let temp_dir = WORK_DIR.join(TEST_DIR);
    println!("Creating tests in dir: {}", (&temp_dir).to_str().unwrap());
    create_dir_all(&temp_dir);

    let fixture =
        b"This is some textual test data.\nCan be converted to strings and there are two lines.";

    if !std::path::Path::new(file_name).exists() {
        let mut f = File::create(temp_dir.join(file_name)).unwrap();
        f.write_all(fixture).unwrap();
    }
}
pub fn wordcount(sc: &Arc<Context>, file_name: Option<&str>){

    let deserializer = Fn!(|file: std::path::PathBuf| {
        let mut file = File::open(file).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        let parsed: Vec<_> = content.lines().map(|s| s.to_string()).collect();
        parsed
    });
    let WORK_DIR=std::env::current_dir().unwrap();
    let file_path=WORK_DIR.join("tests/testdoc");
    println!("file_path={:?}",file_path);
    let file_name=file_name.unwrap_or("doc_1");
    set_up(file_name);
    
    let textfile = 
        sc.read_source(LocalFsReaderConfig::new(file_path), deserializer)
            .flat_map(Fn!(|line:Vec<std::string::String>|{
                // println!("{:?}",line);
                Box::new(line.into_iter()) as Box<dyn Iterator<Item = _>>
            }));
    let r=textfile.map(Fn!(|line|{
        println!("{line}");
        for s in line.split(" "){
            
        }
    }));
    

    println!("{:?}",r.collect());
}