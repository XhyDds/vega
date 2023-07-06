use std::time::Instant;
use std::{env, io::Write};
use vega::io::{HdfsReaderConfig, HdfsIO};
use vega::*;
use vega::rdd::{HdfsReadRdd, MapperRdd};
fn main() -> Result<()> {
    std::env::set_var("JAVA_HOME", "/home/lml/.jdk/jdk1.8.0_371");
    std::env::set_var("HADOOP_HOME", "/home/lml/hadoop-3.3.5");
    let start = Instant::now();
    let mut file = std::fs::File::create("/tmp/env1.txt").expect("create failed");
    for (key, value) in env::vars() {
        let msg = format!("{}: {}\n", key, value);
        file.write(msg.as_bytes()).expect("write failed");
    }

    let context = Context::new()?;
    let deserializer = Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    });
    //let lines = context.read_source(HdfsReaderConfig::new("/csv"), deserializer);
    let lines = HdfsReadRdd::new(context.clone(), "/csv_folder".to_string(), 2);
    let lines = lines.map(deserializer);
    let lines = lines.flat_map(Fn!(|lines: Vec<String>| {
            Box::new(lines.into_iter().map(|line| {
                let line = line.split(',').collect::<Vec<_>>();
                (
                    (line[0].to_string()),
                    (line[7].parse::<f64>().unwrap(), 1.0),
                )
            })) as Box<dyn Iterator<Item = _>>
        }));
    let res = lines.collect().unwrap();
    // for content in res {
    //     println!("{}", String::from_utf8(content).unwrap());
    // }
    println!("result:{:?}", res);
    let duration = start.elapsed();
    println!("Time elapsed is: {:?}", duration);
    Ok(())
}
