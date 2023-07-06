use std::time::Instant;
use std::{env, io::Write};
use vega::*;
use vega::io::HdfsIO;
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
    let mut H = HdfsIO::new("192.168.179.129".to_string()).unwrap();
    let lines = H.read_to_rdd("/csv_folder", &context, 2, deserializer).unwrap();
    //let lines = context.read_source(HdfsReaderConfig::new("/csv"), deserializer);
    //let lines = HdfsReadRdd::new(context.clone(), "/csv".to_string(), 2);
    //let lines = lines.map(deserializer);
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
    println!("{:?}", H.write_to_hdfs(format!("{:?}", res).as_bytes(), "/res/1.txt", true));
    let duration = start.elapsed();
    println!("Time elapsed is: {:?}", duration);
    Ok(())
}
