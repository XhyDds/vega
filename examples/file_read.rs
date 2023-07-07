use std::time::Instant;
use std::{env, io::Write};
use vega::io::{HdfsIO, LocalFsIO, Decoders};
use vega::*;
fn main() -> Result<()> {
    let start = Instant::now();
    let mut file = std::fs::File::create("/tmp/env1.txt").expect("create failed");
    for (key, value) in env::vars() {
        let msg = format!("{}: {}\n", key, value);
        file.write(msg.as_bytes()).expect("write failed");
    }

    let context = Context::new()?;
    // let deserializer = Fn!(|file: Vec<u8>| {
    //     String::from_utf8(file)
    //         .unwrap()
    //         .lines()
    //         .map(|s| s.to_string())
    //         .collect::<Vec<_>>()
    // });
    //let mut h = HdfsIO::new().unwrap();
    // let lines = h
    //     .read_to_rdd_and_decode("/csv_folder", &context, 2, Decoders::to_strings());
    let lines = LocalFsIO::read_to_rdd_and_decode("/home/lml/1.csv", &context, 2, Decoders::to_strings());
    // let lines = lines.flat_map(Fn!(|lines: Vec<String>| {
    //     Box::new(lines.into_iter().map(|line| {
    //         let line = line.split(',').collect::<Vec<_>>();
    //         (
    //             (line[0].to_string()),
    //             (line[7].parse::<f64>().unwrap(), 1.0),
    //         )
    //     })) as Box<dyn Iterator<Item = _>>
    // }));
    let res = lines.collect().unwrap();
    println!("{:?}", res);
    // println!("{:?}", h.write_to_hdfs(format!("{:?}", res).as_bytes(), "/res/2.txt", true));
    let duration = start.elapsed();
    println!("Time elapsed is: {:?}", duration);
    Ok(())
}
