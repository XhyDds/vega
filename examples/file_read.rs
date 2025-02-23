use std::time::Instant;
use std::{env, io::Write};
#[cfg(test)]
use vega::io::{HdfsIO, LocalFsIO, Decoders};
use vega::*;
#[cfg(test)]
fn main() -> Result<()> {
    let start = Instant::now();

    let context = Context::new()?;
    // let deserializer = Fn!(|file: Vec<u8>| {
    //     String::from_utf8(file)
    //         .unwrap()
    //         .lines()
    //         .map(|s| s.to_string())
    //         .collect::<Vec<_>>()
    // });
    let mut h = HdfsIO::new().unwrap();
     let lines = h
         .read_to_rdd_and_decode("/csv", &context, 2, Decoders::to_utf8());
    // let lines = LocalFsIO::read_to_rdd_and_decode("/home/lml/1.csv", &context, 2, Decoders::to_utf8_lines());
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
