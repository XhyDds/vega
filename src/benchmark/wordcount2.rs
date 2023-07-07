use std::time::Instant;
use std::{env, io::Write};
use vega::io::{HdfsIO, LocalFsIO, Decoders};
use vega::*;
pub fn wordcount2() -> Result<()> {
    //let start = Instant::now();

    let context = Context::new()?;
    let mut h = HdfsIO::new().unwrap();
    let lines = h
        .read_to_rdd_and_decode("/wc/1", &context, 2, Decoders::to_utf8_lines());
    let lines = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().map(|line| {
            let line = line.split(' ').collect::<Vec<_>>().into_iter().map(|s| s.to_string()).collect::<Vec<_>>();
            line
        })) as Box<dyn Iterator<Item = _>>
    }));
    let kv = lines.flat_map(Fn!(
        |words: Vec<String>| {
            Box::new(words.into_iter().map(
                |word| (word, 1)
            )) as Box<dyn Iterator<Item = _>>
    }));

    let res = kv.reduce_by_key(Fn!(
        |(a, b)| a + b
    ), 2).collect().unwrap();
    //println!("{:?}", res);
    println!("{:?}", h.write_to_hdfs(format!("{:?}", res).as_bytes(), "/res/1", true));
    //let duration = start.elapsed();
    //println!("Time elapsed is: {:?}", duration);
    Ok(())
}
