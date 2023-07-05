use chrono::prelude::*;
use vega::io::*;
use vega::*;

fn main() -> Result<()> {
    let context = Context::new()?;
    let deserializer = Fn!(|file: Vec<u8>| {
        String::from_utf8(file)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    });
    let lines = context.read_source(
        LocalFsReaderConfig::new("/home/hao/lab3-data.csv"),
        deserializer,
    );
    println!("successfully read source");
    let line = lines.flat_map(Fn!(|lines: Vec<String>| {
        Box::new(lines.into_iter().map(|line| {
            let line = line.split(',').collect::<Vec<_>>();
            (
                (line[5].to_string()),
                (line[11].parse::<f64>().unwrap(), 1.0),
            )
        })) as Box<dyn Iterator<Item = _>>
    }));
    println!("successfully flat map");
    //let sum = line.reduce_by_key(Fn!(|((vl, cl), (vr, cr))| (vl + vr, cl + cr)), 1);
    //let avg = sum.map(Fn!(|(k, (v, c))| (k, v as f64 / c)));
    //let res = avg.collect().unwrap();
    println!("result: {:?}", line.collect().unwrap());
    Ok(())
}
