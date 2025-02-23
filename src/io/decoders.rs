use crate::{*, serializable_traits::SerFunc};
pub struct Decoders {}

impl Decoders {
    pub fn to_utf8() -> impl SerFunc(Vec<u8>) -> String {
        let f = Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
                .unwrap()
        });
        f
    }

    pub fn to_utf8_lines() -> impl SerFunc(Vec<u8>) -> Vec<String> {
        let f = Fn!(|file: Vec<u8>| {
            String::from_utf8(file)
                .unwrap()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        });
        f
    }
}