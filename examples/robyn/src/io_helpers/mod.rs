// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).

use std::fs::File;
use std::io::Read;

use anyhow::Result;

use crate::types::headers::Headers;

/// Port of `apply_hashmap_headers`: append every (name, value) pair of a Robyn
/// `Headers` onto a hyper `HeaderMap`.
#[inline]
pub fn apply_hashmap_headers(hyper_headers: &mut hyper::HeaderMap, headers: &Headers) {
    for iter in headers.headers.iter() {
        let (key, values) = iter.pair();
        let Ok(header_name) = hyper::header::HeaderName::from_bytes(key.as_bytes()) else {
            continue;
        };
        for value in values {
            if let Ok(header_value) = hyper::http::HeaderValue::from_str(value) {
                hyper_headers.append(&header_name, header_value);
            }
        }
    }
}

/// A function to read lossy files and serve it as a html response
///
/// # Arguments
///
/// * `file_path` - The file path that we want the function to read
///
// ideally this should be async
pub fn read_file(file_path: &str) -> Result<Vec<u8>> {
    let mut file = File::open(file_path)?;
    let mut buf = vec![];
    file.read_to_end(&mut buf)?;
    Ok(buf)
}
