use reqwest::header::HeaderMap;
use serde_json::Value;
use std::collections::HashMap;

pub async fn post(signal: String) -> Result<HashMap<String, Value>, reqwest::Error> {
    let client = reqwest::Client::new();

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    let mut data = HashMap::new();
    data.insert("signal", signal);

    Ok(client
        .post("http://localhost:8000/interface")
        .headers(headers)
        .json(&data)
        .send()
        .await?
        .json::<HashMap<String, Value>>()
        .await?)
}
