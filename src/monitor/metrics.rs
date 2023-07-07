use prometheus_client::encoding::EncodeLabelValue;
use prometheus_client::encoding::{text::encode, EncodeLabelSet};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::context::Context;
use crate::env;

use tide::{Middleware, Next, Request, Result};

#[derive(Debug, Deserialize, Serialize)]
struct PostMessage {
    signal: String,
}

pub async fn add_metric(sc: Arc<Context>) -> std::result::Result<(), std::io::Error> {
    let mut registry = Registry::default();
    let http_requests_total = Family::<Labels, Counter>::default();
    let node_num: Counter = Counter::default();
    let vega_seconds: Counter = Counter::default();
    let vega_content = Family::<ContentSet, Gauge>::default();

    registry.register(
        "http_requests",
        "Number of HTTP requests",
        http_requests_total.clone(),
    );
    registry.register("node_num", "Number of nodes in cluster", node_num.clone());
    registry.register(
        "vega_seconds",
        "Running seconds of vega",
        vega_seconds.clone(),
    );
    registry.register(
        "vega_content",
        "Running content of vega",
        vega_content.clone(),
    );

    node_num.inc();

    tokio::spawn(async move {
        loop {
            vega_seconds.inc();
            tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
        }
    });

    let deployment_mode = env::Configuration::get().deployment_mode;
    if deployment_mode == env::DeploymentMode::Distributed {
        node_num.inc_by(sc.address_map.len() as u64);
    }

    let content = Content::Prepared;
    let _ = vega_content.get_or_create(&ContentSet { content });
    let content = Content::Finished;
    let _ = vega_content.get_or_create(&ContentSet { content });

    let middleware = MetricsMiddleware {
        http_requests_total,
        vega_content,
    };
    let mut app = tide::with_state(State {
        registry: Arc::new(registry),
    });

    app.with(middleware);
    app.at("/").get(|_| async { Ok("Hello, world!") });
    app.at("/metrics")
        .get(|req: tide::Request<State>| async move {
            let mut encoded = String::new();
            encode(&mut encoded, &req.state().registry).unwrap();
            let response = tide::Response::builder(200)
                .body(encoded)
                .content_type("text/plain; version=1.0.0; charset=utf-8")
                .build();
            Ok(response)
        });
    app.at("/interface")
        .post(|mut req: Request<State>| async move {
            let _: PostMessage = req.body_json().await?;
            let response = tide::Response::builder(201)
                .body("1")
                .content_type("text/plain; version=1.0.0; charset=utf-8")
                .build();
            Ok(response)
        });

    app.listen("127.0.0.1:8000").await?;

    Ok(())
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Labels {
    method: Method,
    path: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ContentSet {
    content: Content,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum Content {
    Prepared,
    Finished,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum Method {
    Get,
    Put,
    Post,
}

#[derive(Clone)]
struct State {
    registry: Arc<Registry>,
}

#[derive(Default)]
struct MetricsMiddleware {
    http_requests_total: Family<Labels, Counter>,
    vega_content: Family<ContentSet, Gauge>,
}

#[tide::utils::async_trait]
impl Middleware<State> for MetricsMiddleware {
    async fn handle(&self, mut req: Request<State>, next: Next<'_, State>) -> Result {
        let method = match req.method() {
            http_types::Method::Get => Method::Get,
            http_types::Method::Put => Method::Put,
            http_types::Method::Post => {
                let post_message: PostMessage = req.body_json().await?;
                let content = if post_message.signal == "0" {
                    Content::Prepared
                } else if post_message.signal == "1" {
                    Content::Finished
                } else {
                    panic!("error signal");
                };
                self.vega_content
                    .get_or_create(&ContentSet { content })
                    .inc();
                Method::Post
            }
            _ => todo!(),
        };
        let path = req.url().path().to_string();
        let _ = self
            .http_requests_total
            .get_or_create(&Labels { method, path })
            .inc();

        let res = next.run(req).await;
        Ok(res)
    }
}
