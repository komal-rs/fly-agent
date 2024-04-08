use axum::http::StatusCode;
use axum::{response::Html, routing::get, Router};
use axum_auth::AuthBearer;
use std::env;
use std::net::SocketAddr;
use yup_oauth2::authenticator::HyperClientBuilder;

use reqwest::Client;
use serde_json::Value;
use yup_oauth2::{Error, hyper, ServiceAccountAuthenticator};

#[tokio::main]
async fn main() {
    // build our application with a route
    let app = Router::new()
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/call_bq", get(call_bigquery));

    // run it
    // let addr: SocketAddrV6 = "[::]:8080".parse().unwrap();
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 8080));

    println!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn hello_work_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
    if token
        != env::var("CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY")
            .expect("$CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY is not set")
    {
        return Html("Unauthorized");
    }

    Html("Hello, World!")
}

async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}

async fn call_bigquery() {
    let data = serde_json::json!({
        "kind": "bigquery#tableDataInsertAllRequest",
        "rows": [
            {
                "json": {
                    "event": "John3",
                    "params": "{\"hello3\":\"there3\"}"
                }
            }
        ]
    });

    println!("Data: {:?}", data);

    let res = stream_to_bigquery(data).await;
    println!("Response: {:?}", res);
}

pub struct WebpkiHyperClient;

impl HyperClientBuilder for WebpkiHyperClient {
    type Connector = yup_oauth2::hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>;

    fn build_hyper_client(self) -> Result<hyper::Client<Self::Connector>, Error> {
            let connector = yup_oauth2::hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();

        Ok(hyper::Client::builder()
            .pool_max_idle_per_host(0)
            .build::<_, hyper::Body>(connector))
    }

    fn build_test_hyper_client(self) -> hyper::Client<Self::Connector> {
            let connector = yup_oauth2::hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .unwrap()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();

        hyper::Client::builder()
            .pool_max_idle_per_host(0)
            .build::<_, hyper::Body>(connector)
    }
}

async fn get_access_token() -> String {
    // Load your service account key
    let sa_key_file = env::var("GOOGLE_SA_KEY_FILE").expect("GOOGLE_SA_KEY_FILE is required");

    // Load your service account key
    let sa_key =
        yup_oauth2::parse_service_account_key(sa_key_file).expect("GOOGLE_SA_KEY_FILE.json");

    let auth = ServiceAccountAuthenticator::with_client(sa_key, WebpkiHyperClient.build_hyper_client().unwrap())
        .build()
        .await
        .unwrap();

    let scopes = &["https://www.googleapis.com/auth/bigquery.insertdata"];
    let token = auth.token(scopes).await.unwrap();

    match token.token() {
        Some(t) => t.to_string(),
        _ => panic!("No access token found"),
    }
}

async fn stream_to_bigquery(data: Value) -> Result<(), Box<dyn std::error::Error>> {
    let token = get_access_token().await;
    let client = Client::builder()
        .tls_built_in_webpki_certs(true)
        .use_rustls_tls()
        .build()?;
    let request_url = "https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/events_analytics/insertAll";

    println!("Data streaming...");

    let response = client
        .post(request_url)
        .bearer_auth(token)
        .json(&data)
        .send()
        .await?;

    if response.status().is_success() {
        println!("Data streamed successfully.");
    } else {
        println!("Failed to stream data: {:?}", response.text().await?);
    }

    Ok(())
}


