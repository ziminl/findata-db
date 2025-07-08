use futures::{StreamExt, SinkExt};
use std::sync::{Arc};
use tokio::sync::broadcast::{self, Sender};
use tokio_tungstenite::connect_async;
use tungstenite::Message;
use warp::Filter;
use url::Url;

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel::<String>(100);

    let tx_binance = tx.clone();
    let tx_bybit = tx.clone();

    tokio::spawn(async move {
        binance_feed(tx_binance).await;
    });

    tokio::spawn(async move {
        bybit_feed(tx_bybit).await;
    });

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and_then(move |ws: warp::ws::Ws| {
            let tx = tx.clone();
            async move {
                Ok::<_, warp::Rejection>(ws.on_upgrade(move |socket| client_handler(socket, tx)))
            }
        });

    println!("WebSocket server running on ws://127.0.0.1:3030/ws");
    warp::serve(ws_route).run(([127, 0, 0, 1], 3030)).await;
}

async fn binance_feed(tx: Sender<String>) {
    let url = Url::parse("wss://stream.binance.com:9443/ws/btcusdt@trade").unwrap();

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect to Binance");
    println!("Connected to Binance.");

    let (_, read) = ws_stream.split();

    read.for_each(|message| {
        let tx = tx.clone();
        async move {
            if let Ok(Message::Text(text)) = message {
                let _ = tx.send(format!(r#"{{"exchange":"binance","data":{}}}"#, text));
            }
        }
    }).await;
}

async fn bybit_feed(tx: Sender<String>) {
    let url = Url::parse("wss://stream.bybit.com/v5/public/linear").unwrap();

    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect to Bybit");
    println!("Connected to Bybit.");

    let subscribe_msg = serde_json::json!({
        "op": "subscribe",
        "args": ["publicTrade.BTCUSDT"]
    });

    ws_stream
        .send(Message::Text(subscribe_msg.to_string()))
        .await
        .unwrap();

    let (_, read) = ws_stream.split();

    read.for_each(|message| {
        let tx = tx.clone();
        async move {
            if let Ok(Message::Text(text)) = message {
                let _ = tx.send(format!(r#"{{"exchange":"bybit","data":{}}}"#, text));
            }
        }
    }).await;
}

async fn client_handler(ws: warp::ws::WebSocket, tx: Sender<String>) {
    let (_, mut write) = ws.split();

    let mut rx = tx.subscribe();

    while let Ok(msg) = rx.recv().await {
        if write.send(warp::ws::Message::text(msg)).await.is_err() {
            break;
        }
    }
}
