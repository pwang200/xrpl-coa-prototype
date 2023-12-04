// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use crypto::{generate_production_keypair, PublicKey, SecretKey};
use worker::Transaction;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let client = Client {
        target,
        size,
        rate,
        nodes,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

async fn fanout(tag: u64, burst: u64, tx_size: usize, jump: u64, mut r: u64, tx_txns: Sender<(u64, Vec<Bytes>)>) {
    info!("fanout {}", tag);
    let (public_key, secret_key) = generate_production_keypair();
    let mut data = 0u64;
    let mut counter = tag;
    let mut tx = BytesMut::with_capacity(tx_size);
    loop {
        let mut txns = Vec::with_capacity(burst as usize);
        for x in 0..burst {
            if x == counter % burst {
                tx.put_u8(0u8); // Sample txs start with 0.
                tx.put_u64(counter); // This counter identifies the tx.
            } else {
                r += jump;
                tx.put_u8(1u8); // Standard txs start with 1.
                tx.put_u64(r); // Ensures all clients send different txs.
            };

            let t = Transaction::new(tag, data, public_key.clone(), &secret_key);
            data += 1;
            // let good_sig = t.verify();
            // info!("SigTestSign {}", good_sig);

            let payload = bincode::serialize(&t).unwrap();
            tx.put_u8(payload.len() as u8);
            tx.extend(payload.into_iter());
            tx.resize(tx_size, 0u8);
            let bytes = tx.split().freeze();
            txns.push(bytes);
        }

        tx_txns.send((counter, txns)).await;
        counter += jump;
    }
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    nodes: Vec<SocketAddr>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;
        const FANOUT: u64 = worker::FANOUT as u64;
        const CHANNEL_CAPACITY: usize = 250_000;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        // assert_eq!(burst % FANOUT, 0);
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter_expect = 0;
        let mut r = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        let mut tx_sources = Vec::new();
        for tag in 0..FANOUT {
            let (tx_txns, rx_txns) = channel(CHANNEL_CAPACITY);
            tokio::task::spawn(fanout(tag, burst, self.size, FANOUT, r, tx_txns));
            tx_sources.push(rx_txns);
        }

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            let (counter, txns) = tx_sources[(counter_expect % FANOUT) as usize].recv().await.unwrap();
            assert!(counter == counter_expect);
            // NOTE: This log entry is used to compute performance.
            info!("Sending sample transaction {}", counter);
            for tx in txns {
                if let Err(e) = transport.send(tx).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }

            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client. time {:?} ms, should be < {:?}", now.elapsed().as_millis(), BURST_DURATION);
            }
            counter_expect += 1;
        }

        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
