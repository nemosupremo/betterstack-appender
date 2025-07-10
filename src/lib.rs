use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use flume::Sender;
use tracing_subscriber::fmt::MakeWriter;

pub const DEFAULT_BUFFERED_LINES_LIMIT: usize = 128_000;

#[allow(dead_code)]
#[derive(Debug)]
enum JoinHandle<T = ()> {
    Async(tokio::task::JoinHandle<T>),
    Sync(std::thread::JoinHandle<T>),
}

#[derive(Debug)]
pub(crate) enum Msg {
    Line(bytes::Bytes),
    Flush(flume::Sender<reqwest::Result<()>>),
    Shutdown,
}

#[must_use]
#[derive(Debug)]
pub struct WorkerGuard {
    _guard: Option<JoinHandle<()>>,
    sender: Sender<Msg>,
    shutdown: Sender<()>,
}

#[derive(Clone, Debug)]
pub struct BetterStackWriter {
    error_counter: ErrorCounter,
    channel: Sender<Msg>,
    is_lossy: bool,
}

#[derive(Clone, Debug)]
pub struct ErrorCounter(Arc<AtomicUsize>);

impl BetterStackWriter {
    pub fn builder<T: AsRef<str>, U: AsRef<str>>(ingestion_url: T, token: U) -> BetterStackBuilder {
        BetterStackBuilder {
            buffered_lines_limit: DEFAULT_BUFFERED_LINES_LIMIT,
            is_lossy: true,
            ingestion_url: String::from(ingestion_url.as_ref()),
            token: String::from(token.as_ref()),
            flush_interval: Duration::from_secs(5),
            handle: None,
            batch_size: 128,
            gzip: cfg!(feature = "gzip"),
        }
    }

    fn create(
        ingestion_url: String,
        token: String,
        buffered_lines_limit: usize,
        flush_interval: Duration,
        batch_size: usize,
        is_lossy: bool,
        use_gzip: bool,
        handle: Option<tokio::runtime::Handle>,
    ) -> (BetterStackWriter, WorkerGuard) {
        let (sender, receiver) = flume::bounded(buffered_lines_limit);

        let (shutdown_sender, shutdown_receiver) = flume::bounded(0);

        let http_client = reqwest::Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", token).try_into().unwrap(),
                );
                headers.insert(
                    reqwest::header::CONTENT_TYPE,
                    "application/x-ndjson".try_into().unwrap(),
                );
                headers
            })
            .build()
            .unwrap();

        let worker = worker::Worker::new(
            receiver,
            http_client,
            ingestion_url,
            shutdown_receiver,
            flush_interval,
            batch_size,
            use_gzip,
            handle,
        );
        let worker_guard =
            WorkerGuard::new(worker.worker_thread(), sender.clone(), shutdown_sender);

        (
            Self {
                channel: sender,
                error_counter: ErrorCounter(Arc::new(AtomicUsize::new(0))),
                is_lossy,
            },
            worker_guard,
        )
    }

    pub fn error_counter(&self) -> ErrorCounter {
        self.error_counter.clone()
    }
}

#[derive(Debug)]
pub struct BetterStackBuilder {
    buffered_lines_limit: usize,
    is_lossy: bool,
    ingestion_url: String,
    token: String,
    flush_interval: Duration,
    batch_size: usize,
    handle: Option<tokio::runtime::Handle>,
    gzip: bool,
}

impl BetterStackBuilder {
    pub fn buffered_lines_limit(mut self, buffered_lines_limit: usize) -> Self {
        self.buffered_lines_limit = buffered_lines_limit;
        self
    }

    pub fn lossy(mut self, is_lossy: bool) -> Self {
        self.is_lossy = is_lossy;
        self
    }

    pub fn batch_size(mut self, batch_size: std::num::NonZeroUsize) -> Self {
        self.batch_size = batch_size.get();
        self
    }

    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    pub fn handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    #[cfg(feature = "gzip")]
    pub fn gzip(mut self, use_gzip: bool) -> Self {
        self.gzip = use_gzip;
        self
    }

    pub fn finish(self) -> (BetterStackWriter, WorkerGuard) {
        BetterStackWriter::create(
            self.ingestion_url,
            self.token,
            self.buffered_lines_limit,
            self.flush_interval,
            self.batch_size,
            self.is_lossy,
            self.gzip,
            self.handle,
        )
    }
}

impl std::io::Write for BetterStackWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let buf_size = buf.len();
        if self.is_lossy {
            if self
                .channel
                .try_send(Msg::Line(bytes::Bytes::from(buf.to_vec())))
                .is_err()
            {
                self.error_counter.incr_saturating();
            }
        } else {
            return match self
                .channel
                .send(Msg::Line(bytes::Bytes::from(buf.to_vec())))
            {
                Ok(_) => Ok(buf_size),
                Err(_) => Err(io::Error::from(io::ErrorKind::Other)),
            };
        }
        Ok(buf_size)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write(buf).map(|_| ())
    }
}

impl<'a> MakeWriter<'a> for BetterStackWriter {
    type Writer = BetterStackWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl WorkerGuard {
    fn new(handle: JoinHandle<()>, sender: Sender<Msg>, shutdown: Sender<()>) -> Self {
        WorkerGuard {
            _guard: Some(handle),
            sender,
            shutdown,
        }
    }
}

impl WorkerGuard {
    pub fn flush(&self) -> flume::Receiver<reqwest::Result<()>> {
        let (sender, receiver) = flume::bounded(1);
        let _ = self.sender.send(Msg::Flush(sender));
        receiver
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        match self
            .sender
            .send_timeout(Msg::Shutdown, Duration::from_millis(100))
        {
            Ok(_) => {
                // Attempt to wait for `Worker` to flush all messages before dropping. This happens
                // when the `Worker` calls `recv()` on a zero-capacity channel. Use `send_timeout`
                // so that drop is not blocked indefinitely.
                // TODO: Make timeout configurable.

                let _ = self.shutdown.send_timeout((), Duration::from_millis(5000));
            }
            Err(flume::SendTimeoutError::Disconnected(_)) => (),
            Err(flume::SendTimeoutError::Timeout(e)) => println!(
                "Failed to send shutdown signal to logging worker. Error: {:?}",
                e
            ),
        }
    }
}

impl ErrorCounter {
    pub fn dropped_lines(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    fn incr_saturating(&self) {
        let mut curr = self.0.load(Ordering::Acquire);
        // We don't need to enter the CAS loop if the current value is already
        // `usize::MAX`.
        if curr == usize::MAX {
            return;
        }

        // This is implemented as a CAS loop rather than as a simple
        // `fetch_add`, because we don't want to wrap on overflow. Instead, we
        // need to ensure that saturating addition is performed.
        loop {
            let val = curr.saturating_add(1);
            match self
                .0
                .compare_exchange(curr, val, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(actual) => curr = actual,
            }
        }
    }
}

mod worker {
    use std::fmt::Debug;
    use std::thread;
    use std::time::Duration;

    use super::{JoinHandle, Msg};
    use flume::{Receiver, RecvError, TryRecvError};
    use futures::{FutureExt, TryFutureExt};
    use tokio::time::Instant;

    pub(crate) struct Worker {
        client: reqwest::Client,
        ingestion_url: String,
        buffer: Vec<bytes::Bytes>,
        receiver: Receiver<Msg>,
        shutdown: Receiver<()>,
        batch_size: usize,
        last_flush: Instant,
        flush_interval: Duration,
        use_gzip: bool,
        handle: Option<tokio::runtime::Handle>,
    }

    #[derive(Debug, Clone)]
    pub(crate) enum WorkerState {
        Empty,
        Disconnected,
        Continue,
        Flush(flume::Sender<reqwest::Result<()>>),
        Shutdown,
    }

    impl Worker {
        pub(crate) fn new(
            receiver: Receiver<Msg>,
            client: reqwest::Client,
            ingestion_url: String,
            shutdown: Receiver<()>,
            flush_interval: Duration,
            batch_size: usize,
            use_gzip: bool,
            handle: Option<tokio::runtime::Handle>,
        ) -> Worker {
            Self {
                client,
                receiver,
                shutdown,
                ingestion_url,
                batch_size,
                buffer: vec![],
                last_flush: Instant::now(),
                flush_interval,
                use_gzip,
                handle,
            }
        }

        fn any_buffer_datasize(&self) -> bool {
            self.buffer.iter().any(|b| b.len() > 0)
        }

        fn buffer_datasize(&self) -> usize {
            self.buffer.iter().map(|b| b.len()).sum()
        }

        fn handle_recv(&mut self, result: Result<Msg, RecvError>) -> reqwest::Result<WorkerState> {
            match result {
                Ok(Msg::Line(msg)) => {
                    self.buffer.push(msg.clone());
                    Ok(WorkerState::Continue)
                }
                Ok(Msg::Flush(sender)) => Ok(WorkerState::Flush(sender)),
                Ok(Msg::Shutdown) => Ok(WorkerState::Shutdown),
                Err(_) => Ok(WorkerState::Disconnected),
            }
        }

        fn handle_try_recv(
            &mut self,
            result: Result<Msg, TryRecvError>,
        ) -> reqwest::Result<WorkerState> {
            match result {
                Ok(Msg::Line(msg)) => {
                    self.buffer.push(msg.clone());
                    Ok(WorkerState::Continue)
                }
                Ok(Msg::Flush(sender)) => Ok(WorkerState::Flush(sender)),
                Ok(Msg::Shutdown) => Ok(WorkerState::Shutdown),
                Err(TryRecvError::Empty) => Ok(WorkerState::Empty),
                Err(TryRecvError::Disconnected) => Ok(WorkerState::Disconnected),
            }
        }

        /// Blocks on the first recv of each batch of logs, unless the
        /// channel is disconnected. Afterwards, grabs as many logs as
        /// it can off the channel, buffers them and attempts a flush.
        pub(crate) async fn work(&mut self) -> reqwest::Result<WorkerState> {
            // Worker thread yields here if receive buffer is empty
            let msg = loop {
                let f = if !self.any_buffer_datasize() {
                    self.receiver.recv_async().map(|m| Ok(m)).left_future()
                } else {
                    tokio::time::timeout_at(
                        self.last_flush + self.flush_interval,
                        self.receiver.recv_async(),
                    )
                    .right_future()
                };
                if let Ok(msg) = f.await {
                    break msg;
                }
                self.flush(false).await?;
                continue;
            };
            let mut worker_state = self.handle_recv(msg)?;

            while matches!(worker_state, WorkerState::Continue) {
                let try_recv_result = self.receiver.try_recv();
                let handle_result = self.handle_try_recv(try_recv_result);
                if self.last_flush.elapsed() > self.flush_interval && self.any_buffer_datasize() {
                    self.flush(false).await?;
                }
                worker_state = handle_result?;
            }
            self.flush(false).await?;
            Ok(worker_state)
        }

        pub(crate) async fn worker_task(mut self) {
            loop {
                match self.work().await {
                    Ok(WorkerState::Flush(sender)) => {
                        let _ = sender.send(self.flush(true).await);
                    }
                    Ok(WorkerState::Continue) | Ok(WorkerState::Empty) => {}
                    Ok(WorkerState::Shutdown) | Ok(WorkerState::Disconnected) => {
                        if let Err(e) = self.flush(true).await {
                            eprintln!("Failed to flush. Error: {}", e);
                        }
                        let _ = self.shutdown.recv_async().await;
                        break;
                    }
                    Err(_) => {
                        // TODO: Expose a metric for IO Errors, or print to stderr
                    }
                }
            }
        }

        /// Creates a worker thread that processes a channel until it's disconnected
        pub(crate) fn worker_thread(mut self) -> JoinHandle {
            let handle = self
                .handle
                .take()
                .or_else(|| tokio::runtime::Handle::try_current().ok());
            if let Some(handle) = handle {
                JoinHandle::Async(handle.spawn(self.worker_task()))
            } else {
                JoinHandle::Sync(
                    thread::Builder::new()
                        .spawn(move || {
                            let rt = tokio::runtime::Builder::new_current_thread()
                                .enable_io()
                                .enable_time()
                                .build()
                                .unwrap();
                            rt.block_on(self.worker_task())
                        })
                        .expect("failed to spawn `tracing-appender` non-blocking worker thread"),
                )
            }
        }

        async fn flush(&mut self, force: bool) -> reqwest::Result<()> {
            let content_length = self.buffer_datasize();
            if content_length == 0 {
                return Ok(());
            }
            if self.buffer.len() > self.batch_size
                || self.last_flush.elapsed() > self.flush_interval
                || force
            {
                let req = self.client.post(&self.ingestion_url);

                let req = if self.use_gzip && cfg!(feature = "gzip") {
                    #[cfg(feature = "gzip")]
                    {
                        use tokio_util::io::StreamReader;

                        let stream_reader = StreamReader::new(futures::stream::iter(
                            self.buffer
                                .clone()
                                .into_iter()
                                .map(|buf| Ok::<_, tokio::io::Error>(buf)),
                        ));
                        let compressed_reader =
                            async_compression::tokio::bufread::GzipEncoder::new(stream_reader);
                        let stream = tokio_util::io::ReaderStream::new(compressed_reader);
                        let body = reqwest::Body::wrap_stream(stream);

                        req.header(reqwest::header::CONTENT_ENCODING, "gzip")
                            .body(body)
                    }
                    #[cfg(not(feature = "gzip"))]
                    unreachable!()
                } else {
                    let body = reqwest::Body::wrap_stream(futures::stream::iter(
                        self.buffer
                            .clone()
                            .into_iter()
                            .map(|b| Ok::<_, std::convert::Infallible>(b)),
                    ));

                    req.header(reqwest::header::CONTENT_LENGTH, content_length.to_string())
                        .body(body)
                };

                req.send()
                    .and_then(|resp| async move { resp.error_for_status() })
                    .await?;

                self.buffer.clear();
                self.last_flush = Instant::now();
            }
            return Ok(());
        }
    }
}
