use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use obscura_net::{RequestInfo, interceptor::{RequestInterceptor, InterceptAction}};
use obscura_js::ops::{InterceptedRequest, InterceptResolution};

use serde_json::{json, Value};

use crate::dispatch::CdpContext;

/// Once installed, this page policy cannot be disabled or replaced by another
/// session. Any rejection, lost controller or deadline poisons the whole page.
struct RequestStageGuard {
    tx: tokio::sync::mpsc::UnboundedSender<InterceptedRequest>,
    session_id: String,
    frame_id: String,
    failed: AtomicBool,
    timeout: std::time::Duration,
}

struct GuardAttempt<'a> {
    failed: &'a AtomicBool,
    approved: bool,
}

impl Drop for GuardAttempt<'_> {
    fn drop(&mut self) {
        if !self.approved { self.failed.store(true, Ordering::Release); }
    }
}

#[async_trait::async_trait]
impl RequestInterceptor for RequestStageGuard {
    fn minimum_wait_timeout(&self) -> std::time::Duration { self.timeout }

    async fn intercept(&self, info: &RequestInfo) -> InterceptAction {
        if self.failed.load(Ordering::Acquire) { return InterceptAction::Block; }
        // Cancellation by a navigation/render deadline must poison the page
        // just like a lost controller; it must never silently skip a decision.
        let mut attempt = GuardAttempt { failed: &self.failed, approved: false };
        if !matches!(info.url.scheme(), "http" | "https")
            || obscura_net::env_allows_private_network()
            || info.headers.keys().any(|name| name.eq_ignore_ascii_case("host") || name.eq_ignore_ascii_case("proxy-authorization"))
        {
            self.failed.store(true, Ordering::Release);
            return InterceptAction::Block;
        }
        let (resolver, resolution) = tokio::sync::oneshot::channel();
        let request = InterceptedRequest {
            session_id: Some(self.session_id.clone()),
            frame_id: Some(self.frame_id.clone()),
            request_id: format!("guard-{}", uuid::Uuid::new_v4()),
            url: info.url.to_string(),
            method: info.method.clone(),
            headers: info.headers.clone(),
            resource_type: format!("{:?}", info.resource_type),
            resolver,
        };
        if self.tx.send(request).is_ok() {
            if let Ok(Ok(InterceptResolution::Continue { url: None, method: None, headers: None, body: None })) =
                tokio::time::timeout(self.timeout, resolution).await
            {
                if !self.failed.load(Ordering::Acquire) {
                    attempt.approved = true;
                    return InterceptAction::Continue;
                }
            }
        }
        self.failed.store(true, Ordering::Release);
        InterceptAction::Block
    }
}

pub struct PausedRequest {
    pub request_id: String,
    pub url: String,
    pub method: String,
    pub headers: HashMap<String, String>,
    pub resource_type: String,
    pub resolver: tokio::sync::oneshot::Sender<FetchResolution>,
}

pub enum FetchResolution {
    Continue {
        url: Option<String>,
        method: Option<String>,
        headers: Option<HashMap<String, String>>,
        post_data: Option<String>,
    },
    Fulfill {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    },
    Fail {
        reason: String,
    },
}

pub struct FetchInterceptState {
    pub enabled: bool,
    pub guarded_pages: HashSet<String>,
    pub patterns: Vec<String>,
    pub paused: HashMap<String, PausedRequest>,
    request_counter: u64,
}

impl FetchInterceptState {
    pub fn new() -> Self {
        FetchInterceptState {
            enabled: false,
            guarded_pages: HashSet::new(),
            patterns: Vec::new(),
            paused: HashMap::new(),
            request_counter: 0,
        }
    }

    pub fn next_request_id(&mut self) -> String {
        self.request_counter += 1;
        format!("interception-{}", self.request_counter)
    }
}

pub async fn handle(
    method: &str,
    params: &Value,
    ctx: &mut CdpContext,
    session_id: &Option<String>,
) -> Result<Value, String> {
    if matches!(method, "enable" | "disable") {
        let sid = session_id.as_ref().ok_or("Fetch requires an attached page session")?;
        let page_id = ctx.sessions.get(sid).ok_or("Unknown Fetch session")?;
        if ctx.fetch_intercept.guarded_pages.contains(page_id) {
            return Err("Request-stage guard is immutable; close the target to remove it".into());
        }
    }
    match method {
        "enable" => {
            if params.get("requestGuard").and_then(Value::as_bool) == Some(true) {
                let patterns = params.get("patterns").and_then(Value::as_array)
                    .ok_or("Request-stage guard requires all Request patterns")?;
                if patterns.len() != 1 || patterns[0].get("urlPattern").and_then(Value::as_str) != Some("*")
                    || patterns[0].get("requestStage").and_then(Value::as_str) != Some("Request")
                    || patterns[0].get("resourceType").is_some()
                {
                    return Err("Request-stage guard requires an unrestricted Request pattern".into());
                }
                let sid = session_id.as_ref().unwrap().clone();
                let tx = ctx.intercept_tx.clone().ok_or("Request-stage controller unavailable")?;
                let page = ctx.get_session_page_mut(session_id).ok_or("Unknown Fetch page")?;
                if page.context.allow_private_network || page.http_client.allow_private_network
                    || obscura_net::env_allows_private_network() || page.context.allow_file_access
                    || page.context.proxy_url.is_some() || page.http_client.proxy_url().is_some()
                {
                    return Err("Request-stage guard requires public-network direct transport and no file access".into());
                }
                if page.url.as_ref().is_some_and(|url| url.as_str() != "about:blank")
                    || page.http_client.in_flight.load(Ordering::Acquire) != 0
                    || !page.frames.is_empty()
                {
                    return Err("Request-stage guard must be installed before navigation".into());
                }
                let page_id = page.id.clone();
                page.set_navigation_timeout(std::time::Duration::from_secs(65));
                page.intercept_block_patterns.clear();
                page.enable_intercept(false);
                page.set_request_interceptor(Some(Arc::new(RequestStageGuard {
                    tx, session_id: sid, frame_id: page.frame_id.clone(),
                    failed: AtomicBool::new(false), timeout: std::time::Duration::from_secs(65),
                })));
                ctx.fetch_intercept.guarded_pages.insert(page_id);
                return Ok(json!({"requestGuard": {
                    "version": 1, "stage": "beforeNetwork", "allRequests": true,
                    "redirects": true, "connectTimeDnsValidated": true,
                }}));
            }
            let patterns = params
                .get("patterns")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|p| {
                            p.get("urlPattern")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec!["*".to_string()]);

            ctx.fetch_intercept.enabled = true;
            ctx.fetch_intercept.patterns = patterns.clone();
            let tx_clone = ctx.intercept_tx.clone();
            if let Some(page) = ctx.get_session_page_mut(session_id) {
                page.intercept_block_patterns = patterns.clone();
                if let Some(tx) = tx_clone {
                    page.set_intercept_tx(tx);
                }
                page.enable_intercept(true);
            }

            tracing::info!("Fetch interception enabled");
            Ok(json!({}))
        }
        "disable" => {
            ctx.fetch_intercept.enabled = false;
            ctx.fetch_intercept.patterns.clear();
            if let Some(page) = ctx.get_session_page_mut(session_id) {
                page.intercept_block_patterns.clear();
                page.enable_intercept(false);
            }
            let paused: Vec<_> = ctx.fetch_intercept.paused.drain().collect();
            for (_, req) in paused {
                let _ = req.resolver.send(FetchResolution::Continue {
                    url: None,
                    method: None,
                    headers: None,
                    post_data: None,
                });
            }
            Ok(json!({}))
        }
        "continueRequest" => {
            let request_id = params
                .get("requestId")
                .and_then(|v| v.as_str())
                .ok_or("requestId required")?;

            if let Some(paused) = ctx.fetch_intercept.paused.remove(request_id) {
                let _ = paused.resolver.send(FetchResolution::Continue {
                    url: params
                        .get("url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    method: params
                        .get("method")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    headers: None,
                    post_data: params
                        .get("postData")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                });
            }
            Ok(json!({}))
        }
        "fulfillRequest" => {
            let request_id = params
                .get("requestId")
                .and_then(|v| v.as_str())
                .ok_or("requestId required")?;

            let status = params
                .get("responseCode")
                .and_then(|v| v.as_u64())
                .unwrap_or(200) as u16;
            let headers: HashMap<String, String> = params
                .get("responseHeaders")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|h| {
                            let name = h.get("name")?.as_str()?.to_string();
                            let value = h.get("value")?.as_str()?.to_string();
                            Some((name, value))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let body = params
                .get("body")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if let Some(paused) = ctx.fetch_intercept.paused.remove(request_id) {
                let _ = paused.resolver.send(FetchResolution::Fulfill {
                    status,
                    headers: headers.into_iter().collect(),
                    body,
                });
            }
            Ok(json!({}))
        }
        "failRequest" => {
            let request_id = params
                .get("requestId")
                .and_then(|v| v.as_str())
                .ok_or("requestId required")?;

            let reason = params
                .get("errorReason")
                .and_then(|v| v.as_str())
                .unwrap_or("Failed")
                .to_string();

            if let Some(paused) = ctx.fetch_intercept.paused.remove(request_id) {
                let _ = paused.resolver.send(FetchResolution::Fail { reason });
            }
            Ok(json!({}))
        }
        "getResponseBody" => Ok(json!({ "body": "", "base64Encoded": false })),
        "takeResponseBodyAsStream" => {
            // Hand the client a streaming handle for a large response body so it
            // can pull it in chunks via IO.read and free it with IO.close,
            // instead of receiving one giant base64 blob (issue #360). The body
            // is moved out of the page cache into the stream, so it is held once
            // and released on close. Requires the body to have been cached
            // (raise OBSCURA_NETWORK_BODY_BUFFER_BYTES for large downloads).
            let request_id = params
                .get("requestId")
                .and_then(|v| v.as_str())
                .ok_or("Fetch.takeResponseBodyAsStream requires requestId")?;

            let bytes = {
                let page = ctx.get_session_page_mut(session_id).ok_or("No page")?;
                page.take_response_body_raw(request_id)
            }
            .or_else(|| {
                ctx.pages
                    .iter_mut()
                    .find_map(|p| p.take_response_body_raw(request_id))
            })
            .ok_or_else(|| {
                format!("Fetch.takeResponseBodyAsStream: no cached body for {request_id}")
            })?;

            let handle = ctx
                .io_streams
                .insert(bytes)
                .map_err(|error| format!("Fetch.takeResponseBodyAsStream: {error}"))?;
            Ok(json!({ "stream": handle }))
        }
        _ => Err(format!("Unknown Fetch method: {}", method)),
    }
}

#[cfg(test)]
mod request_stage_tests {
    use super::*;
    use obscura_browser::BrowserContext;
    use obscura_net::ResourceType;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn guard() -> (Arc<RequestStageGuard>, tokio::sync::mpsc::UnboundedReceiver<InterceptedRequest>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Arc::new(RequestStageGuard {
            tx, session_id: "owner".into(), frame_id: "frame".into(),
            failed: AtomicBool::new(false), timeout: std::time::Duration::from_millis(100),
        }), rx)
    }

    fn info() -> RequestInfo {
        RequestInfo { url: "https://example.edu/profile".parse().unwrap(), method: "GET".into(),
            headers: HashMap::new(), resource_type: ResourceType::Document }
    }

    #[tokio::test]
    async fn request_stage_disconnect_and_timeout_poison_page() {
        let (policy, rx) = guard();
        drop(rx);
        assert!(matches!(policy.intercept(&info()).await, InterceptAction::Block));
        let (policy, mut rx) = guard();
        let request = info();
        let (action, _) = tokio::join!(policy.intercept(&request), async {
            let pending = rx.recv().await.unwrap();
            assert_eq!(pending.session_id.as_deref(), Some("owner"));
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            assert!(pending.resolver.is_closed());
        });
        assert!(matches!(action, InterceptAction::Block));
        assert!(matches!(policy.intercept(&request).await, InterceptAction::Block));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn request_stage_rewrite_cannot_bypass_policy() {
        let (policy, mut rx) = guard();
        let request = info();
        let (action, _) = tokio::join!(policy.intercept(&request), async {
            let pending = rx.recv().await.unwrap();
            pending.resolver.send(InterceptResolution::Continue {
                url: Some("http://127.0.0.1/".into()), method: None, headers: None, body: None,
            }).ok();
        });
        assert!(matches!(action, InterceptAction::Block));
        assert!(policy.failed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn request_stage_cancelled_wait_poison_page() {
        let (policy, mut receiver) = guard();
        let worker_policy = policy.clone();
        let request = tokio::spawn(async move { worker_policy.intercept(&info()).await });
        let pending = receiver.recv().await.unwrap();
        request.abort();
        assert!(matches!(request.await, Err(error) if error.is_cancelled()));
        assert!(pending.resolver.is_closed());
        assert!(matches!(policy.intercept(&info()).await, InterceptAction::Block));
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn request_stage_waits_for_serialized_approval_of_concurrent_resources() {
        let (mut policy, mut receiver) = guard();
        Arc::get_mut(&mut policy).unwrap().timeout = std::time::Duration::from_secs(65);
        let mut requests = Vec::new();
        for _ in 0..16 {
            let policy = policy.clone();
            requests.push(tokio::spawn(async move { policy.intercept(&info()).await }));
        }
        // Model a controller that queues the whole batch then grants one at a
        // time. Each future must stay suspended until its own decision arrives.
        let mut pending = Vec::new();
        for _ in 0..16 { pending.push(receiver.recv().await.unwrap()); }
        assert!(requests.iter().all(|request| !request.is_finished()));
        for request in pending {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            request.resolver.send(InterceptResolution::Continue { url: None, method: None, headers: None, body: None }).ok();
        }
        for request in requests { assert!(matches!(request.await.unwrap(), InterceptAction::Continue)); }
    }

    #[tokio::test]
    async fn request_stage_enable_rejects_missing_owner_proxy_and_narrow_patterns() {
        let mut ctx = CdpContext::new();
        let patterns = json!({"requestGuard":true,"patterns":[{"urlPattern":"*","requestStage":"Request"}]});
        assert!(handle("enable", &patterns, &mut ctx, &Some("unknown".into())).await.is_err());
        let id = ctx.create_page();
        let sid = Some("owner".to_string());
        ctx.sessions.insert("owner".into(), id);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        ctx.intercept_tx = Some(tx);
        assert!(handle("enable", &json!({"requestGuard":true,"patterns":[{"urlPattern":"*","requestStage":"Response"}]}), &mut ctx, &sid).await.is_err());
        let capability = handle("enable", &patterns, &mut ctx, &sid).await.unwrap();
        assert_eq!(capability["requestGuard"]["version"], 1);
        assert!(handle("disable", &json!({}), &mut ctx, &sid).await.is_err());
        assert!(handle("enable", &json!({}), &mut ctx, &sid).await.is_err());
        let mut proxied = CdpContext::new_with_proxy(Some("http://127.0.0.1:8080".into()));
        let id = proxied.create_page();
        proxied.sessions.insert("owner".into(), id);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        proxied.intercept_tx = Some(tx);
        assert!(handle("enable", &patterns, &mut proxied, &sid).await.is_err());
    }

    // The private-network opt-in is confined to this local transport fixture;
    // Fetch.enable explicitly rejects it in the production guard contract.
    #[tokio::test(flavor = "current_thread")]
    async fn request_stage_pauses_document_static_js_xhr_frame_and_each_redirect() {
        const CHROME_UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cors_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cors_address = cors_listener.local_addr().unwrap();
        let document = format!(r#"<html><head><link rel="stylesheet" href="/style"><script src="/script"></script></head><body><img src="/image"><iframe src="/frame"></iframe><script>document.body.getBoundingClientRect();fetch('/fetch-start');var x=new XMLHttpRequest();x.open('GET','/xhr');x.send();fetch('http://{cors_address}/cors',{{method:'POST',headers:{{'X-Guard':'1'}},body:'fixture'}});</script></body></html>"#);
        let cors_headers = format!("Access-Control-Allow-Origin: http://{address}\r\nAccess-Control-Allow-Methods: POST\r\nAccess-Control-Allow-Headers: x-guard\r\n");
        let approvals = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let observed = approvals.clone();
        let served = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let hits = served.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = tokio::select! {
                    connection = listener.accept() => connection.unwrap(),
                    connection = cors_listener.accept() => connection.unwrap(),
                };
                let mut bytes = [0; 8192];
                let count = socket.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..count]);
                assert!(request.lines().any(|line| line.to_ascii_lowercase().starts_with("user-agent:")
                    && line.split_once(':').unwrap().1.trim() == CHROME_UA), "caller Chrome UA lost: {request}");
                assert!(!request.to_ascii_lowercase().contains("hortus")
                    && !request.to_ascii_lowercase().contains("tutor"), "unexpected collector header");
                let path = request.split_whitespace().nth(1).unwrap_or("/").to_string();
                {
                    let mut pending = observed.lock().unwrap();
                    let index = pending.iter().position(|approved| approved == &path)
                        .unwrap_or_else(|| panic!("network preceded approval: {path}"));
                    pending.remove(index);
                }
                if request.starts_with("OPTIONS ") {
                    let requested_headers = request.lines().find(|line| line.to_ascii_lowercase()
                        .starts_with("access-control-request-headers:")).unwrap();
                    assert!(!requested_headers.to_ascii_lowercase().contains("user-agent"));
                }
                hits.lock().unwrap().push(path.clone());
                let (status, extra, body) = match path.as_str() {
                    "/start" => (302, "Location: /document\r\n", ""),
                    "/document" => (200, "Content-Type: text/html\r\n", document.as_str()),
                    "/cors" => (200, cors_headers.as_str(), "ok"),
                    "/style" => (200, "Content-Type: text/css\r\n", "@font-face{font-family:Fixture;src:url('/font')}body{color:blue;font-family:Fixture}"),
                    "/script" => (200, "Content-Type: application/javascript\r\n", "window.staticRan=true;"),
                    "/frame" => (200, "Content-Type: text/html\r\n", "<body>frame</body>"),
                    "/fetch-start" => (302, "Location: /fetch-end\r\n", ""),
                    _ => (200, "Content-Type: text/plain\r\n", "ok"),
                };
                let response = format!("HTTP/1.1 {status} Fixture\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let context = Arc::new(BrowserContext::with_storage_and_network("fixture".into(), None, false, None, None, true));
        let mut ctx = CdpContext::new_with_shared_context(context);
        let page_id = ctx.create_page();
        ctx.sessions.insert("owner".into(), page_id);
        crate::domains::network::handle("setUserAgentOverride", &json!({"userAgent": CHROME_UA}),
            &mut ctx, &Some("owner".into())).await.unwrap();
        let mut page = ctx.pages.remove(0);
        let (mut policy, mut rx) = guard();
        Arc::get_mut(&mut policy).unwrap().timeout = std::time::Duration::from_secs(5);
        page.set_request_interceptor(Some(policy));
        let controller = tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                assert!(request.headers.iter().any(|(name, value)| name.eq_ignore_ascii_case("user-agent")
                    && value == CHROME_UA), "guard did not observe the caller Chrome UA");
                let path = url::Url::parse(&request.url).unwrap().path().to_string();
                if path == "/font" {
                    // A queued controller decision must outlive the normal
                    // one-second render warmup without being discarded.
                    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
                }
                approvals.lock().unwrap().push(path);
                request.resolver.send(InterceptResolution::Continue { url: None, method: None, headers: None, body: None }).ok();
            }
        });
        page.navigate(&format!("http://{address}/start")).await.unwrap();
        #[cfg(feature = "render")]
        page.prepare_screenshot_resources(1000).await;
        let hits = served.lock().unwrap().clone();
        for path in ["/start", "/document", "/style", "/script", "/frame", "/fetch-start", "/fetch-end", "/xhr"] {
            assert!(hits.contains(&path.to_string()), "missing fixture request {path}: {hits:?}");
        }
        assert_eq!(hits.iter().filter(|path| path.as_str() == "/cors").count(), 2, "OPTIONS and POST must both pass the guard");
        #[cfg(feature = "render")]
        assert!(hits.contains(&"/image".into()), "missing image: {hits:?}");
        #[cfg(feature = "render")]
        assert!(hits.contains(&"/font".into()), "missing font: {hits:?}");
        controller.abort();
        server.abort();
    }
}
