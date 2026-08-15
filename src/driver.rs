use irodori_connector_abi::{collect_url_auth, option_bool, option_string, push_sensitive};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use reqwest::{Client, RequestBuilder};
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, QdrantConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct QdrantConnection {
    client: Client,
    config: QdrantConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QdrantConfig {
    base_url: String,
    api_key: Option<String>,
    bearer_token: Option<String>,
    tls: TlsConfig,
    redaction_values: Vec<String>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, QdrantConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match QdrantConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let client = match config.tls.build_client() {
        Ok(client) => client,
        Err(err) => return abi::error("connector.invalidRequest", config.redact(&err)),
    };
    let connection = QdrantConnection { client, config };
    let version = match runtime().and_then(|runtime| runtime.block_on(load_version(&connection))) {
        Ok(version) => version,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let mut response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "endpoint".to_string(),
            Value::String(connection.config.base_url.clone()),
        ),
    ]);
    if let Some(version) = version {
        response.insert("serverVersion".to_string(), Value::String(version));
    }
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(query_input) = abi::string_field(request, "query")
        .or_else(|| abi::string_field(request, "sql"))
        .or_else(|| abi::string_field(request, "statement"))
        .or_else(|| abi::string_field(request, "collection"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a collection name or JSON query string.",
        );
    };
    let query = match QdrantQuery::from_input(query_input, request, abi::max_rows(request)) {
        Ok(query) => query,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(run_scroll(&connection, query))) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl QdrantConnection {
    fn auth(&self, builder: RequestBuilder) -> RequestBuilder {
        if let Some(api_key) = self.config.api_key.as_deref() {
            builder.header("api-key", api_key)
        } else if let Some(token) = self.config.bearer_token.as_deref() {
            builder.bearer_auth(token)
        } else {
            builder
        }
    }
}

/// Transport security, as `connector.config.json` declares it under
/// `clientCertificate`.
///
/// Paths, never key material: connector options persist to the workspace in the
/// clear, so the profile carries a path and the driver reads the file at
/// connect time.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TlsConfig {
    root_cert_path: Option<String>,
    client_cert_path: Option<String>,
    client_key_path: Option<String>,
    accept_invalid_certs: bool,
}

impl TlsConfig {
    fn from_request(request: &Value) -> Self {
        Self {
            root_cert_path: option_string(
                request,
                &["sslRootCert", "sslrootcert", "ssl-ca", "caCert"],
            ),
            client_cert_path: option_string(
                request,
                &["sslCert", "sslcert", "ssl-cert", "clientCert"],
            ),
            client_key_path: option_string(request, &["sslKey", "sslkey", "ssl-key", "clientKey"]),
            accept_invalid_certs: option_string(request, &["sslInsecure", "tlsInsecure"])
                .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes")),
        }
    }

    /// A client honouring the profile's TLS material.
    ///
    /// Returns the default client untouched when nothing is configured, so a
    /// plain `http://` endpoint keeps working exactly as before.
    fn build_client(&self) -> Result<Client, String> {
        if self.root_cert_path.is_none()
            && self.client_cert_path.is_none()
            && self.client_key_path.is_none()
            && !self.accept_invalid_certs
        {
            return Client::builder()
                .build()
                .map_err(|err| format!("HTTP client setup failed: {err}"));
        }

        let mut builder = Client::builder();

        if let Some(path) = &self.root_cert_path {
            let pem = read_pem(path, "SSL root certificate")?;
            let bundle = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|err| format!("SSL root certificate at {path} is not valid PEM: {err}"))?;
            // `from_pem_bundle` answers Ok(vec![]) for a file with no PEM
            // blocks in it. Adding nothing and carrying on would fall back to
            // the system roots — the connection would succeed while verifying
            // against something other than the CA the user named.
            if bundle.is_empty() {
                return Err(format!(
                    "SSL root certificate at {path} contains no PEM certificate."
                ));
            }
            for certificate in bundle {
                builder = builder.add_root_certificate(certificate);
            }
        }

        // reqwest wants one PEM carrying both halves. Accept them as separate
        // files, which is how every other tool spells it, and join them here.
        match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let mut pem = read_pem(cert_path, "SSL client certificate")?;
                if !pem.ends_with(b"\n") {
                    pem.push(b'\n');
                }
                pem.extend_from_slice(&read_pem(key_path, "SSL client key")?);
                builder = builder.identity(
                    reqwest::Identity::from_pem(&pem)
                        .map_err(|err| format!("SSL client identity is not usable: {err}"))?,
                );
            }
            (Some(_), None) => {
                return Err("SSL client certificate needs a matching client key.".to_string())
            }
            (None, Some(_)) => {
                return Err("SSL client key needs a matching client certificate.".to_string())
            }
            (None, None) => {}
        }

        if self.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        builder
            .build()
            .map_err(|err| format!("TLS client setup failed: {err}"))
    }
}

fn read_pem(path: &str, label: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|err| format!("{label} at {path} could not be read: {err}"))
}

impl QdrantConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let base_url = option_string(request, &["connectionString", "url", "dsn"])
            .unwrap_or_else(|| build_url(request));
        // The desktop form labels the password field "API key / token" for this
        // engine, so a key typed there arrives as `password`. Resolve it in a
        // second pass rather than appending to the list above: `option_string`
        // scans container-first, and `password` sits in the profile container
        // while an explicit `apiKey` usually sits in `options` — one combined
        // list would let a stale password shadow the explicit option.
        let api_key = option_string(request, &["apiKey", "api_key"])
            .or_else(|| option_string(request, &["password"]));
        let bearer_token = option_string(request, &["token", "bearerToken", "accessToken"]);
        let tls = TlsConfig::from_request(request);
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, api_key.as_deref());
        push_sensitive(&mut redaction_values, bearer_token.as_deref());
        collect_url_auth(&base_url, &mut redaction_values);
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            bearer_token,
            tls,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.base_url, "<qdrant-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

struct QdrantQuery {
    collection: String,
    body: Value,
}

impl QdrantQuery {
    fn from_input(input: &str, request: &Value, cap: usize) -> Result<Self, String> {
        let input = input.trim();
        let mut collection = option_string(request, &["collection", "collectionName"]);
        let mut body = json!({
            "limit": cap,
            "with_payload": true,
            "with_vector": false
        });
        if input.starts_with('{') {
            let value: Value = serde_json::from_str(input)
                .map_err(|err| format!("invalid Qdrant query JSON: {err}"))?;
            collection = value
                .get("collection")
                .or_else(|| value.get("collectionName"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or(collection);
            body = value
                .get("body")
                .cloned()
                .unwrap_or_else(|| scroll_body_from_query(&value, cap));
        } else if !input.is_empty() {
            collection = Some(input.to_string());
        }
        let collection = collection.ok_or("Qdrant query needs a collection name.")?;
        Ok(Self { collection, body })
    }
}

fn scroll_body_from_query(value: &Value, cap: usize) -> Value {
    let mut body = Map::from_iter([
        ("limit".to_string(), json!(cap)),
        ("with_payload".to_string(), json!(true)),
        ("with_vector".to_string(), json!(false)),
    ]);
    if let Some(filter) = value.get("filter") {
        body.insert("filter".to_string(), filter.clone());
    }
    if let Some(offset) = value.get("offset") {
        body.insert("offset".to_string(), offset.clone());
    }
    if let Some(with_vector) = value.get("with_vector").or_else(|| value.get("withVector")) {
        body.insert("with_vector".to_string(), with_vector.clone());
    }
    if let Some(with_payload) = value
        .get("with_payload")
        .or_else(|| value.get("withPayload"))
    {
        body.insert("with_payload".to_string(), with_payload.clone());
    }
    Value::Object(body)
}

async fn load_version(connection: &QdrantConnection) -> Result<Option<String>, String> {
    let response = connection
        .auth(connection.client.get(&connection.config.base_url))
        .send()
        .await
        .map_err(|err| format!("Qdrant root request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Qdrant response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "Qdrant root request returned HTTP {status}: {text}"
        ));
    }
    let value = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
    Ok(value
        .get("version")
        .and_then(Value::as_str)
        .map(|version| format!("Qdrant {version}")))
}

async fn run_scroll(
    connection: &QdrantConnection,
    query: QdrantQuery,
) -> Result<QueryOutput, String> {
    let response = connection
        .auth(connection.client.post(format!(
            "{}/collections/{}/points/scroll",
            connection.config.base_url,
            url_component(&query.collection)
        )))
        .json(&query.body)
        .send()
        .await
        .map_err(|err| format!("Qdrant scroll request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Qdrant scroll response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("Qdrant scroll returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("Qdrant scroll JSON parse failed: {err}: {text}"))?;
    Ok(points_to_output(value))
}

async fn load_metadata(connection: &QdrantConnection) -> Result<Value, String> {
    let response = connection
        .auth(
            connection
                .client
                .get(format!("{}/collections", connection.config.base_url)),
        )
        .send()
        .await
        .map_err(|err| format!("Qdrant collections request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Qdrant collections response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("Qdrant collections returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
    let collections = value
        .get("result")
        .and_then(|result| result.get("collections"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut objects = Vec::new();
    for collection in collections {
        let Some(name) = collection.get("name").and_then(Value::as_str) else {
            continue;
        };
        let detail = load_collection_detail(connection, name)
            .await
            .unwrap_or(Value::Null);
        objects.push(json!({
            "schema": "default",
            "name": name,
            "kind": "collection",
            "columns": [
                {"name": "id", "dataType": "point_id", "nullable": false, "ordinal": 1},
                {"name": "payload", "dataType": "json", "nullable": true, "ordinal": 2},
                {"name": "vector", "dataType": "vector", "nullable": true, "ordinal": 3}
            ],
            "indexes": [],
            "primaryKey": [{"name": "id", "keyType": "primary"}],
            "foreignKeys": [],
            "details": detail
        }));
    }
    Ok(json!({ "schemas": [{ "name": "default", "objects": objects }] }))
}

async fn load_collection_detail(
    connection: &QdrantConnection,
    name: &str,
) -> Result<Value, String> {
    let response = connection
        .auth(connection.client.get(format!(
            "{}/collections/{}",
            connection.config.base_url,
            url_component(name)
        )))
        .send()
        .await
        .map_err(|err| format!("Qdrant collection detail request failed: {err}"))?;
    response
        .json::<Value>()
        .await
        .map_err(|err| format!("Qdrant collection detail JSON failed: {err}"))
}

fn points_to_output(value: Value) -> QueryOutput {
    let points = value
        .get("result")
        .and_then(|result| result.get("points"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let next_page_offset = value
        .get("result")
        .and_then(|result| result.get("next_page_offset"))
        .cloned()
        .unwrap_or(Value::Null);
    let rows = points
        .into_iter()
        .map(|point| {
            vec![
                point.get("id").cloned().unwrap_or(Value::Null),
                point.get("payload").cloned().unwrap_or(Value::Null),
                point.get("vector").cloned().unwrap_or(Value::Null),
            ]
        })
        .collect::<Vec<_>>();
    (
        vec![
            "id".to_string(),
            "payload".to_string(),
            "vector".to_string(),
        ],
        rows,
        !next_page_offset.is_null(),
    )
}

fn build_url(request: &Value) -> String {
    let host = option_string(request, &["host", "endpoint"]).unwrap_or_else(|| "127.0.0.1".into());
    let port = option_string(request, &["port"]).unwrap_or_else(|| "6333".into());
    let scheme = if option_bool(request, &["tls", "ssl"]).unwrap_or(false) {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{host}:{port}")
}

fn connection(connection_id: &str) -> Result<QdrantConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn url_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_query_input() {
        let query = QdrantQuery::from_input(
            r#"{"collection":"docs","filter":{"must":[]}}"#,
            &json!({}),
            20,
        )
        .unwrap();
        assert_eq!(query.collection, "docs");
        assert_eq!(query.body["limit"], 20);
        assert!(query.body.get("filter").is_some());
    }

    #[test]
    fn maps_points_to_rows() {
        let (columns, rows, truncated) = points_to_output(json!({
            "result": {
                "points": [{"id": 1, "payload": {"title": "a"}, "vector": [0.1]}],
                "next_page_offset": 2
            }
        }));
        assert_eq!(columns, vec!["id", "payload", "vector"]);
        assert_eq!(rows[0][0], json!(1));
        assert!(truncated);
    }

    #[test]
    fn builds_url_from_profile() {
        let request = json!({"profile": {"host": "qdrant.local", "port": 6443, "tls": true}});
        let config = QdrantConfig::from_request(&request).unwrap();
        assert_eq!(config.base_url, "https://qdrant.local:6443");
    }

    #[test]
    fn takes_the_api_key_from_the_password_field() {
        // The connection form labels `password` "API key / token" for qdrant,
        // so this is the shape a profile filled in through the UI arrives as.
        let config = QdrantConfig::from_request(&json!({
            "profile": { "host": "qdrant.local", "password": "key_from_the_form" }
        }))
        .unwrap();
        assert_eq!(config.api_key.as_deref(), Some("key_from_the_form"));
    }

    #[test]
    fn explicit_api_key_option_wins_over_password() {
        let config = QdrantConfig::from_request(&json!({
            "profile": {
                "host": "qdrant.local",
                "password": "stale",
                "options": { "apiKey": "explicit" }
            }
        }))
        .unwrap();
        assert_eq!(config.api_key.as_deref(), Some("explicit"));
    }

    #[test]
    fn redacts_an_api_key_taken_from_the_password_field() {
        let config = QdrantConfig::from_request(&json!({
            "profile": { "host": "qdrant.local", "password": "key_secret" }
        }))
        .unwrap();
        assert_eq!(
            config.redact("rejected key key_secret"),
            "rejected key ****"
        );
    }

    #[test]
    fn reads_tls_paths_from_the_connector_options() {
        let tls = TlsConfig::from_request(&json!({
            "profile": {
                "options": {
                    "sslRootCert": "/etc/ssl/ca.pem",
                    "sslCert": "/etc/ssl/client.pem",
                    "sslKey": "/etc/ssl/client.key"
                }
            }
        }));
        assert_eq!(tls.root_cert_path.as_deref(), Some("/etc/ssl/ca.pem"));
        assert_eq!(tls.client_cert_path.as_deref(), Some("/etc/ssl/client.pem"));
        assert_eq!(tls.client_key_path.as_deref(), Some("/etc/ssl/client.key"));
        assert!(!tls.accept_invalid_certs);
    }

    #[test]
    fn accepts_the_driver_spellings_of_the_tls_options() {
        let tls = TlsConfig::from_request(&json!({
            "profile": { "options": { "sslrootcert": "/ca.pem", "ssl-cert": "/c.pem" } }
        }));
        assert_eq!(tls.root_cert_path.as_deref(), Some("/ca.pem"));
        assert_eq!(tls.client_cert_path.as_deref(), Some("/c.pem"));
    }

    #[test]
    fn a_profile_without_tls_options_keeps_the_plain_client() {
        let tls = TlsConfig::from_request(&json!({ "profile": {} }));
        assert_eq!(tls, TlsConfig::default());
        assert!(tls.build_client().is_ok());
    }

    #[test]
    fn half_a_client_identity_is_rejected_with_a_usable_message() {
        // Silently ignoring the half that was supplied would connect without
        // the certificate the user asked for.
        let cert_only = TlsConfig {
            client_cert_path: Some("/etc/ssl/client.pem".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            cert_only.build_client().unwrap_err(),
            "SSL client certificate needs a matching client key."
        );

        let key_only = TlsConfig {
            client_key_path: Some("/etc/ssl/client.key".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            key_only.build_client().unwrap_err(),
            "SSL client key needs a matching client certificate."
        );
    }

    #[test]
    fn an_unreadable_certificate_names_the_file_and_the_field() {
        let tls = TlsConfig {
            root_cert_path: Some("/definitely/not/here.pem".into()),
            ..TlsConfig::default()
        };
        let err = tls.build_client().unwrap_err();
        assert!(
            err.starts_with("SSL root certificate at /definitely/not/here.pem"),
            "{err}"
        );
    }

    #[test]
    fn a_certificate_file_with_no_pem_block_is_rejected() {
        // reqwest answers Ok(vec![]) rather than an error for a file with no
        // PEM blocks, so without an explicit emptiness check this connection
        // would silently verify against the system roots instead of the named
        // CA.
        let dir = std::env::temp_dir().join("irodori-qdrant-tls-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("not-a-cert.pem");
        std::fs::write(&path, b"this is not a certificate").unwrap();

        let tls = TlsConfig {
            root_cert_path: Some(path.to_string_lossy().into_owned()),
            ..TlsConfig::default()
        };
        let err = tls.build_client().unwrap_err();
        assert!(err.contains("contains no PEM certificate"), "{err}");

        std::fs::remove_file(&path).ok();
    }
}
