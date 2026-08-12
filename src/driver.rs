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
    let connection = QdrantConnection {
        client: Client::new(),
        config,
    };
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
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, api_key.as_deref());
        push_sensitive(&mut redaction_values, bearer_token.as_deref());
        collect_url_auth(&base_url, &mut redaction_values);
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            bearer_token,
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
    let scheme = if bool_option(request, &["tls", "ssl"]).unwrap_or(false) {
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

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn bool_option(request: &Value, fields: &[&str]) -> Option<bool> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| container.get(*field).and_then(Value::as_bool))
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

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

fn collect_url_auth(url: &str, values: &mut Vec<String>) {
    let Some(after_scheme) = url.split_once("://").map(|(_, rest)| rest) else {
        return;
    };
    let Some(auth) = after_scheme
        .split('/')
        .next()
        .and_then(|host| host.split('@').next())
    else {
        return;
    };
    if auth.contains(':') {
        for part in auth.split(':') {
            push_sensitive(values, Some(part));
        }
    }
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
}
