<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# Qdrant Connector

Native Irodori Table connector extension for Qdrant.

This crate packages the connector metadata, native ABI exports, and driver implementation used by the Irodori extension marketplace.

## Connector

- Extension ID: `irodori.qdrant`
- Engine ID: `qdrant`
- Wire protocol: `qdrant`
- Default port: `6333`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `yes`
- Marketplace visibility: `public`
- Package version: `0.1.7`

The package uses the connector metadata and native driver directly; no desktop adapter source snapshot is required.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust crate exports the native ABI from `src/lib.rs`, uses `irodori-connector-abi` for shared JSON/buffer helpers, and keeps connector behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `hostPort`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `yes`
- TLS required by default: `no`
- Custom driver options: `yes`

### Endpoint Fields

| Field | Label | Type | Required |
| --- | --- | --- | --- |
| `host` | Host | `string` | yes |
| `protocol` | Protocol (REST or gRPC) | `string` | yes |
| `port` | Port | `number` | no |
| `database` | Database | `string` | no |

## Authentication

The connector advertises these authentication modes so clients can render the right credential fields. Driver-specific or provider-specific values can still be passed through `options` when needed.

| Auth method | Label | Kind | Secret purposes |
| --- | --- | --- | --- |
| `none` | No authentication | `none` | none |
| `connectionString` | Connection string / DSN | `connectionString` | none |
| `apiKey` | API key | `apiKey` | `token` |
| `bearerToken` | Bearer token | `token` | `token` |
| `clientCertificate` | Client certificate / mTLS | `certificate` | `privateKey`, `privateKeyPassphrase` |
| `customDriverOptions` | Custom driver options | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## Experience Metadata

- Domains: `vector`
- Result views: `vectorNeighbors`, `table`, `json`
- Object types: `collections`, `indexes`, `vectors`, `payloadFields`, `partitions`, `namespaces`
- Inspired by: Qdrant Collections, Qdrant filtering, Qdrant payload indexes

| Workflow | Result view | Templates |
| --- | --- | --- |
| Similarity search | `vectorNeighbors` | `vector-similarity` |
| Filtered ANN search | `vectorNeighbors` | `vector-filtered` |
| Collection or index health | `table` | `vector-health` |

| Template | Label | Language | Result view |
| --- | --- | --- | --- |
| `vector-similarity` | Qdrant similarity search | `json` | `vectorNeighbors` |
| `vector-filtered` | Qdrant filtered search | `json` | `vectorNeighbors` |
| `vector-health` | Qdrant collection info | `text` | `json` |

## Native ABI Calls

| Method | Response |
| --- | --- |
| `health` | Returns connector health, engine id, ABI version, and driver status. |
| `describe` | Returns the embedded manifest and connector config. |
| `manifest` | Returns raw `irodori.extension.json`. |
| `config` | Returns raw `connector.config.json`. |
| `connect` | Opens and validates a native connector connection. |
| `query` | Runs a connector query and returns structured rows or JSON results. |
| `metadata` | Reads schemas, tables, columns, indexes, collections, or equivalent metadata. |
| `close` | Closes and removes a cached native connection. |

## Development

All extension crates in this checkout share `../target` so dependencies compile once across sibling repositories.

```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.

## License

0BSD. You can use, copy, modify, and distribute this project for almost any purpose.
