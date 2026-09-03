# Logma Pub/Sub resource model

This document describes the current Fatline v2 Logma Pub/Sub control-plane model. Redis is the first storage/transport provider; Redis syntax is not the domain model.

## Domain resources

```text
Channel
  durable Logma listening resource
  may exist with zero Subscribers

Callback
  independently addressable action
  webhook -> one or many HTTP(S) targets
  lua     -> function name

Subscriber
  durable attachment to one Channel
  references one or more Callbacks

Publisher
  producer binding to one Channel
  producer-specific config remains opaque to Logma

SubscriptionGroup
  durable grouping of zero or more Subscribers

ServerlessEndpoint
  requester-driven surface such as SSE
  not a standing Subscriber
```

The important distinction is:

```text
Logma Channel resource != Redis Pub/Sub transport topic
Subscriber               != Channel
ServerlessEndpoint        != standing Subscriber
```

## Canonical identity and Redis addresses

`FATLINE_SCOPE` is an explicit, non-empty, pattern-safe security boundary. New v2 code parses it with `keyspace.ParseScope`; it does not synthesize an `unknown` scope.

Domain identities are canonicalized once by trimming outer whitespace, then encoded injectively when interpolated into Redis key segments. Structural grammar remains package-owned and unescaped. For example:

```text
logical identity: market:oil
encoded segment:  market%3Aoil

logical identity: request 123
encoded segment:  request%20123
```

The encoding prevents opaque identities from colliding with structural children. For example, Channel identity `foo:subscribers` cannot alias the `:subscribers` relationship set for Channel `foo`.

Raw logical identities remain raw values inside HASH fields and SET membership.

The current v2 graph is:

```text
<scope>:logma:pubsub:channel:<id>
<scope>:logma:pubsub:channel:<id>:subscribers
<scope>:logma:pubsub:channel:<id>:publishers

<scope>:logma:pubsub:callback:<id>
<scope>:logma:pubsub:callback:<id>:urls
<scope>:logma:pubsub:callback:<id>:subscribers

<scope>:logma:pubsub:subscriber:<id>
<scope>:logma:pubsub:subscriber:<id>:callbacks
<scope>:logma:pubsub:subscriber:<id>:subscription-groups

<scope>:logma:pubsub:publisher:<id>

<scope>:logma:pubsub:subscription-group:<id>
<scope>:logma:pubsub:subscription-group:<id>:subscribers

<scope>:logma:pubsub:registry:channels
<scope>:logma:pubsub:registry:callbacks
<scope>:logma:pubsub:registry:subscribers
```

`ResourceKeysV2` is the production mapper for `RedisStore` on this branch. There is no legacy-key fallback or dual read/write path.

## Redis-native graph semantics

The resource representation is HASH + SET rather than JSON-per-resource. Discovery uses explicit registries; production graph traversal does not use `SCAN`.

Forward and reverse relationships are maintained together:

```text
Channel -> Subscribers
Channel -> Publishers
Callback -> Subscribers
Subscriber -> Callbacks
Subscriber -> SubscriptionGroups
SubscriptionGroup -> Subscribers
```

Relationship-bearing writes require referenced resources to exist. Deletion of a referenced Channel, Callback, or Subscriber fails with `ErrReferenced` rather than producing a dangling graph.

SubscriptionGroup membership is bidirectional. A Subscriber cannot be deleted while a group references it; updating or deleting a group reconciles each Subscriber's reverse `subscription-groups` set.

Publisher `config` remains uninterpreted by the generic model, but when present it must still be valid JSON because its representation is `json.RawMessage`.

## Optimistic transaction behavior

Graph mutations that reconcile references use Redis `WATCH` plus transactional pipelines. A normal concurrent modification can therefore produce `redis.TxFailedErr` without representing an application error.

The store owns this provider concern: watched mutations use a bounded, context-aware retry policy. Only `redis.TxFailedErr` is retried; validation, graph-policy, Redis/network, and context errors return immediately. Exhausted contention returns an explicit error instead of retrying indefinitely.

Qualification includes a forced conflict from an independent Redis connection and proves that the first EXEC loses the WATCH race while a later bounded retry commits successfully.

## Semantic capability and ACL provider

New v2 authority is expressed semantically and compiled into Redis requirements:

```text
LogmaPubSubGraph(Read | Write)
        -> Redis provider
        -> key patterns + command grants
```

The graph capability is scoped to:

```text
~<scope>:logma:pubsub:*
```

and does not imply Redis Pub/Sub transport, scripting, Function administration, or neighboring Logma runtime authority.

`Write` means semantic permission to perform graph mutations. The Redis provider may therefore include internal read primitives required to perform a correct guarded mutation (`HGET`, `SMEMBERS`, `SCARD`, `EXISTS`, etc.) without granting unrelated higher-level read APIs such as `HGETALL` unless `Read` is also requested.

The real Redis ACL gate proves both combined read/write and write-only principals. It also proves `NOPERM` for:

```text
neighboring <scope>:logma:runtime:* keys
foreign FATLINE_SCOPE keys
PUBLISH transport operations
SUBSCRIBE transport operations
```

## Callbacks and runtime attachment

Webhook callbacks contain one or more normalized HTTP(S) targets. Redis stores target membership as an unordered SET, so durable reconstruction is deterministic by sorting; declaration order is not a delivery contract.

Lua callbacks are valid domain resources but are not yet executable by the current Subscriber runtime. Runtime attachment currently supports webhook Callbacks only.

One active Redis subscription is owned per active Logma Channel. Subscriber handlers multiplex onto that listener. Runtime handles are generation-specific so stale handles cannot deactivate or detach a replacement generation with the same identity. Channel deactivation cancels the activation context used by in-flight webhook requests.

Attached Subscribers snapshot Callback configuration at attach time; durable updates are not silently hot-reloaded.

Webhook fanout is currently synchronous. A slow target can delay later targets/messages. Introducing a bounded delivery dispatcher remains a separate runtime behavior change and should receive its own qualification rather than being folded into storage semantics.

## HTTP adapter boundary

The additive HTTP resource API currently exposes Channel, Callback, and Subscriber operations. JSON bodies are bounded to 1 MiB, reject unknown fields, and accept exactly one JSON value. Missing references and referenced-resource deletion map to HTTP `409`; missing resources map to `404`; storage/configuration failures remain server errors.

Domain identities are not defined by HTTP path syntax. Redis v2 can safely encode characters such as `/`, but the current Chi `{id}` routes have not qualified slash-bearing identities. If that becomes required, change the HTTP adapter (for example by an explicit encoded-ID contract) rather than weakening the domain identity grammar.

## Legacy compatibility boundary

The historical API/runtime still uses flattened keys such as:

```text
active_subscriptions:<subscription-id>:<channel> = <callbackURL>
```

That remains a separate compatibility surface. The v2 branch does not reinterpret or silently migrate those keys.

Likewise, older `serverless/keyspace` helpers (`FromEnv`, `Scope.Name`, `Worker`, `Profile`, and current `NewsProfile`) remain compatibility primitives for already-qualified deployments. They are intentionally not the template for new resource authority because their older normalization can be lossy. New v2 work uses:

```text
ParseScope
Family / Resource
semantic Grant
CompileRedisRequirements
```

## Repair boundaries

Indexes are first-class durable graph state. The store does not use hidden `SCAN` repair logic.

If a resource HASH is externally deleted, an idempotent delete can clean indexes for which enough relationship identity remains locally. It cannot reconstruct unknown historical relationships that were never indexed. If stronger repair semantics become required, add the necessary durable reverse/index resource and qualify it explicitly rather than scanning the keyspace or guessing.

## Current qualification boundary

The current hardened v2 graph is qualified against exact Redis 7.2.5 after a source-only gate of:

```text
go mod tidy + clean go.mod/go.sum
gofmt
go vet ./...
go test ./...
go build ./cmd/api
```

The Redis stage then qualifies canonical addresses, graph/registry behavior, bidirectional group integrity, bounded WATCH retry, generated ACL behavior, write-only mutation authority, negative NOPERM boundaries, and scoped residue cleanup.

This remains local provider/resource evidence. Retained Farcaster provider behavior requires a separate side-by-side qualification before any canonical retained provider cutover.
