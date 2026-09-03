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
  durable producer binding to one Channel
  producer type selects a runtime adapter
  producer-specific config remains opaque to Logma

SubscriptionGroup
  durable grouping of zero or more Subscribers

PublisherGroup
  durable grouping of zero or more Publishers
  does not imply a shared Channel, provider type, or transport credential

ServerlessEndpoint
  requester-driven surface such as SSE
  not a standing Subscriber
```

The important distinctions are:

```text
Logma Channel resource != Redis Pub/Sub transport topic
Subscriber               != Channel
Publisher graph record   != active producer process/runtime
ServerlessEndpoint        != standing Subscriber
```

## Canonical identity and Redis addresses

`FATLINE_SCOPE` is explicit, non-empty, and pattern-safe. New v2 code parses it fail-closed rather than synthesizing a scope such as `unknown`.

Domain identities are canonicalized once by trimming outer whitespace, then encoded injectively when interpolated into Redis key segments. Structural grammar remains package-owned. Raw logical identities remain raw values in HASH fields and SET membership.

The current graph is:

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
<scope>:logma:pubsub:publisher:<id>:publisher-groups

<scope>:logma:pubsub:subscription-group:<id>
<scope>:logma:pubsub:subscription-group:<id>:subscribers

<scope>:logma:pubsub:publisher-group:<id>
<scope>:logma:pubsub:publisher-group:<id>:publishers

<scope>:logma:pubsub:registry:channels
<scope>:logma:pubsub:registry:callbacks
<scope>:logma:pubsub:registry:subscribers
<scope>:logma:pubsub:registry:publishers
<scope>:logma:pubsub:registry:subscription-groups
<scope>:logma:pubsub:registry:publisher-groups
```

`ResourceKeysV2` is the production mapper for `RedisStore` on this branch. There is no legacy-key fallback or dual read/write path.

## Redis-native graph semantics

The representation is HASH + SET rather than JSON-per-resource. Collection discovery uses explicit registries; production graph traversal does not use `SCAN`.

Strong relationships are maintained forward and reverse:

```text
Channel <- Subscriber
Channel <- Publisher
Callback <- Subscriber
Subscriber <- SubscriptionGroup
Publisher <- PublisherGroup
```

Relationship-bearing writes require referenced resources to exist. Deletion of a referenced Channel, Callback, Subscriber, or Publisher returns `ErrReferenced` rather than producing a dangling graph. Updating or deleting either group type reconciles its members' reverse membership sets transactionally.

Publisher `config` is uninterpreted by the generic model, but non-empty config must be valid JSON because its representation is `json.RawMessage`.

## Publisher reconciliation

Persistence and runtime activation are deliberately separate operations:

```text
Publisher resource
      -> PublisherReconciler
      -> load referenced Channel
      -> ensure Channel runtime is active
      -> resolve Publisher.Type in PublisherRegistry
      -> PublisherProvider.EnsureActive(...)
```

`PublisherProvider` is the adapter boundary for producer implementations such as `stonks`, `news`, Unix/socket producers, SSE-oriented producers, or other Fatline-native integrations. Logma does not hardcode those producer types. Provider registration rejects duplicate type ownership, and `EnsureActive` is expected to be idempotent for a stable Publisher identity/configuration.

Channel activation is an injected dependency because the graph-store credential intentionally does not imply Redis `SUBSCRIBE` authority. A composition must supply a transport-authorized Channel runtime separately. The default resource router therefore persists Publishers but does not manufacture runtime authority; its reconcile endpoint returns `503` when no reconciler has been explicitly configured.

## Semantic capability and ACL provider

New v2 authority is expressed semantically and compiled into Redis requirements:

```text
LogmaPubSubGraph(Read | Write)
        -> Redis provider
        -> key patterns + command grants
```

The graph capability is scoped to `~<scope>:logma:pubsub:*` and does not imply Redis Pub/Sub transport, scripting, Function administration, or neighboring Logma runtime authority. Semantic `Write` includes provider-internal read primitives required for safe guarded mutations without granting unrelated independent read surfaces.

Real Redis ACL qualification proves combined read/write and write-only graph principals and verifies `NOPERM` for neighboring `logma:runtime`, a foreign `FATLINE_SCOPE`, `PUBLISH`, and `SUBSCRIBE`.

## Runtime attachment semantics

One active Redis subscription is owned per active Logma Channel. Subscriber handlers multiplex onto that listener. Runtime handles are generation-specific so stale handles cannot deactivate or detach a replacement generation with the same identity. Channel deactivation cancels the activation context used by in-flight webhook requests.

Attached Subscribers snapshot Callback configuration at attach time. Webhook fanout remains synchronous; moving delivery onto a bounded dispatcher is a separate runtime behavior change.

Publisher reconciliation uses that Channel runtime only through the narrow Channel-activation interface; producer startup remains owned by the registered PublisherProvider.

## HTTP resource surface

The additive resource API now exposes consistent collection/create/read/delete operations for:

```text
GET/POST /pubsub/channels
GET/POST /pubsub/callbacks
GET/POST /pubsub/subscribers
GET/POST /pubsub/publishers
GET/POST /pubsub/subscription-groups
GET/POST /pubsub/publisher-groups
```

Each family also has `GET` and `DELETE` by identity. Publisher additionally exposes:

```text
POST /pubsub/publishers/{id}/reconcile
```

The default router returns `503` for reconciliation until a composition injects a PublisherReconciler. A successful configured reconcile returns `204`.

JSON bodies are bounded to 1 MiB, reject unknown fields, and accept exactly one JSON value. Missing references and referenced-resource deletion map to `409`; missing resources map to `404`; storage/configuration failures remain server errors.

Domain identity is not defined by HTTP path syntax. Redis v2 can encode characters such as `/`, while ordinary Chi path parameters have not yet qualified arbitrary slash-bearing identities. Fix that at the HTTP adapter layer if needed rather than narrowing the domain identity grammar.

## Optimistic transaction behavior

Graph mutations that reconcile references use Redis `WATCH` plus transactional pipelines. The store owns bounded, context-aware retry of `redis.TxFailedErr` only. Validation, graph-policy, Redis/network, ACL, and context errors return immediately; persistent contention surfaces after the retry bound.

Qualification forces a real WATCH conflict from an independent Redis connection and proves a later bounded retry commits.

## Legacy and repair boundaries

Historical flattened `active_subscriptions:*` state remains a separate compatibility surface. The v2 branch does not reinterpret or silently migrate it. Older lossy `serverless/keyspace` helpers likewise remain compatibility primitives rather than templates for new v2 authority.

Indexes are first-class durable graph state. The store does not use hidden `SCAN` repair logic. If stronger repair becomes necessary, add the required explicit durable index and qualify it rather than scanning or guessing historical relationships.

## Current qualification boundary

The current v2 graph is qualified against Redis 7.2.5 after a source-only gate of:

```text
go mod tidy + clean go.mod/go.sum
gofmt
go vet ./...
go test ./...
go build ./cmd/api
```

The Redis stage qualifies canonical addresses, all resource registries, Subscriber/SubscriptionGroup and Publisher/PublisherGroup strong-reference integrity, bounded WATCH retry, generated ACL behavior, write-only mutation authority, negative NOPERM boundaries, and scoped residue cleanup.

Publisher runtime tests separately prove Channel activation occurs before producer-provider activation, active Channels are reused, duplicate provider type registration is rejected, and HTTP reconciliation requires explicit composition.

This remains local provider/resource evidence. Retained Farcaster provider behavior requires a separate side-by-side qualification before any canonical retained provider cutover.
