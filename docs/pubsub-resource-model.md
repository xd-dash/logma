# Logma Pub/Sub resource model

This document defines the target Logma abstraction over Redis Pub/Sub. Redis remains the transport substrate; these resources are Logma control-plane semantics and intentionally do not mirror Redis one-for-one.

## Resource boundaries

```text
Channel
  active Logma Redis listener
  may exist with zero callbacks/subscribers

Callback
  independently addressable delivery/action resource
  typed configuration
    webhook -> one or many callback URLs
    lua     -> function name
    ...future callback types

Subscriber
  durable attachment to one Channel
  references one or more Callback resources
  invalid with zero callbacks

Publisher
  producer binding for one Channel
  activation ensures the Channel exists before producer startup
  producer-specific startup/config remains owned by the producer integration

SubscriptionGroup
  durable group metadata
  references zero or more Subscriber resources

ServerlessEndpoint
  requester-driven delivery surface such as SSE
  exists independently of active Subscriber resources
  may create request-scoped Redis subscriptions/event responses
```

The critical distinction is:

```text
active Channel != Subscriber
Subscriber = active Channel attachment + callback references
```

An empty active Channel is therefore valid. This deliberately diverges from exposing Redis's own ephemeral channel/subscriber model directly.

## Callback resources

Callbacks are resources rather than fields embedded in a subscription. A Subscriber may reference one or many callbacks, and the same callback may be reused by multiple Subscribers when policy permits.

A webhook Callback preserves Logma's historical fanout behavior: one webhook resource may contain either the single-target `callbackURL` compatibility form, the multi-target `callbackURLs` form, or both. Dispatch targets are the normalized non-empty union in declaration order. Each target must be a syntactically valid absolute `http` or `https` URL; target reachability and network policy remain runtime concerns.

Single-target example:

```json
{
  "id": "callback-webhook",
  "type": "webhook",
  "webhook": {
    "callbackURL": "https://example.invalid/hook"
  }
}
```

Multi-target example:

```json
{
  "id": "callback-webhook-many",
  "type": "webhook",
  "webhook": {
    "callbackURLs": [
      "https://one.example.invalid/hook",
      "https://two.example.invalid/hook"
    ]
  }
}
```

```json
{
  "id": "callback-lua",
  "type": "lua",
  "lua": {
    "name": "logma_on_message"
  }
}
```

A callback type owns its own configuration schema. Generic Subscriber state stores callback identities rather than flattening every callback type into subscriber fields.

The historical versioned Pub/Sub experiment accepted repeated callback query parameters, `callbackURL` as either a string or a list, `callbackURLs` as a list, and callback schemes containing both a single URL and URL list. The new resource model must not regress the semantic capability to fan out one webhook callback to multiple HTTP targets even if the eventual public API chooses a cleaner canonical request shape.

## Serverless is not a standing Subscriber

The existing Logma serverless `/run` + `/events` model remains requester-driven. It can create SSE or other request-scoped event delivery without becoming a durable active Subscriber resource. This is intentional: endpoint capability and standing Redis subscription state are different resources.

Serverless endpoints are therefore not persisted in the new Redis resource graph yet. If durable endpoint registration becomes useful later, it can be added as its own resource without pretending that request-scoped SSE delivery is a standing Subscriber.

## Publisher ownership

A Publisher resource identifies a producer binding such as `xd-dash/news` or `xd-dash/stonks`. Activating a Publisher ensures its Channel is active before producer startup, but the generic Pub/Sub model does not absorb producer-specific process, credential, or data-source semantics. Those remain with the producer integration/Fatline composition.

Producer-specific `config` may remain an opaque value inside the Publisher HASH because Logma does not interpret it. This does not make the Publisher a JSON document; the resource identity and graph relationships remain Redis-native.

## Redis-native persistence

The REST representation and Redis representation are intentionally independent:

```text
JSON request/response
        ↓
  Logma resource model
        ↓
Redis HASH + SET graph
```

New persisted resource keys follow the Fatline scope-first grammar rather than extending the historical `active_subscriptions:*` encoding.

Resource identities remain raw logical values in HASH fields and SET membership. Only identity segments interpolated into Redis key names are escaped: `%` becomes `%25` and `:` becomes `%3A`. This prevents legal colon-rich identities such as `global:events` or suffix-looking identities such as `foo:subscribers` from aliasing graph-index keys. The security scope and fixed grammar segments are not decoded from resource identities.

### Channel

```text
HASH <scope>:logma:pubsub:channel:<escaped-channel>
SET  <scope>:logma:pubsub:channel:<escaped-channel>:subscribers
SET  <scope>:logma:pubsub:channel:<escaped-channel>:publishers
SET  <scope>:logma:pubsub:channels
```

The `channels` SET contains raw Channel identities and is the scan-free discovery index.

### Callback

```text
HASH <scope>:logma:pubsub:callback:<escaped-callback-id>
SET  <scope>:logma:pubsub:callback:<escaped-callback-id>:subscribers
SET  <scope>:logma:pubsub:callback:<escaped-callback-id>:urls   # webhook only
SET  <scope>:logma:pubsub:callbacks
```

Webhook URL membership is a SET because fanout order is not part of delivery correctness. The typed HTTP model may preserve declaration order at its boundary, but durable Redis storage treats destinations as unordered identities and removes duplicates. The `callbacks` SET contains raw Callback identities for scan-free discovery.

### Subscriber

```text
HASH <scope>:logma:pubsub:subscriber:<escaped-subscriber-id>
SET  <scope>:logma:pubsub:subscriber:<escaped-subscriber-id>:callbacks
SET  <scope>:logma:pubsub:subscribers
```

The Subscriber HASH stores its Channel reference. The Channel reverse `:subscribers` SET and each Callback reverse `:subscribers` SET are maintained with the forward callback membership so graph traversal does not require `SCAN + GET + decode`. The `subscribers` SET contains raw Subscriber identities for scan-free discovery.

### Publisher

```text
HASH <scope>:logma:pubsub:publisher:<escaped-publisher-id>
```

The Publisher HASH stores its Channel reference. The Channel reverse `:publishers` SET is maintained transactionally with it.

### SubscriptionGroup

```text
HASH <scope>:logma:pubsub:group:<escaped-group-id>
SET  <scope>:logma:pubsub:group:<escaped-group-id>:subscribers
```

An empty group is valid. Membership points at Subscriber resources rather than flattening channel/callback configuration into the group.

`FATLINE_SCOPE` is part of the materialized runtime security boundary. New resource persistence requires an explicit non-empty scope and must not silently copy local fixture scope into a retained deployment.

## Graph consistency

Subscriber and Publisher writes use optimistic Redis transactions so their forward references and reverse indexes change together. Referenced Channel, Callback, or Subscriber resources must already exist before relationship-bearing resources are stored.

Missing graph dependencies and attempts to delete still-referenced resources are typed graph-policy conflicts. The HTTP resource surface maps those conflicts to `409`; Redis/network/storage failures remain server errors rather than being mislabeled as graph conflicts. Resource POST responses are reconstructed from persisted state so trimming, deduplication, and unordered Redis SET semantics are reflected canonically at the boundary. JSON resource requests are bounded, reject unknown fields, and accept exactly one JSON value.

The storage layer deliberately does not use key scans as its relational mechanism. Reverse indexes and resource-kind registries are first-class graph indexes:

```text
channel -> subscribers
channel -> publishers
callback -> subscribers
subscriber -> callbacks
channels registry -> Channel identities
callbacks registry -> Callback identities
subscribers registry -> Subscriber identities
```

A later Redis Function layer may move these mutations behind narrowly scoped `FCALL` operations when we want invariant enforcement entirely server-side. The HASH/SET representation is compatible with that direction and does not require changing the public resource model.

## Runtime attachment semantics

One active Redis listener is owned per persisted Channel. Subscriber handlers are multiplexed onto that listener. Runtime handles are generation-specific: a stale Channel or Subscriber handle cannot deactivate or detach a later replacement with the same identity. Deactivating a Channel cancels the activation context used for in-flight webhook delivery. Durable Subscriber and Callback resources remain intact when a runtime attachment is detached.

Runtime registry descriptors and record enumeration are deterministic even though their underlying sources include Go maps and Redis SETs. This makes stored snapshots and control-plane responses stable without assigning semantic delivery order to Redis SET membership.

Callback/Subscriber resource changes are still explicit runtime operations: an already attached Subscriber uses the callback snapshot resolved at attachment time and is not silently hot-reloaded from Redis.

## Compatibility and migration

Current `main` persists:

```text
active_subscriptions:<subscription-id>:<channel> = <callbackURL>
```

That current representation combines an active Redis listener and one webhook callback in one record. An earlier versioned Pub/Sub branch had already generalized callback request/config parsing to multiple HTTP targets, so the target resource model preserves that fanout capability while separating callback identity from subscriber identity.

Migration remains incremental:

1. introduce typed resource contracts and validation;
2. add scoped Redis-native HASH/SET resource persistence;
3. add Channel activation independent of callbacks;
4. add callback collection API and Subscriber references, including multi-target webhook fanout;
5. adapt legacy `/{channel}/subscribe` as a compatibility operation that creates a webhook Callback plus Subscriber against an active Channel;
6. update subscription groups to store Subscriber/resource identities rather than flattened callback URLs;
7. add Publisher integrations without moving producer-specific semantics into generic Logma Pub/Sub;
8. retain serverless requester endpoints as a separate capability surface;
9. remove historical flattened keys only after exact migration/compatibility qualification.

Do not reinterpret old Redis keys in place without explicit version/migration evidence.
