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
    webhook -> callbackURL
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

Initial callback types:

```json
{
  "id": "callback-webhook",
  "type": "webhook",
  "webhook": {
    "callbackURL": "https://example.invalid/hook"
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

## Serverless is not a standing Subscriber

The existing Logma serverless `/run` + `/events` model remains requester-driven. It can create SSE or other request-scoped event delivery without becoming a durable active Subscriber resource. This is intentional: endpoint capability and standing Redis subscription state are different resources.

## Publisher ownership

A Publisher resource identifies a producer binding such as `xd-dash/news` or `xd-dash/stonks`. Activating a Publisher ensures its Channel is active before producer startup, but the generic Pub/Sub model does not absorb producer-specific process, credential, or data-source semantics. Those remain with the producer integration/Fatline composition.

## Redis key direction

New persisted resource keys follow the Fatline scope-first grammar rather than extending the historical `active_subscriptions:*` encoding:

```text
<scope>:logma:pubsub:channel:<channel identity>
<scope>:logma:pubsub:callback:<callback id>
<scope>:logma:pubsub:subscriber:<subscriber id>
<scope>:logma:pubsub:publisher:<publisher id>
```

`FATLINE_SCOPE` is part of the materialized runtime security boundary. New resource persistence must not silently copy local fixture scope into a retained deployment.

## Compatibility and migration

Current `main` persists:

```text
active_subscriptions:<subscription-id>:<channel> = <callbackURL>
```

That historical representation combines an active Redis listener and one webhook callback in one record. Migration must be incremental:

1. introduce typed resource contracts and validation;
2. add scoped resource persistence;
3. add Channel activation independent of callbacks;
4. add callback collection API and Subscriber references;
5. adapt legacy `/{channel}/subscribe` as a compatibility operation that creates a webhook Callback plus Subscriber against an active Channel;
6. update subscription groups to store Subscriber/resource identities rather than flattened callback URLs;
7. add Publisher integrations without moving producer-specific semantics into generic Logma Pub/Sub;
8. retain serverless requester endpoints as a separate capability surface;
9. remove historical flattened keys only after exact migration/compatibility qualification.

Do not reinterpret old Redis keys in place without explicit version/migration evidence.
