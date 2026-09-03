# Logma Pub/Sub v2 design

Redis is the first storage/transport provider; Redis syntax is not the Logma operator model.

## Operator model

Ordinary Logma stays close to the historical service:

```text
subscribe a callback to a channel
unsubscribe it
save useful groups
activate or shut down a group
register/reconcile a publisher
inspect declaration state
```

The normalized `/pubsub/*` API remains an advanced control-plane surface. Provider details such as HASH/SET layout, reverse indexes, WATCH retry, encoded provider addresses, Redis commands, worker queues, and runtime tokens stay below the ordinary surface.

## Durable resources

```text
Channel
  durable logical namespace
  may exist with zero Subscribers
  does not imply a live Redis listener

Callback
  independently addressable action
  webhook -> one or many HTTP(S) targets
  lua     -> function name (runtime support remains separate)

Subscriber
  durable attachment to one Channel and one or more Callbacks
  presented as Subscription in the operator surface

Publisher
  durable producer binding to one Channel
  type selects an internal provider adapter

SubscriptionGroup
  weak operational set of Subscriber identities
  members may be absent

PublisherGroup
  advanced-control-plane weak set of Publisher identities
  not part of the ordinary surface until producer-group operations consume it
```

`ServerlessEndpoint` is intentionally absent from the current v2 model. SSE/serverless response behavior is a runtime/surface capability until a concrete durable lifecycle requires its own resource identity.

## Strong relationships

Only validity-defining relationships are strong:

```text
Subscriber -> Channel
Subscriber -> Callback
Publisher  -> Channel
```

Reverse SETs support guarded deletion without SCAN. Groups do not create deletion authority.

## Redis graph

Canonical durable addresses are scope-first and injectively encode opaque identities:

```text
<scope>:logma:pubsub:channel:<id>
<scope>:logma:pubsub:channel:<id>:subscribers
<scope>:logma:pubsub:channel:<id>:publishers

<scope>:logma:pubsub:callback:<id>
<scope>:logma:pubsub:callback:<id>:urls
<scope>:logma:pubsub:callback:<id>:subscribers

<scope>:logma:pubsub:subscriber:<id>
<scope>:logma:pubsub:subscriber:<id>:callbacks

<scope>:logma:pubsub:publisher:<id>

<scope>:logma:pubsub:subscription-group:<id>
<scope>:logma:pubsub:subscription-group:<id>:subscribers

<scope>:logma:pubsub:publisher-group:<id>
<scope>:logma:pubsub:publisher-group:<id>:publishers

<scope>:logma:pubsub:registry:...
```

The provider retains HASH + SET persistence, explicit registries, bounded optimistic transactions, and cheap representation-integrity checks. It does not add background repair/scrubbing machinery while the graph writer remains trusted.

## Atomic `/subscribe`

The convenience operation:

```text
POST /subscribe
```

compiles to:

```text
ensure Channel
create Callback
create Subscriber
```

inside one provider-owned transaction. Callback and Subscriber identities are create-only for this path. Conflicts do not overwrite independently managed resources or leave partial graph state.

## Scope and authority

Transport is scoped separately from durable storage:

```text
logical Channel:
  market:quotes

Redis transport:
  <scope>:logma:transport:channel:market%3Aquotes
```

Semantic provider grants remain separate:

```text
LogmaPubSubGraph(Read | Write)
  -> ~<scope>:logma:pubsub:*

LogmaPubSubTransport(Publish | Subscribe)
  -> &<scope>:logma:transport:channel:*
```

Redis ACL enforces scope plus command/channel families. `RedisStore` enforces graph semantics. A generic graph-write Redis credential therefore belongs to the trusted Logma control service rather than arbitrary Fatline applications.

## Subscription runtime

Durable Channel existence and live listener existence are separate.

Normal runtime lifecycle is:

```text
first active Subscription on Channel
    -> create shared Redis listener
    -> wait for Redis SUBSCRIBE acknowledgement

more active Subscriptions
    -> attach handlers

last active Subscription shuts down
    -> release the Redis listener
```

`ActivateSubscription(id)` means reconcile the current Subscriber/Callback declaration and return success only after initial Redis readiness. A reconnecting goroutine that has never subscribed is not successful activation.

Repeated activation installs a replacement handler before retiring the previous one. Stale handles cannot detach newer generations.

Request context bounds lookup/readiness for one command. Controller/service context owns the successfully established runtime after the request returns.

## Bounded webhook delivery

Redis receive must not block behind slow HTTP callbacks.

The v2 runtime restores the historical Logma dispatcher shape:

```text
Redis receive
    -> bounded queue (256)
    -> fixed workers (4)
    -> webhook requests
```

Queue saturation drops/logs instead of blocking the receive loop or growing memory without bound. This remains weak/ephemeral signaling semantics; NQC owns repair/anti-entropy when correctness requires more than best-effort Pub/Sub delivery.

## Publisher runtime

Generic Publisher reconciliation is deliberately smaller:

```text
load Publisher
-> validate referenced durable Channel exists
-> resolve Publisher.Type provider
-> PublisherProvider.EnsureActive(service-owned context)
```

Generic Logma no longer starts an empty consumer listener before a producer. An empty listener does not provide delivery durability. Concrete providers own any stronger producer/consumer readiness requirement they actually need.

## Groups

Groups are weak control sets resolved once at execution time:

```text
Group identities
-> SubscriptionController
-> completed / missing / failed
```

The façade does not preflight and then re-read members in the runtime. `ErrNotFound` maps to `missing`; other execution errors map to `failed`.

## Runtime ownership and restart

Activation is process-local and ephemeral. Durable declarations are not automatically desired runtime state.

Initial production contract:

```text
one Logma runtime owner per FATLINE_SCOPE
```

or equivalent request routing to that owner. Distributed leases/leader election are deferred until multiple runtime owners per scope become a demonstrated requirement.

After restart, Fatline profile/bootstrap intent may activate named Groups. The resource model does not add `desiredActive` to every Subscriber merely to reconstruct runtime state.

## `/state`

`GET /state` is declaration inventory, not runtime observation. Ordinary state should expose ordinary nouns only:

```text
channels
subscriptions
publishers
groups
```

Advanced-only PublisherGroup remains under `/pubsub/*` until an operator workflow uses it.

## Package composition

```text
Logma       ephemeral signaling/fanout
NQC         cache coherence and anti-entropy repair
ratelimiter rate/lifecycle enforcement
Marai       narrow crypto authority
Redis       shared mechanical substrate
Fatline     capability/profile/runtime composition
Huram       exact-source qualification and deployment evidence
```

The next composition boundary is one explicit Fatline-owned `LogmaService` (or equivalent) that owns graph and transport Redis clients separately, constructs the Subscription runtime/controller and Publisher registry/reconciler, exposes operator and advanced routers, and closes all owned runtime workers/listeners cleanly.

## Design pressure rule

When an abstraction repeatedly requires new lifecycle/concurrency guards, first ask whether the abstraction should exist. This review removed standalone empty Channel runtime semantics from ordinary control and removed speculative `ServerlessEndpoint` resource semantics rather than continuing to harden them.
