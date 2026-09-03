# Logma Pub/Sub v2

Redis is the first provider. Redis syntax is not the Logma operator model.

## Operator surface

Ordinary Logma stays small:

```text
subscribe
unsubscribe
group
activate
shutdown
state
publisher registration/reconciliation
```

`/pubsub/*` is the normalized advanced control-plane surface. Provider details such as HASH/SET layout, reverse indexes, WATCH retry, encoded addresses, Redis commands, worker queues, and runtime tokens stay below the ordinary surface.

## Durable model

```text
Channel
  logical namespace; may exist empty

Callback
  webhook: one or more HTTP(S) targets
  lua: function name; runtime support is separate

Subscriber
  one Channel + one or more Callbacks
  presented as Subscription to operators

Publisher
  producer declaration bound to one Channel
  Type selects an internal provider adapter

SubscriptionGroup
  weak operational set of Subscriber identities

PublisherGroup
  advanced-only weak set of Publisher identities
```

`ServerlessEndpoint` is not a v2 durable resource. SSE/serverless behavior remains a runtime/surface capability until it has a concrete independent lifecycle.

Only validity-defining relationships are strong:

```text
Subscriber -> Channel
Subscriber -> Callback
Publisher  -> Channel
```

Groups may name absent members. Reverse SETs exist only where they buy a concrete query or guarded-deletion invariant.

## Provider grammar and authority

Canonical provider addresses are scope-first and opaque identities are injectively encoded:

```text
<scope>:logma:pubsub:...
<scope>:logma:transport:channel:<encoded-logical-channel>
```

Graph and transport authority are independent:

```text
LogmaPubSubGraph(Read | Write)
  -> scoped keys
  -> no Pub/Sub channel authority

LogmaPubSubTransport(Publish | Subscribe)
  -> scoped transport channels
  -> no graph-key authority
```

Redis ACL enforces provider scope and command/channel families. Trusted `RedisStore` code enforces graph semantics. Generic graph-write Redis credentials therefore belong to the trusted Logma control composition, not ordinary applications.

## Storage

The canonical graph remains Redis HASH + SET with explicit registries. This is provider representation, not operator vocabulary.

Keep:

- scope-first injective key construction;
- explicit registries rather than SCAN;
- reverse indexes for validity-defining relationships;
- bounded WATCH retry for optimistic graph reconciliation;
- cheap embedded-identity/address consistency checks.

Do not add repair daemons, background scrubbers, or automatic orphan reconstruction without production evidence.

## Atomic subscribe

`POST /subscribe` is one operation:

```text
ensure Channel
create Callback
create Subscriber
```

The provider performs it atomically. Callback and Subscriber identities are create-only on this path, so a collision neither overwrites independently managed resources nor leaves partial state.

## Runtime model

A durable Channel is not a live Redis listener. Redis Pub/Sub has no replay, so an empty listener preserves nothing.

```text
first active Subscription on Channel
    -> create shared listener

additional active Subscriptions
    -> share listener

last active Subscription shuts down
    -> release listener
```

Each active Subscription owns a child cancellation context beneath the shared listener. Detach cancels queued/in-flight callback work for that Subscription and prevents a stale handler snapshot from enqueueing new work after shutdown.

### First activation

There is no previous known-good handler:

```text
load declaration
-> acquire listener
-> install handler
-> wait for observed SUBSCRIBE readiness
-> publish active handle
-> success
```

The handler is installed before the initial ACK because Redis may deliver immediately after acknowledgement.

### Reconciliation

An already-active Subscription has a previous known-good handler:

```text
keep old handler
-> load replacement declaration
-> acquire/locate target listener
-> wait for target listener's observed current readiness
-> install replacement
-> publish replacement handle
-> retire old handler
```

If lookup or readiness fails, the previous handler remains installed. Same-identity operations serialize; different Subscription identities do not share one controller-wide operation lock.

### Readiness

Readiness is generation-aware at the outer Logma subscription loop. When that loop observes connection loss and enters reconnect, its readiness signal resets; later reconciliation waits for a fresh observed ACK rather than accepting a historical one.

This is an observation boundary, not a durability promise. The network can fail immediately after success. Logma remains best-effort Pub/Sub signaling.

`Runtime.Active` or a goroutine existing is not an operator-level readiness proof.

## Request and runtime lifetime

```text
request context
  lookup / validation / readiness deadline

controller / reconciler / service context
  successful runtime lifetime

shutdown / Close / process termination
  cancellation
```

A failed replacement must not erase a previous known-good definition. A successful activation must not die merely because its HTTP request returned.

## Bounded callback delivery

Redis receive must not synchronously wait on arbitrary HTTP endpoints:

```text
Redis receive
    -> service-wide bounded queue: 256
    -> workers: 4
    -> webhook requests
```

Overflow is counted and dropped without synchronous logging on the Redis receive path. Callback URLs are not written into failure logs because they may contain signed or secret material.

The current dispatcher is a service-wide capacity bound, not a fairness scheduler. One noisy Subscription can consume queue capacity. Do not add per-Subscription queues or fairness machinery until production evidence shows starvation is a real requirement.

NQC owns anti-entropy/repair when correctness requires more than best-effort notification. Logma does not become a durable queue for NQC.

## Publisher runtime

Generic Publisher reconciliation is deliberately small:

```text
load Publisher
-> validate durable Channel exists
-> resolve Publisher.Type provider
-> provider.EnsureActive(service-owned context)
```

Generic Logma does not start an empty consumer listener before a producer. Concrete providers own any stronger producer/consumer readiness dependency they actually require.

## Groups and state

Groups are weak best-effort control sets resolved when an operation executes:

```text
Group identities
-> SubscriptionController
-> completed / missing / failed
```

`GET /state` is declaration inventory, not runtime observation. Ordinary state exposes only:

```text
channels
subscriptions
publishers
groups
```

A sequential inventory read is not promised to be an atomic cross-resource snapshot. Add transactional observation only if an operator contract actually requires it.

## Runtime ownership and restart

Activation is process-local and ephemeral. Durable declarations are not desired runtime state.

Initial production contract:

```text
one Logma runtime owner per FATLINE_SCOPE
```

or equivalent routing to that owner. Distributed leases/leader election are deferred until multiple runtime owners per scope become a demonstrated requirement.

Restart reconstruction belongs to Fatline profile/bootstrap intent, for example activating named Groups after Logma starts. Do not add `desiredActive` to every Subscriber merely to survive restart.

## Package boundaries

```text
Logma       ephemeral signaling/callback fanout
NQC         cache coherence and anti-entropy
ratelimiter rate/lifecycle enforcement
Marai       narrow crypto authority
Redis       shared mechanical substrate
Fatline     profile/capability/runtime composition
Huram       exact-source qualification/deployment evidence
```

Each package owns its semantic provider requirements; Fatline composes them. Higher layers should not copy package-internal Redis command lists.

## Remaining production work

Do these before adding more resource nouns.

### Composition root

Current `cmd/api` is still development composition. The production boundary should be one Fatline-owned service:

```text
LogmaService
  graph Redis client/store
  transport Redis client
  Subscription runtime/controller
  Publisher registry/reconciler
  operator router
  advanced router
  Close()
```

Separate credentials remain separate; ownership and cleanup become explicit. Router constructors should not independently manufacture unmanaged Redis clients.

### Task-shaped unsubscribe

Complete ordinary create/delete symmetry with a task-shaped operation such as:

```text
DELETE /subscriptions/{id}
```

It should stop the locally owned active Subscription, remove its durable Subscriber, remove a convenience-owned Callback only when safe, and retain a shared Channel. It must not infer ownership of an independently managed/shared Callback.

## Design rule

Only machinery that preserves an observable contract earns complexity budget.

When edge-case guards repeatedly accumulate around an abstraction, reconsider the abstraction before hardening it again. Keep cheap correctness checks; defer speculative repair, distributed ownership, fairness schedulers, new resource nouns, and provider vocabulary until a real consumer requires them.
