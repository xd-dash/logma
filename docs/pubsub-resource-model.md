# Logma Pub/Sub resource model

This document describes the Fatline v2 Logma Pub/Sub model after the public-surface recalibration and subsequent implementation audit. Redis is the first storage/transport provider; Redis syntax is not the operator model.

## Operator model first

Ordinary Logma usage should remain close to the original service:

```text
subscribe a callback to a channel
unsubscribe it
save useful groups
activate or shut down a group
register a publisher
inspect declaration state
```

The normal HTTP vocabulary is intentionally small:

```text
POST /subscribe
POST /groups
GET  /groups/{id}
DELETE /groups/{id}
POST /groups/{id}/activate
POST /groups/{id}/shutdown
GET  /state
```

The normalized `/pubsub/*` resource API remains available as an advanced control-plane surface. Provider machinery such as HASH/SET layout, registries, strong-reference reverse indexes, WATCH retry, encoded Redis identities, transport address compilation, runtime adapter registries, and ACL command prerequisites is not ordinary operator vocabulary.

A convenience operation may compile to several normalized resources, but it must still have one operation-level failure contract. Simplifying the API must not mean accepting partial graph mutation underneath it.

## Domain resources

```text
Channel
  durable logical Logma channel
  may exist with zero Subscribers

Callback
  independently addressable action
  webhook -> one or many HTTP(S) targets
  lua     -> function name

Subscriber
  normalized durable attachment to one Channel
  references one or more Callbacks
  presented as Subscription in the streamlined operator surface

Publisher
  durable producer binding to one Channel
  producer type selects an internal runtime adapter
  producer-specific config remains opaque to Logma

SubscriptionGroup
  durable, mutable operational collection of Subscriber identities
  members are weak identities and may be absent

PublisherGroup
  durable, mutable operational collection of Publisher identities
  members are weak identities and may be absent
  does not imply a shared Channel, provider type, process, or credential

ServerlessEndpoint
  requester-driven surface such as SSE
  not a standing Subscriber
```

The important distinctions are:

```text
Logma Channel resource != Redis Pub/Sub transport topic
Subscriber               != Channel
Subscription             = operator-facing view of Subscriber attachment
Publisher graph record   != active producer process/runtime
Group membership         != existential graph reference
stored declaration state != runtime activity observation
ServerlessEndpoint        != standing Subscriber
```

## Strong relationships versus operational groups

Only relationships that define resource validity are strong in the canonical graph:

```text
Subscriber -> Channel
Subscriber -> Callback
Publisher  -> Channel
```

Those writes require their referenced resources to exist. Deleting a referenced Channel or Callback returns `ErrReferenced` rather than producing malformed durable resources. Channel deletion is likewise blocked by a referencing Publisher.

Groups are different. A Group is a named control set, not an existential dependency:

```text
Group morning-feeds
  screen-a
  screen-b
  screen-later
```

`screen-later` may be absent when the Group is created. Deleting `screen-b` does not mutate or invalidate the Group. Group activation/shutdown resolves identities at execution time and reports completed, missing, and failed members separately.

Production protection belongs to authority/profile policy rather than a second public Group taxonomy. A deployment may grant Group mutation only to a narrow principal while keeping the same Group domain and HTTP model.

## Canonical identity and durable Redis addresses

`FATLINE_SCOPE` is explicit, non-empty, and pattern-safe. New v2 code parses it fail-closed rather than synthesizing a scope such as `unknown`.

Domain identities are canonicalized once by trimming outer whitespace, then encoded injectively when interpolated into provider address segments. Structural grammar remains package-owned. Raw logical identities remain raw values in HASH fields and SET membership.

The canonical durable graph is:

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

<scope>:logma:pubsub:registry:channels
<scope>:logma:pubsub:registry:callbacks
<scope>:logma:pubsub:registry:subscribers
<scope>:logma:pubsub:registry:publishers
<scope>:logma:pubsub:registry:subscription-groups
<scope>:logma:pubsub:registry:publisher-groups
```

Group reverse-index families were removed when Group semantics returned to weak operational collections; retaining unused reverse state would imply deletion authority that no longer exists.

`ResourceKeysV2` is the production durable mapper on this branch. There is no legacy-key fallback or hidden dual read/write path.

Provider reads also verify representation identity. If a resource HASH addressed as `subscriber:expected` claims embedded ID `different`, the store fails closed instead of silently returning the wrong logical resource. Key identity and stored identity are one integrity contract.

## Canonical scoped transport addresses

Scope applies to transport as well as storage. A scope-first graph combined with raw global Redis Pub/Sub names is only half-scoped isolation.

The canonical Fatline v2 Redis topic for a logical Logma Channel is:

```text
<scope>:logma:transport:channel:<encoded-logical-channel>
```

For example:

```text
logical Logma Channel
  market:quotes

scope
  tenant-a

Redis Pub/Sub topic
  tenant-a:logma:transport:channel:market%3Aquotes
```

The logical Channel identity remains `market:quotes` throughout the domain and runtime maps. Only the provider transport address changes.

`pubsubruntime.NewScoped` is the canonical v2 constructor. The older `New` constructor retains raw-channel behavior only as an explicit compatibility surface. New v2 deployment composition must not grant `&*` merely to compensate for unscoped topic naming.

## Redis-native persistence remains

The public simplification is not a return to JSON-per-resource storage. The provider still uses HASH + SET, explicit collection registries, injective resource addressing, bounded transactional mutation, and strong reverse state only where an actual existential relationship requires guarded deletion.

This preserves production properties such as:

```text
incremental mutation
explicit collection discovery
cheap membership reads
atomic relationship updates
ACL-able key and channel families
bounded optimistic contention
no SCAN-based production discovery
```

while keeping them below the normal operator surface.

## Atomic streamlined subscription composition

The common webhook path is compiled from one request:

```text
POST /subscribe
  channel + callbackURL
        |
        v
one provider transaction
        |
        +--> ensure Channel
        +--> create webhook Callback
        +--> create Subscriber
```

Channel is ensure-style because several subscriptions naturally share one logical Channel. The Callback and Subscriber identities created by this convenience operation are create-only. An explicit or generated identity collision returns conflict; it does not overwrite an independently managed resource.

The provider operation is atomic under the same bounded WATCH/MULTI/EXEC policy used by the graph. A late Subscriber conflict therefore cannot leave a new Callback or Channel fragment behind, and a Callback collision cannot be used to rewrite a Callback already referenced by another Subscriber.

Ordinary callers may supply explicit subscription/callback IDs, but do not need to manually perform three normalized resource writes.

Durable declaration composition still does not activate Redis transport. Storage authority does not silently become `SUBSCRIBE` authority.

## Group runtime control uses one resolution point

The simple Group endpoints deliberately use an injected subscription controller:

```text
Group declaration
      |
      v
member identities
      |
      v
authorized SubscriptionController
      |
      +--> ErrNotFound -> missing
      +--> other error -> failed
      +--> success      -> completed
```

The HTTP façade does not preflight `GetSubscriber` and then ask the controller to resolve the same identity again. Double resolution creates a TOCTOU window and ambiguous error classification. The controller is the authoritative execution-time resolver.

Without explicit runtime composition, group activate/shutdown returns unavailable rather than reusing the graph credential as transport authority.

## Activate means reconcile current declaration

`ActivateSubscription(id)` means ensure the runtime reflects the current Subscriber/Callback declaration, not merely ensure that an old handler bearing the same ID exists.

Repeated activation:

```text
read current Subscriber + Callback declarations
      |
ensure Channel active
      |
attach replacement handler
      |
only after success, detach previous handle
```

If current reconciliation fails, the previous known-good handler remains installed. Generation/token-safe handles ensure closing the previous handle cannot remove its replacement.

Concurrent operations for different Subscriber IDs are independent. Concurrent operations for the same ID are serialized by the controller rather than by a global lock around Redis/runtime work.

Shutdown is idempotent.

## Request context is not runtime lifetime

HTTP/request context authorizes and bounds the command. It must not accidentally become the lifetime owner of a successful long-lived subscription or producer.

The lifecycle rule is:

```text
request context
  validates / loads / bounds one command

controller or reconciler context
  owns runtime that survives the command

Close / shutdown
  cancels that runtime lifetime
```

The SubscriptionController therefore owns Channel activation lifetime independently of the request that asked for activation. PublisherReconciler follows the same rule for Channel activation and `PublisherProvider.EnsureActive`.

A canceled request may stop an operation that has not completed. Once a runtime has been successfully established, later cancellation of that HTTP request does not tear it down. Explicit controller/reconciler shutdown does.

## Publisher reconciliation

Persistence and runtime activation remain separate internally:

```text
Publisher resource
      -> PublisherReconciler
      -> load referenced Channel
      -> ensure Channel runtime active using reconciler lifetime
      -> resolve Publisher.Type in PublisherRegistry
      -> PublisherProvider.EnsureActive(reconciler lifetime, ...)
```

`PublisherProvider`, `PublisherRegistry`, and Channel activation are package/runtime extension points, not ordinary operator nouns. Concrete producer integrations such as `stonks`, `news`, Unix/socket producers, SSE-oriented producers, or NQC adapters register implementations without being hardcoded into generic Logma.

Provider `EnsureActive` remains expected to be idempotent/reconciling for a stable Publisher identity/configuration.

## Semantic capabilities and provider ACLs

New v2 authority is expressed semantically and compiled into provider requirements:

```text
LogmaPubSubGraph(Read | Write)
      -> ~<scope>:logma:pubsub:*
      -> exact Redis key commands

LogmaPubSubTransport(Publish | Subscribe)
      -> &<scope>:logma:transport:channel:*
      -> exact Redis Pub/Sub commands
```

Graph authority grants no Redis channel patterns and does not imply Pub/Sub transport, scripting, Function administration, or neighboring Logma runtime authority.

Transport authority grants no durable key patterns. Publish-only and Subscribe-only are independently expressible and independently qualified. A Subscribe principal cannot PUBLISH; a Publish principal cannot SUBSCRIBE; neither can cross into another FATLINE scope.

Semantic `Write` includes provider-internal read primitives needed for safe guarded mutation without granting an unrelated independent read surface such as `HGETALL`.

Higher-level Fatline profiles should compose package-declared Logma, ratelimiter, NQC, and Marai requirements so application authors do not manually reconstruct Redis commands and patterns.

## `/state` is declaration inventory

`GET /state` intentionally translates durable registries into operator vocabulary:

```text
channels
subscriptions
publishers
groups
publisherGroups
```

It currently means declared state, not proof that every listed Subscription or Publisher is active in this process. Runtime observation is a separate authority/observation concern and should be added deliberately rather than inferred from durable registries.

This distinction preserves a small operator vocabulary without conflating persistence and process-local runtime state.

## NQC, ratelimiter, and Marai boundaries

The intended composition remains deliberately small:

```text
Logma       ephemeral signaling and callback/runtime fanout
NQC         cache coherence and anti-entropy repair
ratelimiter rate/lifecycle enforcement
Marai       narrow cryptographic authority
Redis       shared mechanical substrate
Fatline     capability/profile composition
Huram       exact-source deployment and qualification evidence
```

NQC reliability does not require Logma to become a durable queue or replay system. NQC adapters may use Logma signaling while NQC continues to own revision/origin/reconciliation semantics.

Marai's function-shaped authority and ratelimiter's opaque profile composition remain reference patterns: complex provider machinery should compile from small application-facing operations/capabilities rather than leak upward into the domain vocabulary.

## HTTP surfaces

Normal operator surface:

```text
POST   /subscribe
POST   /groups
GET    /groups/{id}
DELETE /groups/{id}
POST   /groups/{id}/activate
POST   /groups/{id}/shutdown
GET    /state
```

Advanced normalized control-plane surface:

```text
GET/POST /pubsub/channels
GET/POST /pubsub/callbacks
GET/POST /pubsub/subscribers
GET/POST /pubsub/publishers
GET/POST /pubsub/subscription-groups
GET/POST /pubsub/publisher-groups
```

Each normalized family also has `GET` and `DELETE` by identity. Publisher additionally exposes its explicit reconcile endpoint for advanced runtime compositions.

## Complexity budget

The design intentionally separates four levels:

```text
operator vocabulary
  Channel / Callback / Subscription / Publisher / Group
        |
package/runtime API
  Store / Runtime / controllers / reconcilers / adapters / profiles
        |
provider machinery
  HASH / SET / registries / WATCH / key+channel ACLs / encoded identities
        |
deployment qualification
  exact SHA / provider version / ACL positives+negatives / cleanup evidence
```

Provider sophistication is acceptable when it remains below the operator boundary. A provider-specific invariant alone is not sufficient reason to invent a new public domain noun.

The simplification rule is therefore not merely “hide complexity.” It is:

```text
fewer concepts upward
stronger and more explicit contracts downward
```

## Legacy and qualification boundaries

Historical flattened `active_subscriptions:*` state remains a separate compatibility surface. The v2 branch does not reinterpret or silently migrate it. Older lossy `serverless/keyspace` helpers and raw-channel `pubsubruntime.New` behavior likewise remain compatibility primitives rather than templates for new v2 authority.

The source gate remains:

```text
go mod tidy + clean go.mod/go.sum
gofmt
go vet ./...
go test ./...
go build ./cmd/api
```

Redis qualification proves canonical durable addresses, scoped transport addresses, resource registries, existential graph integrity, weak Group semantics, atomic convenience composition, bounded WATCH retry, generated graph ACL behavior, independent publish/subscribe transport ACLs, negative cross-scope/authority boundaries, stored-identity integrity, and scoped residue cleanup.

Retained Farcaster provider behavior still requires separate side-by-side qualification before any canonical retained provider cutover.
