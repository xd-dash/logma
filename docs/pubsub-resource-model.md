# Logma Pub/Sub resource model

This document describes the Fatline v2 Logma Pub/Sub model after the public-surface recalibration. Redis is the first storage/transport provider; Redis syntax is not the operator model.

## Operator model first

Ordinary Logma usage should remain close to the original service:

```text
subscribe a callback to a channel
unsubscribe it
save useful groups
activate or shut down a group
register a publisher
inspect state
```

The normal HTTP vocabulary is therefore intentionally small:

```text
POST /subscribe
POST /groups
GET  /groups/{id}
DELETE /groups/{id}
POST /groups/{id}/activate
POST /groups/{id}/shutdown
GET  /state
```

The normalized `/pubsub/*` resource API remains available as an advanced control-plane surface. Provider machinery such as registries, reverse indexes, WATCH retry, encoded Redis identities, PublisherProvider registration, and ACL command prerequisites is not part of the ordinary operator vocabulary.

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
ServerlessEndpoint        != standing Subscriber
```

## Strong relationships versus operational groups

Only relationships that define resource validity are strong in the canonical graph:

```text
Subscriber -> Channel
Subscriber -> Callback
Publisher  -> Channel
```

Those writes require their referenced resources to exist. Deleting a referenced Channel or Callback returns `ErrReferenced` rather than producing malformed resources.

Groups are different. A Group is a named control set, not an existential dependency:

```text
Group morning-feeds
  screen-a
  screen-b
  screen-later
```

`screen-later` may be absent when the Group is created. Deleting `screen-b` does not mutate or invalidate the Group. Group activation/shutdown resolves identities at execution time and reports completed, missing, and failed members separately.

Production protection belongs to authority/profile policy rather than a second public Group taxonomy. A deployment may grant Group mutation only to a narrow Redis principal/keyspace while keeping the same Group domain and HTTP model.

## Canonical identity and Redis addresses

`FATLINE_SCOPE` is explicit, non-empty, and pattern-safe. New v2 code parses it fail-closed rather than synthesizing a scope such as `unknown`.

Domain identities are canonicalized once by trimming outer whitespace, then encoded injectively when interpolated into Redis key segments. Structural grammar remains package-owned. Raw logical identities remain raw values in HASH fields and SET membership.

The canonical graph is:

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

`ResourceKeysV2` is the production mapper for `RedisStore` on this branch. There is no legacy-key fallback or dual read/write path.

## Redis-native persistence remains

The public simplification is not a return to JSON-per-resource storage. The provider still uses HASH + SET, explicit collection registries, injective resource addressing, bounded transactional mutation, and strong reverse state where an actual existential relationship requires guarded deletion.

This preserves production properties such as:

```text
incremental mutation
explicit collection discovery
cheap membership reads
atomic relationship updates
ACL-able key families
bounded optimistic contention
no SCAN-based production discovery
```

while keeping them below the normal operator surface.

## Streamlined subscription composition

The common webhook path is compiled from one request:

```text
POST /subscribe
  channel + callbackURL
        |
        v
ensure Channel
ensure webhook Callback
create Subscriber
```

Callers may supply explicit subscription/callback IDs, but ordinary callers do not need to manually perform three normalized resource writes.

The current façade composes durable declarations. Runtime activation remains separately authorized; storage authority does not silently become Redis `SUBSCRIBE` authority.

## Group runtime control

The simple Group endpoints deliberately use an injected subscription controller:

```text
Group declaration
      |
      v
resolve Subscriber identities now
      |
      +--> missing -> report missing
      |
      +--> present -> authorized runtime controller
                         |
                         +--> activate/shutdown
```

Without explicit runtime composition, group activate/shutdown returns unavailable rather than reusing the graph credential as transport authority.

## Publisher reconciliation

Persistence and runtime activation remain separate internally:

```text
Publisher resource
      -> PublisherReconciler
      -> load referenced Channel
      -> ensure Channel runtime is active
      -> resolve Publisher.Type in PublisherRegistry
      -> PublisherProvider.EnsureActive(...)
```

`PublisherProvider`, `PublisherRegistry`, and Channel activation are package/runtime extension points, not ordinary operator nouns. Concrete producer integrations such as `stonks`, `news`, Unix/socket producers, SSE-oriented producers, or NQC adapters register their implementations without being hardcoded into generic Logma.

## Semantic capability and ACL provider

New v2 authority is expressed semantically and compiled into provider requirements:

```text
Logma capability
      -> Redis provider requirements
      -> exact key/channel/command ACL
```

The existing graph capability is scoped to `~<scope>:logma:pubsub:*` and does not imply Redis Pub/Sub transport, scripting, Function administration, or neighboring Logma runtime authority. Semantic `Write` includes only provider-internal prerequisites needed for safe mutation.

Higher-level Fatline profiles should compose package-declared Logma, ratelimiter, NQC, and Marai requirements so application authors do not manually reconstruct Redis commands and patterns.

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

Each normalized family also has `GET` and `DELETE` by identity. Publisher additionally exposes its explicit reconcile endpoint for advanced compositions.

## Complexity budget

The design intentionally separates four levels:

```text
operator vocabulary
  Channel / Callback / Subscription / Publisher / Group
        |
package/runtime API
  Store / Runtime / adapters / profiles
        |
provider machinery
  HASH / SET / registries / WATCH / ACL patterns / encoded identities
        |
deployment qualification
  exact SHA / Redis version / ACL negatives / cleanup evidence
```

Provider sophistication is acceptable when it remains below the operator boundary. A provider-specific invariant alone is not sufficient reason to invent a new public domain noun.

## Legacy and qualification boundaries

Historical flattened `active_subscriptions:*` state remains a separate compatibility surface. The v2 branch does not reinterpret or silently migrate it. Older lossy `serverless/keyspace` helpers likewise remain compatibility primitives rather than templates for new v2 authority.

The source gate remains:

```text
go mod tidy + clean go.mod/go.sum
gofmt
go vet ./...
go test ./...
go build ./cmd/api
```

Redis qualification continues to prove canonical addresses, resource registries, existential graph integrity, weak Group persistence semantics, bounded WATCH retry, generated ACL behavior, write-only mutation authority, negative NOPERM boundaries, and scoped residue cleanup.

Retained Farcaster provider behavior still requires separate side-by-side qualification before any canonical retained provider cutover.
