# Marai-backed Logma state

Logma uses Marai as an encrypted, ephemeral state service while continuing to use
Redis Pub/Sub as the notification transport.

## Trust boundary

DB 0 is reserved for the Marai operator and is never selected by Logma. The Logma
Redis connection defaults to DB 1, and the Marai cache database independently defaults
to DB 1.

The Logma ACL identity does not need raw Redis keyspace commands. Persistent runtime
state is accessed only through:

- `marai_cache_set`
- `marai_cache_get`
- `marai_cache_delete`

Pub/Sub remains native Redis and is restricted with Redis channel ACL patterns.

## State model

The Marai cache namespace defaults to `logma`.

Encrypted cache records:

- `active:<subscription-id>` — one active subscription descriptor.
- `group:<group-id>` — one independently encrypted saved group manifest.

Saved-group discovery uses the Marai index named `groups`, not an encrypted global
JSON catalog. The index contains group IDs only and is paged with a numeric
`cursor`/`count`; each group payload remains a separate MRC1 record.

Active-subscription discovery also does not use Redis `SCAN`. The running process
owns the authoritative live subscription map and snapshots it when saving a group.

A group manifest contains the channel and callback secret for every member. The
callback secret contains:

- callback URL;
- optional access token;
- optional token scheme (defaults to `Bearer`).

The URL and token are serialized together and encrypted by Marai before Redis stores
them. They are decrypted only when Logma needs to restore a subscription or dispatch a
callback.

## Callback delivery

Access tokens are never logged. If configured, Logma constructs:

`Authorization: <scheme> <token>`

only for the outbound callback request.

Callback URLs are also omitted from error logs. HTTPS should be required for callback
endpoints in production.

URLs and tokens do not travel in Redis Pub/Sub messages. They travel between Logma and
Marai only as arguments/results of the cache functions. Prefer a Unix-domain Redis
socket for colocated deployments; use Redis TLS if that transport ever becomes remote.

## Group index

Group membership is maintained by Marai's DB-pinned index API. `saveGroup` writes
the encrypted `group:<id>` record first and then idempotently adds the group ID to
the `groups` index. If the index add fails, Logma removes the newly written group
record rather than returning success with an undiscoverable group.

Listing is bounded and paginated through `GET /channels/groups?cursor=<n>&count=<n>`.
The default count is 100 and the maximum is 1000. The cursor is an offset into the
sorted ID index and is not a snapshot cursor; concurrent additions/removals may shift
subsequent pages. Do not reintroduce Redis keyspace `SCAN` for discovery.

## Bootstrap

Before Logma starts, the Marai administrator creates the configured KMS key (default
`logma`) and provisions the Logma ACL identity. Function loading remains a Marai
startup concern; Logma assumes the encrypted-cache functions are already present.

The runtime fails closed. If Marai cannot encrypt or persist state, a new subscription
is not accepted.

## Ephemeral semantics

Marai/Redis remains a cache, not the source of truth. A Marai process restart destroys
the KMS authority and therefore invalidates encrypted cache entries. Any durable
configuration, callback registration, token minting authority, or IaC data that must
survive that event belongs in an external source of truth and can be rehydrated during
bootstrap.
