# Security event transport boundary

Logma is the event-distribution integration point for regional security/runtime cells. This contract reserves the event shape without implementing cross-region consistency.

Producers may emit namespaced events such as `secret.updated`, `config.updated`, `kms.rotated`, and `credential.rotated`. Every event must carry a tenant namespace, source region, event ID, logical version, and timestamp. Consumers must treat delivery as at-least-once-capable and idempotent even when the active transport is Redis Pub/Sub.

Redis Pub/Sub is a low-latency notification transport, not a durable replication log. Regional convergence must eventually pair fast notification with a durable catch-up transport such as Redis Streams or Google Pub/Sub. Logma must keep the transport behind an event-bus abstraction so callers do not depend directly on Pub/Sub semantics.

No event payload should assume marai master-key continuity across regions. Portable durable secrets require a separately designed encrypted representation and regional re-wrapping strategy.
