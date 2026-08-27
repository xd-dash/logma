# Security scopes

Logma treats deployment tenancy as an operator-defined security scope. A scope commonly maps to one external cloud project, but no cloud provider is required by the core application.

```text
LOGMA_SCOPE_ID=scope-7f85
LOGMA_SCOPE_NAMESPACE=subscriptions
LOGMA_SCOPE_DB=4
MARAI_KEY_ID=logma
```

Optional provider metadata can describe how a bootstrap/control plane mapped the scope:

```text
LOGMA_SCOPE_PROVIDER=gcp
LOGMA_EXTERNAL_PROJECT=customer-prod
```

Those provider values are metadata only. They are deliberately excluded from Marai cryptographic arguments so moving the same deployment model to another provider does not change ciphertext semantics.

`LOGMA_SCOPE_DB` controls Redis routing for this Logma process and takes precedence over the legacy `REDIS_DB`. Redis DBs are organizational boundaries, not hostile-tenant security boundaries. Redis Pub/Sub is server-wide, and Marai's native key store is process-wide.

For secret material, Logma's Marai integration uses the scoped `MRA2` context:

```text
scope_id + namespace + key_id + key_version
```

The recommended multi-project profile is one immutable scope ID and one independent Marai key family per external project. Multiple projects owned by the same customer remain cryptographically independent unless the operator intentionally configures a coarser model.

Isolation profiles may therefore range from:

```text
shared Redis DB + separate scope/key family
project DB + separate scope/key family   # recommended project profile
separate Marai process                   # strongest process boundary
```

A GCP bootstrap implementation may authenticate a project identity and issue the scope configuration, but GCP IAM, project IDs, and Cloud Functions are optional integration details rather than Logma or Marai core requirements.
