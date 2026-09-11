# Fatline authority bindings

Logma currently houses the provider-neutral Go contract for compiled Fatline authority bindings because Logma is the durable lifecycle owner in the current composition. This does not make Logma the owner of organization business policy; it persists and validates the compiled result handed to runtime/deployment ownership.

The hierarchy is:

```text
organization business/security authority
        |
        v
      OrgGrant
        |
        v
   AuthPolicy (immutable digest)
        |
        +-- AuthProfile (execution capability shape)
        |
        `-- ratelimiter profile + PolicyCode
                         |
                         v
                      Binding
```

A child authorization policy may only narrow its parent policy. `AuthPolicy.Allows` enforces subset semantics over actions, resources, audiences, roles, claim values, delegation, expiry requirements, and network scopes.

Profiles describe enforcement capability only. They are not product tiers or roles. Supported v1 profile identifiers are `minimal-v1`, `jwt-v1`, `claims-v1`, `capability-v1`, and `delegated-v1`.

## Redis grammar

Control-plane policy data and runtime state are separate:

```text
<org>:fatline:auth:policy:<sha256>
<org>:fatline:auth:binding:tenant:<tenant>
<org>:fatline:auth:binding:service:<tenant>:<service>
<org>:fatline:auth:binding:route:<tenant>:<service>:<route>
<org>:fatline:auth:binding-artifact:<sha256>

<org>:fatline:runtime:<family>:<tenant>:...
```

The `auth:binding:*` key is a mutable control-plane alias for the current binding at that scope. `auth:binding-artifact:<sha256>` is the immutable compiled binding artifact. Durable deployment/lifecycle intent freezes the binding snapshot/digest; it must never resolve a mutable alias during restart or reconstruction.

Ratelimiter `PolicyCode` is encoded as decimal text inside cross-language bindings. It must not be emitted as a JSON number because JavaScript cannot exactly represent every `uint64` value.

## Lifecycle handoff

`lifecycle.Registration` accepts an optional exact `binding` snapshot. New deployments should provide it. Existing records without a binding remain readable for migration compatibility.

When a binding contains a ratelimiter policy, its `rate_policy_code` must exactly equal the lifecycle registration's canonical `policy_code`. Retry matching compares the exact binding snapshot, preventing a repeated handoff from silently changing org/tenant/service authority.

The lifecycle owner persists the compiled contract; it does not recompile organization business rules on restart.
