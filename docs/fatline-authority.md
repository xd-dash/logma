# Fatline authority bindings

Logma owns the durable Fatline binding registry, mutable aliases, immutable binding artifacts, lifecycle attachment, and event propagation. It does **not** own generic identity/authz semantics and it does not make Redis ACL usernames into global principals.

The current hierarchy is:

```text
organization / Huram business-security authority
        |
        +-- Prajapati authz.Policy + authz.Profile
        |       immutable policy digest
        |
        +-- ratelimiter PolicyBinding
        |       rate/lifecycle execution policy
        |
        `-- ratelimiter/redisacl profile
                local Redis execution shape only
                         |
                         v
                    Logma Binding
```

Prajapati's public `authz` package owns provider-neutral authorization semantics and monotonic parent -> child narrowing. Logma stores the resulting `auth_profile` and `auth_policy_digest` in its compiled `Binding`; it does not reinterpret the policy language.

Ratelimiter continues to own machine rate/lifecycle policy and profile capability validation. `redis_acl_profile`, when present, selects a local Redis execution shape that Logma can compile through `ratelimiter/redisacl`. The generated username/password/rules are process-local enforcement material, not the distributed caller identity.

A typical runtime therefore looks like:

```text
ed25519:logma/world-17
        |
        v
     Prajapati
 principal + audience + action + resource
        |
        v
      Logma
 exact frozen Binding
        |
        +-- rate/lifecycle policy
        |
        `-- local Redis ACL execution identity
```

Marai is separate: it retains its fixed `marai-app` / `marai-admin` ACL split and does not use Logma's tenant Redis ACL compiler.

## Redis grammar

Control-plane policy data and runtime state remain separate:

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

## HTTP authentication compatibility

The older Logma ACL-auth experiment allowed HTTP Basic credentials backed directly by Redis ACL users. That remains useful as a development/compatibility mode, but it is not the intended Fatline identity architecture. The intended path is external credential -> Prajapati normalized principal/authz -> Logma operation -> constrained local Redis execution identity.
