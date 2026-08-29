# Logma ACL profiles

Logma has two authentication profiles.

## legacy (default)

This preserves the existing single-tenant deployment contract:

- HTTP requests authenticate with `X-API-Key`.
- Logma uses the Redis credentials supplied through `REDIS_USERNAME` and
  `REDISCLI_AUTH`.
- Redis ACL creation and customization remain the bootstrapper's responsibility.

Example:

```sh
LOGMA_AUTH_PROFILE=legacy
API_KEY=dev-logma
REDIS_URI=127.0.0.1:6379
REDIS_USERNAME=default
REDISCLI_AUTH=change-me
```

## managed

Managed mode uses Redis ACL credentials for HTTP Basic authentication. The Logma
process itself connects as an application administrator. That identity is expected
to be bootstrapped outside Logma and must be able to manage ACL users, load/delete
Function libraries, manage Logma control-plane metadata, and operate all channels.

Minimal development bootstrap:

```redis
ACL SETUSER logma-admin reset on >change-this-admin-password ~* &* +@all
```

Run Logma with:

```sh
LOGMA_AUTH_PROFILE=managed
LOGMA_ADMIN_USER=logma-admin
REDIS_USERNAME=logma-admin
REDISCLI_AUTH=change-this-admin-password
```

The `default` Redis user should normally be disabled after bootstrap.

### Tenant profiles

The application admin can mint users through:

```text
POST   /pubsub/api/v0.0.1/users/
GET    /pubsub/api/v0.0.1/users/
PUT    /pubsub/api/v0.0.1/users/{username}
DELETE /pubsub/api/v0.0.1/users/{username}
```

Built-in policies:

| profile | data keyspace | publish | subscribe | FCALL |
| --- | --- | --- | --- | --- |
| `tenant` | yes | yes | yes | no |
| `tenant-functions` | yes | yes | yes | yes |
| `publisher` | no | yes | no | no |
| `subscriber` | no | no | yes | no |

A tenant named `acme` is scoped to:

```text
Redis keys:     logma:tenant:acme:*
Pub/Sub:        tenant:acme:*
Function prefix logma_acme__
```

Tenant policies begin with `-@all` and add explicit commands. They do not grant
`EVAL`, `EVALSHA`, `FUNCTION LOAD`, `FUNCTION DELETE`, `ACL`, `KEYS`,
`SCAN`, `FLUSH*`, `SWAPDB`, `CONFIG`, `MODULE`, or broad command
categories.

Subscription groups still use Logma's historical global Redis key layout. They
remain application-admin-only in managed mode until that storage schema is
tenant-prefixed.

### Function service

Tenants with the `tenant-functions` profile can upload callback function bodies
through:

```text
POST   /pubsub/api/v0.0.1/functions/
GET    /pubsub/api/v0.0.1/functions/
DELETE /pubsub/api/v0.0.1/functions/{name}
```

Logma, not the tenant, owns `FUNCTION LOAD`. It wraps the submitted callback body
in a tenant-namespaced library and registered function name. A callback can be
attached to a channel as:

```json
{
  "callbacks": {
    "type": "redis-function",
    "config": {
      "name": "normalize",
      "keys": ["state"],
      "args": ["example"]
    }
  }
}
```

For tenant `acme`, `state` becomes `logma:tenant:acme:state`. Logma executes
the callback using the tenant's Redis ACL identity, not the application-admin
identity. The published message JSON is appended to the function arguments.

Function-name namespacing prevents collisions and makes ownership visible; it is
not the authorization boundary. Redis ACL command/key/channel permissions are the
authorization boundary.

### ACL persistence

Logma mutates the live ACL table. Set:

```sh
LOGMA_ACL_SAVE=true
```

only when Redis is configured with an ACL file and the application-admin identity
may run `ACL SAVE`. Otherwise the deployment/bootstrap layer remains responsible
for persisting ACL configuration.
