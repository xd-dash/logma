# Logma runtime cell

The production-shaped container target lives in `flake.nix`.

```text
public host/network
       |
       v
nginx :8080
       |
       v
logma :18080 (loopback only)
       |
       v
/run/logma-redis/redis.sock
       |
       v
Redis (port 0)
```

s6-overlay is PID 1 and supervises:

```text
init-dirs -> redis -> logma -> nginx
```

Redis has TCP disabled and is never published from the container. Nginx exposes
only Logma HTTP traffic. Logma uses its existing `REDIS_SOCKET` support.

The image declares `/run/logma-redis` as a volume boundary. Mount the same
named volume into a sibling container to connect directly to Redis over the
Unix socket without involving nginx or the host network:

```sh
docker volume create logma-redis-socket

docker run -d \
  --name logma \
  -v logma-redis-socket:/run/logma-redis \
  -p 8080:8080 \
  logma-cell:latest

docker run --rm \
  -v logma-redis-socket:/run/logma-redis \
  --entrypoint redis-cli \
  logma-cell:latest \
  -s /run/logma-redis/redis.sock ping
```

The second container reaches the Redis Unix socket through the shared container
volume. Redis still has no TCP listener.

Build:

```sh
nix build .#image
docker load < result
```

The image uses `dockerTools.buildLayeredImage`, preserving reusable Nix store
paths as OCI layers rather than flattening the filesystem.


## CI

`.github/workflows/container-cell.yml` builds and smoke-tests this image on
`blacksmith-2vcpu-ubuntu-2404`. The Nix builder runs with named volumes for
`/nix` and the Nix user cache; those volumes are archived into GitHub Actions
cache at the end of a cold build and restored on later runs.


## s6-overlay v3 layout

The cell uses s6-overlay v3.2.3.2 and the current s6-rc user-bundle layout:

```text
/etc/s6-overlay/s6-rc.d/<service>/
/etc/s6-overlay/user-bundles.d/user/contents.d/<service>
```

Services depend on the s6 `base` target plus their application dependency:

```text
base
  |
init-dirs
  |
redis
  |
logma
  |
nginx
```

This removes the deprecated v3 compatibility layout under
`s6-rc.d/user/contents.d`.


## Shared Redis socket permissions

The Redis daemon runs as a dedicated `redis` user/group with numeric GID
`6379`. The shared socket directory is owned by `6379:6379` with mode
`0770`, and Redis creates `redis.sock` with mode `0770`.

A sibling container therefore does not need to run as root. It can mount the
same volume and run with group `6379`:

```sh
docker run --rm \
  --user 10001:6379 \
  -v logma-redis-socket:/run/logma-redis \
  --entrypoint redis-cli \
  logma-cell:latest \
  -s /run/logma-redis/redis.sock ping
```

This is host-local IPC through the container volume layer. It does not traverse
nginx, a published port, or the host TCP stack.
