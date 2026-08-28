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
/run/redis/redis.sock
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

Build:

```sh
nix build .#image
docker load < result
```

The image uses `dockerTools.buildLayeredImage`, preserving reusable Nix store
paths as OCI layers rather than flattening the filesystem.
