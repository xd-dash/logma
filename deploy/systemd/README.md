# systemd service

Logma runs as the dedicated unprivileged `logma` user, managed by the system systemd instance. This keeps boot/restart semantics independent of an interactive login session while still preventing the application from running as root.

Production deployments should build the Linux binary in CI and copy the resulting artifact to the server. The server does not need the Go toolchain.

Initial host setup:

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin logma

sudo mkdir -p /etc/logma
sudo cp deploy/systemd/logma.env.example /etc/logma/logma.env
sudo chown logma:logma /etc/logma/logma.env
sudo chmod 600 /etc/logma/logma.env
# edit /etc/logma/logma.env to set the Marai socket/credential, API key, and callbacks

sudo cp deploy/systemd/logma.service /etc/systemd/system/logma.service
sudo systemctl daemon-reload
sudo systemctl enable logma
```

A CI-built binary can first be copied to a staging location such as `/tmp/logma-<commit-sha>`. Installing or promoting that artifact is a separate privileged host operation:

```sh
sudo install -o root -g root -m 0755 /tmp/logma-<commit-sha> /usr/local/bin/logma
sudo systemctl restart logma
```

Check status and logs:

```sh
sudo systemctl status logma
sudo journalctl -u logma -f
```

Keeping build, transport, and privileged installation as separate steps makes rollback and access boundaries explicit.


## Marai-backed state

The service is designed to connect to the colocated Marai Redis process on DB 1.
DB 0 is reserved for the Marai operator and must not be selected by Logma.

Before starting Logma:

1. Create the Marai KMS key named by `MARAI_KMS_KEY_ID` (default `logma`) with the Marai administrator identity.
2. Provision the Logma Redis ACL identity with:
   - `SELECT 1` (or the configured connection DB);
   - the Marai encrypted-cache FCALL/native command permissions;
   - only the required Pub/Sub channel patterns.
3. Put the Logma credential in the mode-0600 environment file or an equivalent systemd credential source.

Logma does not require raw `GET`, `SET`, `DEL`, `SCAN`, or `KEYS` permissions.
Subscription/group state is encrypted through Marai and raw Redis key enumeration is
not part of the runtime model.

For transport confidentiality, prefer the local Unix socket. If Redis is remote, use
a TLS-protected Redis connection; Marai encrypts stored values, but plaintext function
arguments still traverse the client transport before the native module encrypts them.

Callback URLs and optional access tokens are encrypted together in Marai-backed state.
Tokens are only materialized when building the outbound HTTP request and are never
written to logs. Callback endpoints should use HTTPS.
