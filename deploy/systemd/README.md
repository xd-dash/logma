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
# edit /etc/logma/logma.env to set PORT

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
