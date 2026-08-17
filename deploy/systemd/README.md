# systemd service

Installs `logma` as a systemd service running under a dedicated system user.

```sh
# build and install the binary
go build -o /usr/local/bin/logma ./cmd/api

# create a dedicated, unprivileged system user
sudo useradd --system --no-create-home --shell /usr/sbin/nologin logma

# install the environment file (readable only by the logma user)
sudo mkdir -p /etc/logma
sudo cp deploy/systemd/logma.env.example /etc/logma/logma.env
sudo chown logma:logma /etc/logma/logma.env
sudo chmod 600 /etc/logma/logma.env
# edit /etc/logma/logma.env to set PORT

# install and enable the unit
sudo cp deploy/systemd/logma.service /etc/systemd/system/logma.service
sudo systemctl daemon-reload
sudo systemctl enable --now logma

# check status / logs
sudo systemctl status logma
sudo journalctl -u logma -f
```
