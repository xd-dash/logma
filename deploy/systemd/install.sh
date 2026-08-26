#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BINARY_SOURCE="${1:-$SCRIPT_DIR/logma}"
SERVICE_SOURCE="${2:-$SCRIPT_DIR/logma.service}"
ENV_SOURCE="${3:-$SCRIPT_DIR/logma.env.example}"

if [[ $EUID -ne 0 ]]; then
  echo "run with sudo: sudo $0 [binary] [service] [env-example]" >&2
  exit 1
fi

[[ -f "$BINARY_SOURCE" ]] || { echo "missing binary: $BINARY_SOURCE" >&2; exit 1; }
[[ -f "$SERVICE_SOURCE" ]] || { echo "missing service: $SERVICE_SOURCE" >&2; exit 1; }
[[ -f "$ENV_SOURCE" ]] || { echo "missing env example: $ENV_SOURCE" >&2; exit 1; }

if ! id logma >/dev/null 2>&1; then
  useradd --system --no-create-home --shell /usr/sbin/nologin logma
fi

install -d -o root -g root -m 0755 /etc/logma
install -o root -g root -m 0755 "$BINARY_SOURCE" /usr/local/bin/logma
install -o root -g root -m 0644 "$SERVICE_SOURCE" /etc/systemd/system/logma.service

if [[ ! -e /etc/logma/logma.env ]]; then
  install -o logma -g logma -m 0600 "$ENV_SOURCE" /etc/logma/logma.env
  echo "created /etc/logma/logma.env from example; review it before starting Logma"
else
  echo "preserving existing /etc/logma/logma.env"
fi

systemctl daemon-reload

echo
echo "Logma files installed. The service was NOT started or restarted."
echo "Review /etc/logma/logma.env, then run:"
echo "  sudo systemctl enable --now logma"
echo "or, for an existing deployment:"
echo "  sudo systemctl restart logma"
