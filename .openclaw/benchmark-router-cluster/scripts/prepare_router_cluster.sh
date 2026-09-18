#!/usr/bin/env bash
set -euo pipefail

# Benchmark-router-cluster preflight bootstrap.
# Installs runtime dependencies and bootstraps tiup/tiup-cluster only.
# Intentionally excludes pd/tidb/tikv binary builds and MinIO setup.

TARGET_HOST=""
SSH_USER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-host) TARGET_HOST="$2"; shift 2 ;;
    --ssh-user) SSH_USER="$2"; shift 2 ;;
    *) echo "[FAIL] unknown arg: $1"; exit 1 ;;
  esac
done

if [[ -z "$TARGET_HOST" || -z "$SSH_USER" ]]; then
  echo "[FAIL] missing required args: --target-host --ssh-user"
  exit 1
fi

REMOTE="${SSH_USER}@${TARGET_HOST}"

ok() { echo "[OK] $*"; }
skip() { echo "[SKIP] $*"; }
fail() { echo "[FAIL] $*"; exit 1; }
run_remote() { ssh "$REMOTE" "$@"; }

step() { echo "\n-- $* --"; }

echo "== router cluster prepare(start): runtime + tiup bootstrap =="
step "S1 SSH connectivity"
if run_remote "hostname" >/tmp/router_prepare_host.out 2>/tmp/router_prepare_host.err; then
  ok "S1 host reachable: $(tr -d '\n' </tmp/router_prepare_host.out)"
else
  fail "S1 ssh connectivity failed: $(cat /tmp/router_prepare_host.err)"
fi

step "S2 Runtime install + PATH + tiup bootstrap"
run_remote 'bash -se' <<'EOS'
set -euo pipefail
ok(){ echo "[OK] $*"; }
skip(){ echo "[SKIP] $*"; }
fail(){ echo "[FAIL] $*"; exit 1; }
sub(){ echo "   - $*"; }

sub "S2.1 detect package manager"
if command -v apt-get >/dev/null 2>&1; then
  ok "S2.1 package manager: apt"
  export DEBIAN_FRONTEND=noninteractive
  sub "S2.2 apt update"
  sudo apt-get update -y >/dev/null || fail "S2.2 apt update failed"
  ok "S2.2 apt update done"
  sub "S2.3 install runtime packages (sysbench/mysql-client/git/make/gcc/curl/tar)"
  sudo apt-get install -y sysbench default-mysql-client git make gcc g++ curl tar >/dev/null || fail "S2.3 apt install failed"
  ok "S2.3 runtime packages installed"
elif command -v yum >/dev/null 2>&1; then
  ok "S2.1 package manager: yum"
  sub "S2.3 install runtime packages (sysbench/mysql/git/make/gcc/curl/tar)"
  sudo yum install -y sysbench mysql git make gcc gcc-c++ curl tar >/dev/null || fail "S2.3 yum install failed"
  ok "S2.3 runtime packages installed"
else
  skip "S2.1 unknown package manager; skip package install"
fi

sub "S2.4 install latest golang from go.dev"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) GOARCH="amd64" ;;
  aarch64|arm64) GOARCH="arm64" ;;
  *) fail "S2.4 unsupported architecture for Go install: $ARCH" ;;
esac
GO_VER="$(curl -fsSL https://go.dev/VERSION?m=text | head -n1)" || fail "S2.4 fetch latest go version failed"
GO_TARBALL="${GO_VER}.linux-${GOARCH}.tar.gz"
curl -fsSL "https://go.dev/dl/${GO_TARBALL}" -o "/tmp/${GO_TARBALL}" || fail "S2.4 download ${GO_TARBALL} failed"
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf "/tmp/${GO_TARBALL}" || fail "S2.4 extract go tarball failed"
/usr/local/go/bin/go version >/dev/null || fail "S2.4 go install verification failed"
export PATH="/usr/local/go/bin:$PATH"
hash -r
ok "S2.4 latest golang installed: $(go version)"

sub "S2.5 persist PATH for runtime commands"
export_line='export PATH="/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin:$PATH"'
touch ~/.bashrc ~/.profile
grep -qF "$export_line" ~/.bashrc || echo "$export_line" >> ~/.bashrc
grep -qF "$export_line" ~/.profile || echo "$export_line" >> ~/.profile
ok "S2.5 PATH persisted to ~/.bashrc and ~/.profile"

sub "S2.6 verify sysbench/mysql/go commands"
bash -lc 'command -v sysbench >/dev/null && command -v mysql >/dev/null && command -v go >/dev/null' || fail "S2.6 command verify failed"
ok "S2.6 sysbench/mysql/go available"

sub "S2.7 verify tiup/tiup-cluster or bootstrap from ~/tiup"
if command -v tiup >/dev/null 2>&1 && command -v tiup-cluster >/dev/null 2>&1; then
  skip "S2.7 tiup and tiup-cluster already exist; skip tiup bootstrap"
elif command -v tiup-cluster >/dev/null 2>&1; then
  skip "S2.7 tiup-cluster already exists; skip tiup bootstrap"
else
  sub "S2.7.1 tiup-cluster missing; check/clone ~/tiup repo"
  if [ -d ~/tiup/.git ]; then
    (cd ~/tiup && git fetch --all --tags --prune >/dev/null) || fail "S2.7.1 fetch ~/tiup failed"
    ok "S2.7.1 ~/tiup repo fetched"
  else
    rm -rf ~/tiup
    git clone https://github.com/pingcap/tiup.git ~/tiup >/dev/null 2>&1 || fail "S2.7.1 clone ~/tiup failed"
    ok "S2.7.1 ~/tiup repo cloned"
  fi

  sub "S2.7.2 build ~/tiup when ~/tiup/bin empty"
  if [ -d ~/tiup/bin ] && [ "$(ls -A ~/tiup/bin 2>/dev/null | wc -l)" -gt 0 ]; then
    skip "S2.7.2 ~/tiup/bin already non-empty"
  else
    sub "S2.7.2.1 run go mod tidy before make"
    (cd ~/tiup && go mod tidy) || fail "S2.7.2.1 go mod tidy failed"
    ok "S2.7.2.1 go mod tidy done"
    (cd ~/tiup && make tiup cluster) || fail "S2.7.2 tiup targeted build failed (make tiup cluster)"
    ok "S2.7.2 tiup and tiup-cluster built by make tiup cluster"
  fi

  sub "S2.7.3 export ~/tiup/bin PATH now + persist"
  export PATH="$HOME/tiup/bin:$PATH"
  export_line_tiup='export PATH="$HOME/tiup/bin:$PATH"'
  grep -qF "$export_line_tiup" ~/.bashrc || echo "$export_line_tiup" >> ~/.bashrc
  grep -qF "$export_line_tiup" ~/.profile || echo "$export_line_tiup" >> ~/.profile
  command -v tiup-cluster >/dev/null || fail "S2.7.3 current shell tiup-cluster missing"
  bash -lc 'command -v tiup-cluster >/dev/null' || fail "S2.7.3 login shell tiup-cluster missing"
  ok "S2.7.3 tiup-cluster available and PATH persisted"
fi
EOS

echo "== router cluster prepare(done): runtime + tiup bootstrap =="
