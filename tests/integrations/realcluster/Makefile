# Copyright 2023 TiKV Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ROOT_PATH := $(shell pwd)/../../..
GO_TOOLS_BIN_PATH := $(ROOT_PATH)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

static: install-tools
	@ echo "gofmt ..."
	@ gofmt -s -l -d . 2>&1 | awk '{ print } END { if (NR > 0) { exit 1 } }'
	@ echo "golangci-lint ..."
	@ golangci-lint run -c $(ROOT_PATH)/.golangci.yml --verbose ./... --allow-parallel-runners
	@ echo "modernize ..."
	@ go run golang.org/x/tools/go/analysis/passes/modernize/cmd/modernize@v0.52.0 -test ./...

tidy:
	@ go mod tidy
	git diff go.mod go.sum | cat
	git diff --quiet go.mod go.sum

check: tiup test

define PLAYGROUND_PROBE_PL
use strict;
use warnings;
use POSIX qw(:sys_wait_h);
use Time::HiRes qw(time sleep);

my ($$bin, $$timeout) = @ARGV;
$$timeout = 30 if !defined($$timeout) || $$timeout !~ /^(?:\d+(?:\.\d+)?|\.\d+)$$/ || $$timeout <= 0;
exit 1 unless defined($$bin) && -f $$bin && -s $$bin && -x $$bin;

my $$size = -s $$bin;
open my $$fh, "<:raw", $$bin or exit 1;
my $$magic = "";
my $$n = read($$fh, $$magic, 4);
if (defined($$n) && $$n == 4) {
    my $$fits = sub { my ($$off, $$len, $$tot) = @_; return $$off <= $$tot && $$len <= $$tot - $$off; };
    if ($$magic eq "\x7fELF") {
        seek($$fh, 0, 0) or exit 1;
        my $$hdr = "";
        read($$fh, $$hdr, 64) >= 52 or exit 1;
        my ($$cls, $$endi) = unpack("x4 C C", $$hdr);
        exit 1 unless ($$cls == 1 || $$cls == 2) && ($$endi == 1 || $$endi == 2);
        my ($$phoff, $$shoff, $$phent, $$phnum, $$shent, $$shnum);
        if ($$cls == 2) {
            length($$hdr) >= 64 or exit 1;
            my $$fmt = $$endi == 1 ? "x32 Q< Q< x6 v v v v" : "x32 Q> Q> x6 n n n n";
            ($$phoff, $$shoff, $$phent, $$phnum, $$shent, $$shnum) = unpack($$fmt, $$hdr);
        } else {
            my $$fmt = $$endi == 1 ? "x28 V V x6 v v v v" : "x28 N N x6 n n n n";
            ($$phoff, $$shoff, $$phent, $$phnum, $$shent, $$shnum) = unpack($$fmt, $$hdr);
        }
        if ($$phnum > 0) {
            exit 1 unless $$phent >= ($$cls == 2 ? 56 : 32) && $$fits->($$phoff, $$phnum * $$phent, $$size);
            for my $$i (0 .. $$phnum - 1) {
                seek($$fh, $$phoff + $$i * $$phent, 0) or exit 1;
                my $$buf = "";
                read($$fh, $$buf, $$phent) == $$phent or exit 1;
                my ($$poff, $$pfsz) = $$cls == 2
                    ? unpack($$endi == 1 ? "x8 Q< x16 Q<" : "x8 Q> x16 Q>", $$buf)
                    : unpack($$endi == 1 ? "x4 V x8 V" : "x4 N x8 N", $$buf);
                exit 1 if $$pfsz > 0 && !$$fits->($$poff, $$pfsz, $$size);
            }
        }
        if ($$shnum > 0) {
            exit 1 unless $$shent >= ($$cls == 2 ? 64 : 40) && $$fits->($$shoff, $$shnum * $$shent, $$size);
            for my $$i (0 .. $$shnum - 1) {
                seek($$fh, $$shoff + $$i * $$shent, 0) or exit 1;
                my $$buf = "";
                read($$fh, $$buf, $$shent) == $$shent or exit 1;
                my ($$stype, $$soff, $$ssz) = $$cls == 2
                    ? unpack($$endi == 1 ? "x4 V x16 Q< Q<" : "x4 N x16 Q> Q>", $$buf)
                    : unpack($$endi == 1 ? "x4 V x8 V V" : "x4 N x8 N N", $$buf);
                exit 1 if $$stype != 8 && $$ssz > 0 && !$$fits->($$soff, $$ssz, $$size);
            }
        }
    } elsif ($$magic eq "\xfe\xed\xfa\xce" || $$magic eq "\xce\xfa\xed\xfe" ||
             $$magic eq "\xfe\xed\xfa\xcf" || $$magic eq "\xcf\xfa\xed\xfe" ||
             $$magic eq "\xca\xfe\xba\xbe") {
        my $$check_macho = sub {
            my ($$base, $$len) = @_;
            seek($$fh, $$base, 0) or return 0;
            my $$mh = "";
            read($$fh, $$mh, 32) >= 28 or return 0;
            my $$m = substr($$mh, 0, 4);
            my ($$is64, $$le);
            if ($$m eq "\xcf\xfa\xed\xfe") { ($$is64, $$le) = (1, 1); }
            elsif ($$m eq "\xfe\xed\xfa\xcf") { ($$is64, $$le) = (1, 0); }
            elsif ($$m eq "\xce\xfa\xed\xfe") { ($$is64, $$le) = (0, 1); }
            elsif ($$m eq "\xfe\xed\xfa\xce") { ($$is64, $$le) = (0, 0); }
            else { return 0; }
            my $$hdr_len = $$is64 ? 32 : 28;
            return 0 if length($$mh) < $$hdr_len;
            my ($$ncmds, $$sizeofcmds) = unpack($$le ? "x16 V V" : "x16 N N", $$mh);
            return 0 unless $$fits->($$hdr_len, $$sizeofcmds, $$len);
            my $$pos = $$hdr_len;
            for (my $$i = 0; $$i < $$ncmds; $$i++) {
                return 0 unless $$fits->($$pos, 8, $$len);
                seek($$fh, $$base + $$pos, 0) or return 0;
                my $$ch = "";
                read($$fh, $$ch, 8) == 8 or return 0;
                my ($$cmd, $$cmdsize) = unpack($$le ? "V V" : "N N", $$ch);
                return 0 if $$cmdsize < 8 || !$$fits->($$pos, $$cmdsize, $$len);
                if (($$cmd == 0x19 && $$is64) || ($$cmd == 0x1 && !$$is64)) {
                    my $$seg_len = $$is64 ? 72 : 56;
                    return 0 if $$cmdsize < $$seg_len;
                    my $$sb = "";
                    seek($$fh, $$base + $$pos, 0) or return 0;
                    read($$fh, $$sb, $$cmdsize) == $$cmdsize or return 0;
                    my ($$foff, $$fsz, $$nsects) = $$is64
                        ? unpack($$le ? "x40 Q< Q< x8 V" : "x40 Q> Q> x8 N", $$sb)
                        : unpack($$le ? "x32 V V x8 V" : "x32 N N x8 N", $$sb);
                    return 0 if $$fsz > 0 && !$$fits->($$foff, $$fsz, $$len);
                    my $$sec_len = $$is64 ? 80 : 68;
                    return 0 unless $$fits->($$seg_len, $$nsects * $$sec_len, $$cmdsize);
                    for (my $$j = 0; $$j < $$nsects; $$j++) {
                        my $$sec = substr($$sb, $$seg_len + $$j * $$sec_len, $$sec_len);
                        my ($$ssz, $$soff, $$flags) = $$is64
                            ? unpack($$le ? "x40 Q< V x12 V" : "x40 Q> N x12 N", $$sec)
                            : unpack($$le ? "x36 V V x12 V" : "x36 N N x12 N", $$sec);
                        my $$st = $$flags & 0xff;
                        next if $$ssz == 0 || $$st == 0x1 || $$st == 0xc || $$st == 0x12;
                        return 0 unless $$fits->($$soff, $$ssz, $$len);
                    }
                }
                $$pos += $$cmdsize;
            }
            return 1;
        };
        if ($$magic eq "\xca\xfe\xba\xbe") {
            seek($$fh, 0, 0) or exit 1;
            my $$fhdr = "";
            read($$fh, $$fhdr, 8) == 8 or exit 1;
            my $$narch = unpack("x4 N", $$fhdr);
            exit 1 unless $$narch > 0 && $$fits->(8, $$narch * 20, $$size);
            for my $$i (0 .. $$narch - 1) {
                my $$ab = "";
                read($$fh, $$ab, 20) == 20 or exit 1;
                my ($$aoff, $$asz) = unpack("x8 N N", $$ab);
                exit 1 unless $$fits->($$aoff, $$asz, $$size) && $$check_macho->($$aoff, $$asz);
            }
        } else {
            exit 1 unless $$check_macho->(0, $$size);
        }
    }
}
close $$fh;

my $$pid = fork();
exit 1 unless defined $$pid;
if ($$pid == 0) {
    setpgrp(0, 0);
    open STDIN, "<", "/dev/null";
    open STDOUT, ">", "/dev/null";
    open STDERR, ">", "/dev/null";
    { exec { $$bin } $$bin, "--version"; }
    POSIX::_exit(127);
}
my $$deadline = time() + $$timeout;
while (1) {
    my $$r = waitpid($$pid, WNOHANG);
    if ($$r == $$pid) {
        exit($$? == 0 ? 0 : 1);
    }
    if ($$r < 0) {
        exit 1;
    }
    if (time() >= $$deadline) {
        kill "KILL", -$$pid;
        kill "KILL", $$pid;
        waitpid($$pid, 0);
        exit 124;
    }
    sleep 0.02;
}
endef
export PLAYGROUND_PROBE_PL

tiup:
	@echo "==> Checking tiup installation"
	@set -eu; \
	TIUP_DIR="$${TIUP_HOME:-$(HOME)/.tiup}"; \
	mkdir -p "$$TIUP_DIR"; \
	TIUP_DIR="$$(cd "$$TIUP_DIR" && pwd -P)"; \
	if [ -n "$${TIUP_HOME:-}" ]; then \
		export TIUP_HOME="$$TIUP_DIR"; \
	fi; \
	resolve_tiup_bin() { \
		if [ -x "$$TIUP_DIR/bin/tiup" ]; then \
			printf '%s\n' "$$TIUP_DIR/bin/tiup"; \
		elif [ -x "$$HOME/.tiup/bin/tiup" ]; then \
			printf '%s\n' "$$HOME/.tiup/bin/tiup"; \
		else \
			printf '%s\n' "$$TIUP_DIR/bin/tiup"; \
		fi; \
	}; \
	is_playground_usable() { \
		local pg_bin="$$1"; \
		local probe_timeout="$${TIUP_PROBE_TIMEOUT:-30}"; \
		if [ -z "$$pg_bin" ] || [ ! -f "$$pg_bin" ] || [ ! -s "$$pg_bin" ] || [ ! -x "$$pg_bin" ]; then \
			return 1; \
		fi; \
		if command -v perl >/dev/null 2>&1; then \
			perl -e "$$PLAYGROUND_PROBE_PL" "$$pg_bin" "$$probe_timeout"; \
		elif command -v timeout >/dev/null 2>&1; then \
			timeout -k 1 "$$probe_timeout" "$$pg_bin" --version >/dev/null 2>&1; \
		else \
			( "$$pg_bin" --version >/dev/null 2>&1 ) 2>/dev/null; \
		fi; \
	}; \
	TIUP_BIN="$$(resolve_tiup_bin)"; \
	PG_BIN=""; \
	if [ -x "$$TIUP_BIN" ]; then \
		PG_BIN="$$("$$TIUP_BIN" --binary playground 2>/dev/null || true)"; \
	fi; \
	if [ -x "$$TIUP_BIN" ] && is_playground_usable "$$PG_BIN"; then \
		echo "tiup already installed"; \
		echo "tiup version: $$("$$TIUP_BIN" --version)"; \
	else \
		LOCK_FILE="$$TIUP_DIR/.realcluster-playground-install.lock"; \
		run_install() { \
			set -eu; \
			TIUP_BIN="$$(resolve_tiup_bin)"; \
			if [ ! -x "$$TIUP_BIN" ] || [ ! -f "$$TIUP_DIR/bin/root.json" ]; then \
				echo "Installing tiup..."; \
				curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh; \
				TIUP_BIN="$$(resolve_tiup_bin)"; \
			else \
				echo "tiup already installed"; \
			fi; \
			echo "tiup version: $$("$$TIUP_BIN" --version)"; \
			PG_BIN="$$("$$TIUP_BIN" --binary playground 2>/dev/null || true)"; \
			if ! is_playground_usable "$$PG_BIN"; then \
				if [ -n "$$PG_BIN" ]; then \
					PG_VER_DIR="$$(dirname "$$PG_BIN")"; \
					PG_PARENT="$$(cd "$$(dirname "$$PG_VER_DIR")" 2>/dev/null && pwd -P || true)"; \
					PG_ROOT="$$(cd "$$TIUP_DIR/components/playground" 2>/dev/null && pwd -P || true)"; \
					if [ -n "$$PG_PARENT" ] && [ "$$PG_PARENT" = "$$PG_ROOT" ] && [ "$$(basename "$$PG_VER_DIR")" != "." ] && [ "$$(basename "$$PG_VER_DIR")" != ".." ]; then \
						echo "Removing incomplete tiup playground directory $$PG_VER_DIR..."; \
						rm -rf "$$PG_VER_DIR"; \
					fi; \
				fi; \
				echo "Installing tiup playground..."; \
				"$$TIUP_BIN" install playground; \
				PG_BIN="$$("$$TIUP_BIN" --binary playground 2>/dev/null || true)"; \
				if ! is_playground_usable "$$PG_BIN"; then \
					echo "tiup playground binary is missing or not runnable after install: $$PG_BIN" >&2; \
					exit 1; \
				fi; \
			fi; \
		}; \
		if command -v flock >/dev/null 2>&1; then \
			(set -eu; exec 9>>"$$LOCK_FILE" && flock -x 9 && run_install); \
		elif command -v perl >/dev/null 2>&1; then \
			export -f resolve_tiup_bin is_playground_usable run_install; \
			export TIUP_DIR; \
			perl -MFcntl=:flock -e 'open my $$fh, ">>", shift @ARGV or die $$!; flock($$fh, LOCK_EX) or die $$!; my $$rc = system(@ARGV); exit($$rc == -1 ? 127 : ($$rc & 127 ? 128 + ($$rc & 127) : $$rc >> 8))' "$$LOCK_FILE" bash -c 'run_install'; \
		else \
			echo "Neither flock nor perl is available to lock $$LOCK_FILE" >&2; \
			exit 1; \
		fi; \
	fi

deploy: kill_cluster deploy_only

deploy_only:
	@ echo "deploying..."
	./deploy.sh
	@ echo "wait cluster ready..."
	./wait_tiup.sh 15 20
	@ echo "check cluster status..."
	@ pid=$$(ps -ef | grep 'playground' | grep -v grep | awk '{print $$2}' | head -n 1); \
	echo $$pid;

kill_cluster:
	@ echo "kill cluster..."
	@ pid=$$(ps -ef | grep 'playground' | grep -v grep | awk '{print $$2}' | head -n 1); \
	if [ ! -z "$$pid" ]; then \
		echo $$pid; \
		kill $$pid; \
		echo "waiting for cluster to exit..."; \
		sleep 30; \
	fi

test: tiup
	CGO_ENABLED=1 go test ./... -v -tags deadlock -race -cover -timeout 20m || (\
	echo "The following is pd-0 log" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-0/pd.log ; \
	echo "The following is pd-1 log" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-1/pd.log ; \
	echo "The following is pd-2 log" ; \
	cat ~/.tiup/data/pd_real_cluster_test/pd-2/pd.log ; \
	exit 1)

install-tools:
	cd $(ROOT_PATH) && $(MAKE) install-tools

.PHONY: tidy static check tiup deploy deploy_only kill_cluster test install-tools
