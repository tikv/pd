// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package playgroundprobe

import (
	"context"
	"debug/elf"
	"debug/macho"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Check verifies that binPath is a non-empty regular executable file with an
// intact ELF or Mach-O binary layout (when applicable) and responds cleanly to
// `--version` before ctx expires.
func Check(ctx context.Context, binPath string, env []string) bool {
	if !IsExecutable(binPath) {
		return false
	}
	info, err := os.Stat(binPath) //nolint:gosec
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		return false
	}
	if !HasCompleteBinaryLayout(binPath, info.Size()) {
		return false
	}
	return RunVersionCheck(ctx, binPath, env) == nil
}

// IsExecutable reports whether path points to a file executable by the current user.
func IsExecutable(path string) bool {
	if path == "" {
		return false
	}
	if !filepath.IsAbs(path) && !strings.ContainsRune(path, os.PathSeparator) {
		path = "." + string(os.PathSeparator) + path
	}
	_, err := exec.LookPath(path)
	return err == nil
}

// RunVersionCheck executes `binPath --version` in its own process group and
// terminates the entire process group if ctx is canceled or times out.
func RunVersionCheck(ctx context.Context, binPath string, env []string) error {
	return newProcessGroupCommand(ctx, env, binPath, "--version").Run()
}

// LookupBinary executes `tiupBin --binary playground` in its own process group
// and returns the reported playground binary path. It terminates the entire
// process group and returns an error if ctx is canceled or times out. Any other
// TiUP failure means that playground is not installed and returns an empty path.
func LookupBinary(ctx context.Context, tiupBin string, env []string) (string, error) {
	out, err := newProcessGroupCommand(ctx, env, tiupBin, "--binary", "playground").Output()
	if err == nil {
		return strings.TrimSpace(string(out)), nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", fmt.Errorf("%s --binary playground did not finish: %w", tiupBin, ctxErr)
	}
	return "", nil
}

func newProcessGroupCommand(ctx context.Context, env []string, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec
	if len(env) > 0 {
		cmd.Env = env
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 200 * time.Millisecond
	return cmd
}

// ParseTimeout parses a probe timeout given either as a Go duration (e.g. "30s",
// "300ms") or as a positive number of seconds (e.g. "30", "0.3"). An empty value
// returns defaultTimeout.
func ParseTimeout(raw string, defaultTimeout time.Duration) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return defaultTimeout, nil
	}
	if secs, err := strconv.ParseFloat(trimmed, 64); err == nil {
		if math.IsNaN(secs) || math.IsInf(secs, 0) || secs <= 0 {
			return 0, fmt.Errorf("timeout %q must be a positive duration or number of seconds", raw)
		}
		d := time.Duration(secs * float64(time.Second))
		if d <= 0 {
			return 0, fmt.Errorf("timeout %q is too small", raw)
		}
		return d, nil
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid timeout %q: must be a positive duration or number of seconds", raw)
	}
	return d, nil
}

// HasCompleteBinaryLayout verifies that an ELF, Mach-O, or Fat Mach-O binary's
// headers, loadable segments, and on-disk sections fit within fileSize. Non-binary
// executables (such as shell scripts) return true so execution validation decides.
func HasCompleteBinaryLayout(path string, fileSize int64) bool {
	if fileSize <= 0 {
		return false
	}
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()

	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	}
	uSize := uint64(fileSize)

	switch {
	case magic == [4]byte{0x7f, 'E', 'L', 'F'}:
		ef, err := elf.NewFile(f)
		if err != nil {
			return false
		}
		return hasCompleteELFLayout(ef, uSize)
	case isMachOMagic(magic):
		mf, err := macho.NewFile(f)
		if err != nil {
			return false
		}
		return hasCompleteMachOLayout(mf, uSize)
	case isMachOFatMagic(magic):
		ff, err := macho.NewFatFile(f)
		if err != nil {
			return false
		}
		for _, arch := range ff.Arches {
			if !fitsInSize(uint64(arch.Offset), uint64(arch.Size), uSize) || !hasCompleteMachOLayout(arch.File, uint64(arch.Size)) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func hasCompleteELFLayout(ef *elf.File, fileSize uint64) bool {
	for _, p := range ef.Progs {
		if p.Filesz > 0 && !fitsInSize(p.Off, p.Filesz, fileSize) {
			return false
		}
	}
	for _, s := range ef.Sections {
		// Use FileSize (on-disk byte length) rather than Size, because Size is
		// the uncompressed length for SHF_COMPRESSED sections (e.g. Go DWARF debug info).
		if s.Type != elf.SHT_NOBITS && s.FileSize > 0 && !fitsInSize(s.Offset, s.FileSize, fileSize) {
			return false
		}
	}
	return true
}

func hasCompleteMachOLayout(mf *macho.File, fileSize uint64) bool {
	const (
		machoSectionTypeMask   = 0xff
		machoSectionZeroFill   = 0x1
		machoSectionGBZeroFill = 0xc
		machoSectionThreadZero = 0x12
	)
	for _, load := range mf.Loads {
		seg, ok := load.(*macho.Segment)
		if !ok || seg.Filesz == 0 {
			continue
		}
		if !fitsInSize(seg.Offset, seg.Filesz, fileSize) {
			return false
		}
	}
	for _, sec := range mf.Sections {
		secType := sec.Flags & machoSectionTypeMask
		if sec.Size == 0 || secType == machoSectionZeroFill || secType == machoSectionGBZeroFill || secType == machoSectionThreadZero {
			continue
		}
		if !fitsInSize(uint64(sec.Offset), sec.Size, fileSize) {
			return false
		}
	}
	return true
}

func fitsInSize(offset, length, total uint64) bool {
	return offset <= total && length <= total-offset
}

func isMachOMagic(magic [4]byte) bool {
	be := binary.BigEndian.Uint32(magic[:])
	le := binary.LittleEndian.Uint32(magic[:])
	return be == macho.Magic32 || be == macho.Magic64 || le == macho.Magic32 || le == macho.Magic64
}

func isMachOFatMagic(magic [4]byte) bool {
	return binary.BigEndian.Uint32(magic[:]) == macho.MagicFat
}
