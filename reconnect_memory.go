package connect

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cgroupV2MemoryCeiling returns the tightest positive memory.max or memory.high
// found on the process's own cgroup or any ancestor up to the mount root.
// selfCgroup is the content of /proc/self/cgroup; only its cgroup v2 line
// ("0::/path") is used. memory.high counts because the kernel throttles and
// reclaims a cgroup that crosses it, so it is the practical ceiling even though
// memory.max is higher. ok is false when nothing limits the process, or the
// tree cannot be read (callers then fall back to cgroup v1 / MemTotal).
func cgroupV2MemoryCeiling(mount string, selfCgroup string) (ceiling int64, ok bool) {
	ceiling, _, ok = cgroupV2MemoryCeilingSource(mount, selfCgroup)
	return ceiling, ok
}

// cgroupV2MemoryCeilingSource is cgroupV2MemoryCeiling that also names the limit
// that binds, as "memory.max at /system.slice/x.service", so an operator can see
// which file set the number the provider tunes itself against.
func cgroupV2MemoryCeilingSource(mount string, selfCgroup string) (ceiling int64, source string, ok bool) {
	rel := ""
	for _, line := range strings.Split(selfCgroup, "\n") {
		if strings.HasPrefix(line, "0::") {
			rel = strings.TrimSpace(strings.TrimPrefix(line, "0::"))
			break
		}
	}
	if rel == "" {
		return 0, "", false
	}
	dir := filepath.Join(mount, rel)
	mount = filepath.Clean(mount)
	for {
		for _, name := range []string{"memory.max", "memory.high"} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				continue
			}
			v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil || v <= 0 {
				continue // "max", empty, or garbage: this file sets no limit
			}
			if !ok || v < ceiling {
				ceiling, ok = v, true
				at := strings.TrimPrefix(dir, mount)
				if at == "" {
					at = "/"
				}
				source = name + " at " + at
			}
		}
		if dir == mount || len(dir) <= len(mount) {
			return ceiling, source, ok
		}
		dir = filepath.Dir(dir)
	}
}

// cgroupMemoryWorkingSet returns memory.current minus reclaimable file cache
// (inactive_file, read from memory.stat in the same directory), floored at 0.
// On cgroup v2, memory.current includes page cache the kernel is free to
// reclaim on demand, so counting it as "used" makes a long-running unit that
// sits near its limit on cache alone look exhausted when it has plenty of
// real room. This is the kubelet/cAdvisor working-set convention. shmem is
// deliberately not subtracted: it is not reclaimable without swap (e.g. the
// RAMLOGS tmpfs). If memory.stat is missing or unparseable, cur is returned
// unchanged (conservative: no room is invented from data we could not read).
func cgroupMemoryWorkingSet(dir string, cur int64) int64 {
	data, err := os.ReadFile(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return cur
	}
	for _, line := range strings.Split(string(data), "\n") {
		k, v, ok := strings.Cut(line, " ")
		if !ok || k != "inactive_file" {
			continue
		}
		inactive, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return cur
		}
		return max(0, cur-inactive)
	}
	return cur
}

// cgroupV2MemoryHeadroom returns the free room, in bytes, before the tightest
// limit on the process's cgroup or any ancestor: the smallest
// (limit - workingSet) over every level that has a positive memory.max or
// memory.high (memory.high counts because the kernel throttles at it), where
// workingSet is memory.current minus reclaimable page cache (see
// cgroupMemoryWorkingSet). Usage above the limit is zero, never negative. A
// level whose usage cannot be read is skipped rather than guessed; a level
// whose limit cannot be paired with a usage reading makes the whole chain
// indeterminate (ok=false), because the kernel still enforces that level's
// limit even if we cannot read its usage, and adopting an ancestor's room
// would overstate what the process can use.
func cgroupV2MemoryHeadroom(mount string, selfCgroup string) (headroom int64, ok bool) {
	rel := ""
	for _, line := range strings.Split(selfCgroup, "\n") {
		if strings.HasPrefix(line, "0::") {
			rel = strings.TrimSpace(strings.TrimPrefix(line, "0::"))
			break
		}
	}
	if rel == "" {
		return 0, false
	}
	readInt := func(dir, name string) (int64, bool) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return 0, false
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		return v, err == nil && v > 0
	}
	dir := filepath.Join(mount, rel)
	mount = filepath.Clean(mount)
	for {
		limit, haveLimit := int64(0), false
		for _, name := range []string{"memory.max", "memory.high"} {
			if v, good := readInt(dir, name); good && (!haveLimit || v < limit) {
				limit, haveLimit = v, true
			}
		}
		if haveLimit {
			// memory.current can legitimately be 0 only for an empty cgroup;
			// treat an unreadable or non-numeric value as "unknown". The
			// kernel still enforces THIS level's limit, so an unknown usage
			// here must not let a looser ancestor's room speak for the
			// process: report the whole chain as indeterminate.
			data, err := os.ReadFile(filepath.Join(dir, "memory.current"))
			if err != nil {
				return 0, false
			}
			cur, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil || cur < 0 {
				return 0, false
			}
			usage := cgroupMemoryWorkingSet(dir, cur)
			room := max(0, limit-usage)
			if !ok || room < headroom {
				headroom, ok = room, true
			}
		}
		if dir == mount || len(dir) <= len(mount) {
			return headroom, ok
		}
		dir = filepath.Dir(dir)
	}
}

// CgroupMemoryHeadroomBytesForPID is CgroupMemoryHeadroomBytes for another
// process (its cgroup comes from /proc/<pid>/cgroup): the free room before the
// tightest cgroup v2 limit that process runs under. A hotswap candidate shares
// the running provider's cgroup, not the updater's.
func CgroupMemoryHeadroomBytesForPID(pid int) (int64, bool) {
	self, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return 0, false
	}
	return cgroupV2MemoryHeadroom("/sys/fs/cgroup", string(self))
}

// CgroupMemoryHeadroomBytes reports the free room before this process's
// tightest cgroup v2 memory limit, and whether any limit with a usage reading
// exists.
func CgroupMemoryHeadroomBytes() (int64, bool) {
	self, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0, false
	}
	return cgroupV2MemoryHeadroom("/sys/fs/cgroup", string(self))
}

// CgroupMemoryCeiling reports the tightest cgroup v2 memory.max/memory.high
// limiting this process, and whether any limit exists (unlike
// DetectEffectiveRAMLimitBytes it never falls back to MemTotal).
func CgroupMemoryCeiling() (int64, bool) {
	self, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0, false
	}
	return cgroupV2MemoryCeiling("/sys/fs/cgroup", string(self))
}

// memoryPressureFreeShare: pressure when less than this share of the memory this
// process may use is available.
const memoryPressureFreeShare = 0.07

// memoryPressureFrom decides from raw numbers whether memory is critically
// short. avail and total are in bytes; zero total means unknown, which is never
// pressure.
//
// The Go heap nearing GOMEMLIMIT is deliberately NOT a signal: turbo runs with
// GOGC=200 under a limit of 80% of RAM, and a GC-off box grows to the limit, so
// the heap sits near it as the runtime intends.
func memoryPressureFrom(avail uint64, total uint64) bool {
	return 0 < total && float64(avail) < memoryPressureFreeShare*float64(total)
}

// readMemoryPressure samples the memory this process can actually use: its
// cgroup's headroom when it runs under a memory limit (Docker, systemd
// MemoryMax), the host's MemAvailable otherwise. /proc/meminfo shows the host
// even inside a container, so reading only it would miss exactly the container
// that is about to be OOM-killed.
func readMemoryPressure() bool {
	if headroom, ok := CgroupMemoryHeadroomBytes(); ok {
		if ceiling, ok := CgroupMemoryCeiling(); ok && 0 < ceiling && 0 <= headroom {
			return memoryPressureFrom(uint64(headroom), uint64(ceiling))
		}
	}
	avail, total := hostMemory()
	return memoryPressureFrom(avail, total)
}

// hostMemory reads MemAvailable and MemTotal in bytes; zero when unreadable
// (non-Linux, restricted /proc), which reads as "unknown", never as pressure.
func hostMemory() (avail uint64, total uint64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemAvailable:":
			avail = kb << 10
		case "MemTotal:":
			total = kb << 10
		}
	}
	return avail, total
}
