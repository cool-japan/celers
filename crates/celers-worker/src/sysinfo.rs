//! System information helpers for resource monitoring.
//!
//! - Linux: reads `/proc` and, for total memory, the process's cgroup
//!   limit (v2 then v1) in preference to the host-wide total. Pure Rust —
//!   no FFI is involved.
//! - macOS/BSD: reads `sysctl`/`getrusage`/`getloadavg` via `libc`, and so
//!   requires this crate's off-by-default `native-sysinfo` feature (alias:
//!   `linux-sysinfo`).
//! - Everywhere else, and everywhere with `native-sysinfo` off: returns
//!   conservative "unavailable" sentinels (`0` / `None`) rather than
//!   fabricating a number.
//!
//! Every accessor is intentionally honest about unavailability: callers
//! that feed these into ratio-based decisions (e.g. autoscaling) should
//! treat `0`/`None` as "no signal available" rather than "zero load".
//!
//! # FFI policy
//!
//! Every `libc` call in this module is gated on `feature = "native-sysinfo"`,
//! so the crate's default build declares no FFI dependency of its own (`libc`
//! still reaches the binary transitively through `tokio`, which is outside
//! this crate's control). The gate is drawn around the *calls*, not around
//! the functions: the `/proc`-based Linux readers keep working with the
//! feature off, because they never needed `libc` in the first place.

use std::time::Duration;

/// Read current process RSS memory in bytes.
///
/// - Linux: `VmRSS` from `/proc/self/status` (current resident set size).
/// - macOS: `ru_maxrss` from `getrusage(RUSAGE_SELF)` (peak resident set
///   size since process start). macOS/BSD report this field in *bytes*,
///   unlike Linux which reports the analogous `ru_maxrss` in KiB -- this
///   function normalizes both platforms' native readings to bytes.
///
/// The macOS path needs `libc`, so it is compiled only with the
/// `native-sysinfo` feature; without it macOS behaves like an unsupported
/// platform here.
///
/// Returns 0 if unavailable or on an unsupported platform. A running
/// process always has non-zero RSS, so `0` is an unambiguous
/// "unavailable" sentinel for callers.
pub(crate) fn read_process_memory_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return (kb as usize).saturating_mul(1024);
                        }
                    }
                    break;
                }
            }
        }
    }
    #[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
    {
        if let Some(usage) = macos_getrusage_self() {
            // macOS reports `ru_maxrss` in bytes (Linux reports KiB); this
            // is a well-known cross-platform `getrusage` gotcha.
            return usage.ru_maxrss.max(0) as usize;
        }
    }
    0
}

/// Read the effective total memory available to this process, in bytes.
///
/// - Linux: the process's cgroup memory limit when one is configured
///   (cgroup v2 `memory.max`, then cgroup v1 `memory.limit_in_bytes`),
///   falling back to the host's `MemTotal` from `/proc/meminfo` when no
///   cgroup limit is in effect. Preferring the cgroup limit matters inside
///   containers: a pod capped at 512MiB on a 256GiB node would otherwise
///   see the *node's* total memory as the denominator, so a
///   memory-percentage scaling trigger could never fire no matter how
///   close to its actual (much smaller) limit the process gets.
/// - macOS: `hw.memsize` via `sysctlbyname` (needs the `native-sysinfo`
///   feature; without it macOS behaves like an unsupported platform here).
///
/// Returns 0 if unavailable or on an unsupported platform.
#[allow(dead_code)]
pub(crate) fn read_total_memory_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Some(limit) = read_cgroup_memory_limit_bytes() {
            return limit as usize;
        }
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return (kb as usize).saturating_mul(1024);
                        }
                    }
                    break;
                }
            }
        }
    }
    #[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
    {
        if let Some(bytes) = macos_total_memory_bytes() {
            return bytes;
        }
    }
    0
}

/// Parse cgroup v2 `memory.max` file content.
///
/// Returns `None` when no limit is configured (the literal `"max"`) or the
/// content cannot be parsed as a byte count.
fn parse_cgroup_v2_memory_max(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed == "max" {
        None
    } else {
        trimmed.parse::<u64>().ok()
    }
}

/// Parse cgroup v1 `memory.limit_in_bytes` file content.
///
/// cgroup v1 represents "no limit configured" as an implausibly large
/// sentinel (the kernel reports `LLONG_MAX` rounded down to a page
/// boundary, ~`9.22e18`) rather than a special string, so any value at or
/// above a generous threshold (`2^62`, ~`4.6e18` -- far beyond any real
/// memory limit, comfortably below the kernel's actual sentinel) is
/// treated the same as cgroup v2's `"max"`.
fn parse_cgroup_v1_memory_limit(raw: &str) -> Option<u64> {
    const UNLIMITED_SENTINEL: u64 = 1 << 62;
    let limit: u64 = raw.trim().parse().ok()?;
    if limit < UNLIMITED_SENTINEL {
        Some(limit)
    } else {
        None
    }
}

/// Read a cgroup memory limit rooted at `cgroup_root` (v2 first, then v1).
///
/// Returns `None` when neither file is present/parseable, or the limit
/// present is "unlimited" -- callers should fall back to the host-wide
/// total in that case. Split out from [`read_cgroup_memory_limit_bytes`]
/// (which hardcodes the real `/sys/fs/cgroup` mount point) so the
/// read-and-fall-back-in-order logic can be exercised against a fake
/// directory tree in tests, on any platform, without touching the host's
/// actual cgroup filesystem.
#[allow(dead_code)] // production caller is Linux-only; also exercised directly by tests
fn read_cgroup_memory_limit_bytes_from(cgroup_root: &std::path::Path) -> Option<u64> {
    if let Ok(raw) = std::fs::read_to_string(cgroup_root.join("memory.max")) {
        // A v2 file that exists is authoritative: don't also consult v1
        // even if it reports "unlimited", since a host using the unified
        // hierarchy for this cgroup does not have a meaningful v1 view.
        return parse_cgroup_v2_memory_max(&raw);
    }
    if let Ok(raw) = std::fs::read_to_string(cgroup_root.join("memory/memory.limit_in_bytes")) {
        return parse_cgroup_v1_memory_limit(&raw);
    }
    None
}

/// Read this process's cgroup memory limit in bytes from the real
/// `/sys/fs/cgroup` mount point.
#[cfg(target_os = "linux")]
fn read_cgroup_memory_limit_bytes() -> Option<u64> {
    read_cgroup_memory_limit_bytes_from(std::path::Path::new("/sys/fs/cgroup"))
}

/// Number of kernel clock ticks per second (SC_CLK_TCK), typically 100 or 250.
///
/// Only defined on Linux, where it is the sole consumer (see
/// [`read_process_cpu_time`]). On other platforms `/proc` is unavailable, so
/// CPU time accounting short-circuits before this helper would ever be needed.
#[cfg(target_os = "linux")]
pub(crate) fn clock_ticks_per_sec() -> u64 {
    #[cfg(feature = "native-sysinfo")]
    {
        // SAFETY: sysconf is always safe when called with a valid constant.
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if ticks > 0 {
            return ticks as u64;
        }
    }
    // Reached when `native-sysinfo` is off (no `sysconf` to ask) or when the
    // call itself failed. 100 is not a guess in either case: the utime/stime
    // fields of `/proc/self/stat` are reported in USER_HZ, which the Linux
    // kernel fixes at 100 on every mainstream ABI regardless of CONFIG_HZ.
    100
}

/// Read cumulative process CPU time (utime + stime).
///
/// - Linux: parsed from `/proc/self/stat`.
/// - macOS: `ru_utime + ru_stime` via `getrusage(RUSAGE_SELF)` (needs the
///   `native-sysinfo` feature).
///
/// Returns `None` if unavailable. The returned Duration is a process-lifetime
/// cumulative value; subtract consecutive readings to get a per-interval delta.
pub(crate) fn read_process_cpu_time() -> Option<Duration> {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        // Fields are space-separated; field 14 = utime, field 15 = stime (0-indexed).
        // The process name (field 1) may contain spaces inside parentheses, so we
        // skip past the closing ')' before splitting the remainder.
        let after_paren = stat.rfind(')')?.checked_add(1)?;
        let rest = stat.get(after_paren..)?.trim_start();
        let parts: Vec<&str> = rest.split_whitespace().collect();
        // After the closing ')': field 2 = state, field 11 = utime, field 12 = stime
        // (0-indexed in `parts`). The original field numbers minus 2 because we
        // skipped pid and comm.
        let utime: u64 = parts.get(11)?.parse().ok()?;
        let stime: u64 = parts.get(12)?.parse().ok()?;
        let total_ticks = utime.saturating_add(stime);
        let hz = clock_ticks_per_sec();
        Some(Duration::from_nanos(
            total_ticks.saturating_mul(1_000_000_000) / hz,
        ))
    }
    #[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
    {
        let usage = macos_getrusage_self()?;
        let utime_ns = timeval_to_nanos(usage.ru_utime);
        let stime_ns = timeval_to_nanos(usage.ru_stime);
        Some(Duration::from_nanos(utime_ns.saturating_add(stime_ns)))
    }
    #[cfg(not(any(
        target_os = "linux",
        all(target_os = "macos", feature = "native-sysinfo")
    )))]
    {
        None
    }
}

/// Read the 1/5/15-minute system load averages.
///
/// - Linux: parsed from `/proc/loadavg`.
/// - macOS/BSD family (anywhere the POSIX-ish `getloadavg(3)` call is
///   available through `libc`): via `getloadavg`, which needs the
///   `native-sysinfo` feature.
///
/// Returns `None` on an unsupported platform or if the read/call fails.
/// This exists so that a caller reading `/proc/loadavg` directly under a
/// bare `#[cfg(unix)]` guard -- which only exists on Linux and silently
/// yields `[0.0, 0.0, 0.0]` everywhere else -- has a primitive that is
/// honest about unavailability instead. Every load-average reader in the
/// crate now goes through it: `worker_core`'s heartbeat event,
/// `worker_pool`'s load-based scaling signal, and `control`'s
/// `inspect stats` reply.
pub(crate) fn read_load_average() -> Option<[f64; 3]> {
    #[cfg(target_os = "linux")]
    {
        let raw = std::fs::read_to_string("/proc/loadavg").ok()?;
        let mut fields = raw.split_whitespace();
        let one: f64 = fields.next()?.parse().ok()?;
        let five: f64 = fields.next()?.parse().ok()?;
        let fifteen: f64 = fields.next()?.parse().ok()?;
        Some([one, five, fifteen])
    }
    #[cfg(all(
        feature = "native-sysinfo",
        any(
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "visionos",
            target_os = "freebsd",
            target_os = "dragonfly",
            target_os = "openbsd",
            target_os = "netbsd"
        )
    ))]
    {
        let mut loads: [libc::c_double; 3] = [0.0; 3];
        // SAFETY: `loads` is a valid 3-element buffer; `getloadavg` writes
        // at most `nelem` entries and returns the count actually written
        // (or -1 on failure), never writing out of bounds.
        let filled = unsafe { libc::getloadavg(loads.as_mut_ptr(), loads.len() as libc::c_int) };
        if filled == 3 {
            Some(loads)
        } else {
            None
        }
    }
    #[cfg(not(any(
        target_os = "linux",
        all(
            feature = "native-sysinfo",
            any(
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "visionos",
                target_os = "freebsd",
                target_os = "dragonfly",
                target_os = "openbsd",
                target_os = "netbsd"
            )
        )
    )))]
    {
        None
    }
}

/// `timeval` (seconds + microseconds) as total nanoseconds, saturating
/// rather than panicking/wrapping on a (never-expected-but-not-UB-to-guard)
/// negative field.
#[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
fn timeval_to_nanos(tv: libc::timeval) -> u64 {
    (tv.tv_sec.max(0) as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add((tv.tv_usec.max(0) as u64).saturating_mul(1_000))
}

/// `getrusage(RUSAGE_SELF)`, or `None` on failure.
#[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
fn macos_getrusage_self() -> Option<libc::rusage> {
    // SAFETY: `rusage` is a `#[repr(C)]` POD struct of integers/timevals;
    // a zeroed value is a valid (if meaningless) initial state, and
    // `getrusage` fully populates it on success (return code 0).
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc == 0 {
        Some(usage)
    } else {
        None
    }
}

/// `sysctlbyname("hw.memsize")`, or `None` on failure.
#[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
fn macos_total_memory_bytes() -> Option<usize> {
    let mut size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let name = b"hw.memsize\0";
    // SAFETY: `name` is a valid NUL-terminated C string; `size`/`len`
    // describe a correctly-sized output buffer for the u64 the sysctl
    // reports, and `sysctlbyname` writes at most `len` bytes into it.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr() as *const libc::c_char,
            &mut size as *mut u64 as *mut libc::c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 {
        Some(size as usize)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The platforms on which the accessors are expected to return a real
    /// reading: Linux always (pure `/proc`), macOS only with the
    /// `native-sysinfo` feature that compiles its `libc` calls in.
    macro_rules! has_real_readings {
        () => {
            cfg!(any(
                target_os = "linux",
                all(target_os = "macos", feature = "native-sysinfo")
            ))
        };
    }

    #[test]
    fn test_read_process_memory_bytes_returns_nonnegative() {
        let mem = read_process_memory_bytes();
        // mem >= 0 is trivially true for usize; the point is that the
        // function runs without panicking on every platform and feature
        // combination.
        if has_real_readings!() {
            assert!(
                mem > 0,
                "expected nonzero process RSS on a supported platform, got {mem}"
            );
        }
    }

    #[test]
    fn test_read_total_memory_bytes() {
        let total = read_total_memory_bytes();
        if has_real_readings!() {
            assert!(
                total > 0,
                "Expected total memory > 0 on a supported platform, got {total}"
            );
        }
    }

    /// With `native-sysinfo` off there is no `libc` to call, so the macOS
    /// accessors must return the module's documented "unavailable" sentinel
    /// rather than a fabricated number. This is the fallback half of the
    /// FFI gate, and it is what the crate's *default* build compiles.
    #[cfg(all(target_os = "macos", not(feature = "native-sysinfo")))]
    #[test]
    fn test_macos_accessors_report_unavailable_without_native_sysinfo() {
        assert_eq!(read_process_memory_bytes(), 0);
        assert_eq!(read_total_memory_bytes(), 0);
        assert_eq!(read_process_cpu_time(), None);
        assert_eq!(read_load_average(), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_clock_ticks_per_sec() {
        let hz = clock_ticks_per_sec();
        assert!((1..=10_000).contains(&hz), "Unreasonable hz: {}", hz);
    }

    #[test]
    fn test_read_process_cpu_time() {
        // Just check it doesn't panic; where a real source exists it must
        // return Some.
        let cpu = read_process_cpu_time();
        if has_real_readings!() {
            assert!(cpu.is_some(), "Expected Some on a supported platform");
        }
    }

    #[cfg(any(
        target_os = "linux",
        all(target_os = "macos", feature = "native-sysinfo")
    ))]
    #[test]
    fn test_read_load_average_returns_plausible_values() {
        let loads =
            read_load_average().expect("getloadavg/proc-loadavg should succeed on this platform");
        for load in loads {
            assert!(
                (0.0..1_000_000.0).contains(&load),
                "implausible load average: {load}"
            );
        }
    }

    // --- Regression tests: cgroup-aware total memory (idx 180) -----------

    #[test]
    fn test_parse_cgroup_v2_memory_max_limit() {
        assert_eq!(parse_cgroup_v2_memory_max("536870912\n"), Some(536_870_912));
    }

    #[test]
    fn test_parse_cgroup_v2_memory_max_unlimited() {
        // "max" (no limit configured) must not be mistaken for a literal
        // memory limit of some huge parsed value -- it has no numeric
        // parse at all, and is treated as "no limit".
        assert_eq!(parse_cgroup_v2_memory_max("max\n"), None);
    }

    #[test]
    fn test_parse_cgroup_v2_memory_max_garbage() {
        assert_eq!(parse_cgroup_v2_memory_max("not-a-number"), None);
    }

    #[test]
    fn test_parse_cgroup_v1_memory_limit_real_limit() {
        assert_eq!(
            parse_cgroup_v1_memory_limit("1073741824\n"),
            Some(1_073_741_824)
        );
    }

    /// Regression test: cgroup v1's "unlimited" sentinel is an
    /// implausibly large *number* (not a keyword like v2's "max"), close
    /// to `i64::MAX` rounded down to a page boundary. Treating this
    /// number as a literal 8.4-exabyte memory limit would make the
    /// memory-usage percentage permanently ~0%, reproducing the exact
    /// container-blindness bug this fix targets.
    #[test]
    fn test_parse_cgroup_v1_memory_limit_unlimited_sentinel() {
        assert_eq!(
            parse_cgroup_v1_memory_limit("9223372036854771712"),
            None,
            "the kernel's 'no limit' sentinel must not be reported as a real limit"
        );
    }

    #[test]
    fn test_parse_cgroup_v1_memory_limit_garbage() {
        assert_eq!(parse_cgroup_v1_memory_limit(""), None);
        assert_eq!(parse_cgroup_v1_memory_limit("not-a-number"), None);
    }

    /// A scratch directory under `std::env::temp_dir()`, unique per call,
    /// removed on drop. Used to fabricate a fake cgroup filesystem tree
    /// without touching the host's real `/sys/fs/cgroup`.
    struct ScratchDir(std::path::PathBuf);

    impl ScratchDir {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "celers-sysinfo-cgroup-test-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&dir).expect("create scratch dir");
            Self(dir)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn test_read_cgroup_memory_limit_prefers_v2_when_present() {
        let scratch = ScratchDir::new("v2-present");
        std::fs::write(scratch.0.join("memory.max"), "268435456\n").unwrap();
        // Also drop a v1-style legacy tree with a *different* value, to
        // prove v2 wins rather than being averaged/overridden.
        std::fs::create_dir_all(scratch.0.join("memory")).unwrap();
        std::fs::write(
            scratch.0.join("memory/memory.limit_in_bytes"),
            "999999999\n",
        )
        .unwrap();

        assert_eq!(
            read_cgroup_memory_limit_bytes_from(&scratch.0),
            Some(268_435_456)
        );
    }

    #[test]
    fn test_read_cgroup_memory_limit_falls_back_to_v1_when_v2_absent() {
        let scratch = ScratchDir::new("v1-only");
        std::fs::create_dir_all(scratch.0.join("memory")).unwrap();
        std::fs::write(scratch.0.join("memory/memory.limit_in_bytes"), "134217728").unwrap();

        assert_eq!(
            read_cgroup_memory_limit_bytes_from(&scratch.0),
            Some(134_217_728)
        );
    }

    #[test]
    fn test_read_cgroup_memory_limit_none_when_v2_unlimited_even_if_v1_present() {
        let scratch = ScratchDir::new("v2-unlimited");
        std::fs::write(scratch.0.join("memory.max"), "max").unwrap();
        std::fs::create_dir_all(scratch.0.join("memory")).unwrap();
        std::fs::write(scratch.0.join("memory/memory.limit_in_bytes"), "134217728").unwrap();

        // v2 is authoritative once present, even reporting "unlimited":
        // must not silently fall through to a stale/irrelevant v1 view.
        assert_eq!(read_cgroup_memory_limit_bytes_from(&scratch.0), None);
    }

    #[test]
    fn test_read_cgroup_memory_limit_none_when_neither_file_present() {
        let scratch = ScratchDir::new("neither-present");
        assert_eq!(read_cgroup_memory_limit_bytes_from(&scratch.0), None);
    }

    // --- macOS-specific regression coverage (idx 180) ---------------------
    //
    // These run for real on a macOS host (this workspace's own build
    // machine is macOS), directly verifying the sysctl/getrusage-backed
    // implementations rather than only reasoning about them. They need the
    // `native-sysinfo` feature, which is what compiles those `libc` calls in
    // at all; the feature-off behaviour is covered by
    // `test_macos_accessors_report_unavailable_without_native_sysinfo`.

    #[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
    #[test]
    fn test_macos_total_memory_bytes_is_plausible() {
        let bytes = macos_total_memory_bytes().expect("sysctlbyname(hw.memsize) should succeed");
        // Any real or virtualized Mac has at least 1GiB of RAM.
        assert!(
            bytes >= 1024 * 1024 * 1024,
            "implausible total memory: {bytes}"
        );
    }

    #[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
    #[test]
    fn test_macos_getrusage_self_reports_nonzero_rss() {
        let usage = macos_getrusage_self().expect("getrusage(RUSAGE_SELF) should succeed");
        assert!(
            usage.ru_maxrss > 0,
            "a running process must have nonzero RSS"
        );
    }

    #[cfg(all(target_os = "macos", feature = "native-sysinfo"))]
    #[test]
    fn test_read_process_memory_bytes_matches_getrusage_on_macos() {
        // End-to-end: the public accessor must actually be wired to the
        // macOS-specific implementation, not silently falling through to
        // the old "return 0 off Linux" behavior.
        assert!(read_process_memory_bytes() > 0);
    }
}
