//! System information helpers for resource monitoring.
//!
//! Reads from /proc on Linux; returns conservative defaults on other platforms.

use std::time::Duration;

/// Read current process RSS memory in bytes (from VmRSS in /proc/self/status).
///
/// Returns 0 if unavailable or not on Linux.
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
    0
}

/// Read total system physical memory in bytes (from MemTotal in /proc/meminfo).
///
/// Returns 0 if unavailable or not on Linux.
#[allow(dead_code)]
pub(crate) fn read_total_memory_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
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
    0
}

/// Number of kernel clock ticks per second (SC_CLK_TCK), typically 100 or 250.
///
/// Only defined on Linux, where it is the sole consumer (see
/// [`read_process_cpu_time`]). On other platforms `/proc` is unavailable, so
/// CPU time accounting short-circuits before this helper would ever be needed.
#[cfg(target_os = "linux")]
pub(crate) fn clock_ticks_per_sec() -> u64 {
    // SAFETY: sysconf is always safe when called with a valid constant.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 {
        return ticks as u64;
    }
    100 // sensible fallback
}

/// Read cumulative process CPU time (utime + stime from /proc/self/stat).
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
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_process_memory_bytes_returns_nonnegative() {
        // On Linux this should be > 0; elsewhere it's 0.
        let mem = read_process_memory_bytes();
        // mem >= 0 is trivially true for usize, but we assert the function runs.
        let _ = mem;
    }

    #[test]
    fn test_read_total_memory_bytes() {
        let total = read_total_memory_bytes();
        // On Linux should be > 0; on other platforms 0 is fine.
        #[cfg(target_os = "linux")]
        assert!(
            total > 0,
            "Expected total memory > 0 on Linux, got {}",
            total
        );
        let _ = total;
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_clock_ticks_per_sec() {
        let hz = clock_ticks_per_sec();
        assert!((1..=10_000).contains(&hz), "Unreasonable hz: {}", hz);
    }

    #[test]
    fn test_read_process_cpu_time() {
        // Just check it doesn't panic; on Linux it should return Some.
        let cpu = read_process_cpu_time();
        #[cfg(target_os = "linux")]
        assert!(cpu.is_some(), "Expected Some on Linux");
        let _ = cpu;
    }
}
