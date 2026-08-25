//! `setrlimit` bridge, compiled only for Unix targets with the crate's
//! off-by-default `rlimit` feature enabled.

use super::{SandboxConfig, SandboxError};

/// The integer type `getrlimit`/`setrlimit` take for the resource id
/// differs between platforms: glibc uses `__rlimit_resource_t`, every
/// other Unix libc uses a plain `c_int`.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
type RlimitResource = libc::__rlimit_resource_t;
/// See the glibc variant above.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
type RlimitResource = libc::c_int;

/// `rlim_t` is `u64` on Linux/macOS but `i64` on the BSDs, so the two
/// conversions below are load-bearing on some targets and a no-op cast on
/// others.
#[allow(clippy::unnecessary_cast)]
fn to_rlim(value: u64) -> libc::rlim_t {
    value as libc::rlim_t
}

/// See [`to_rlim`].
#[allow(clippy::unnecessary_cast)]
fn from_rlim(value: libc::rlim_t) -> u64 {
    value as u64
}

/// The platform's "no limit" sentinel, widened to `u64`.
pub(super) fn rlim_infinity() -> u64 {
    from_rlim(libc::RLIM_INFINITY)
}

/// Clamp a requested soft limit to the process's current hard limit.
///
/// Lowering is always permitted; raising above the hard limit requires
/// privileges we deliberately do not assume, so the request is capped
/// instead of failing the whole call.
pub(super) fn clamp_to_hard(requested: u64, hard: u64) -> u64 {
    if hard == rlim_infinity() {
        requested
    } else {
        requested.min(hard)
    }
}

fn set_limit(resource: RlimitResource, requested: u64) -> Result<(), SandboxError> {
    let mut current = libc::rlimit {
        rlim_cur: to_rlim(0),
        rlim_max: to_rlim(0),
    };
    // SAFETY: `getrlimit` writes into a caller-owned, fully-initialised
    // `rlimit` POD struct, and `resource` is a libc constant.
    let rc = unsafe { libc::getrlimit(resource, &mut current) };
    if rc != 0 {
        return Err(SandboxError::LimitFailed(format!(
            "getrlimit failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    let target = clamp_to_hard(requested, from_rlim(current.rlim_max));
    let new = libc::rlimit {
        rlim_cur: to_rlim(target),
        rlim_max: current.rlim_max,
    };
    // SAFETY: `new` is a fully-initialised `rlimit` POD struct owned by
    // this frame; `setrlimit` only reads through the pointer.
    let rc = unsafe { libc::setrlimit(resource, &new) };
    if rc != 0 {
        return Err(SandboxError::LimitFailed(format!(
            "setrlimit failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

pub(super) fn apply(config: &SandboxConfig) -> Result<(), SandboxError> {
    if let Some(mb) = config.max_memory_mb() {
        let bytes = (mb as u64).saturating_mul(1024 * 1024);
        set_limit(libc::RLIMIT_AS, bytes)?;
    }
    if let Some(fds) = config.max_file_descriptors() {
        set_limit(libc::RLIMIT_NOFILE, fds as u64)?;
    }
    if let Some(timeout) = config.timeout() {
        // RLIMIT_CPU is CPU-seconds, a strictly weaker bound than the
        // wall-clock timeout, but the closest honest mapping available.
        set_limit(libc::RLIMIT_CPU, timeout.as_secs().max(1))?;
    }
    Ok(())
}
