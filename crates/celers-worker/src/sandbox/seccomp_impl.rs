//! seccomp-bpf bridge, compiled only for Linux with the crate's
//! off-by-default `seccomp` feature enabled.
//!
//! # What this filter is, and is not
//!
//! It is a **process-wide hardening deny-list**, not a per-task jail. A
//! seccomp filter applies to the thread that installs it (and, with
//! `TSYNC`, its siblings) and can never be removed; tasks here are
//! in-process async futures multiplexed across the runtime's worker threads,
//! so there is no thread or process boundary that corresponds to "one task".
//! Filtering per task is therefore impossible by construction, and a filter
//! that blocked, say, `socket` would break the worker's own broker
//! connection rather than the task's.
//!
//! What *is* both safe and useful is denying the syscalls a task worker
//! never legitimately makes — module loading, `ptrace`, mount/namespace
//! manipulation, `bpf`, `perf_event_open`, keyring access, `reboot` and
//! friends. Blocking them shrinks the kernel attack surface reachable from
//! exploited task code without touching anything the worker itself does.
//!
//! The action is `SECCOMP_RET_ERRNO(EPERM)`, deliberately **not**
//! `SECCOMP_RET_KILL_*`: if this list is ever wrong, the caller sees
//! `EPERM` instead of the whole worker dying on `SIGSYS`.
//!
//! # Verification status
//!
//! This module is type-checked against both `x86_64-unknown-linux-gnu` and
//! `aarch64-unknown-linux-gnu`, and [`build_program`]'s jump encoding is
//! covered by a unit test. Neither has been *executed* on a Linux host — the
//! workspace's build machine is macOS, where the whole module is `cfg`'d out.
//! Run `cargo test -p celers-worker --features seccomp` on Linux before
//! relying on it in production.

use super::SandboxError;
use std::sync::atomic::{AtomicBool, Ordering};

/// Classic-BPF instruction, layout-compatible with `struct sock_filter`
/// from `<linux/filter.h>`. Declared here rather than taken from `libc`
/// so the exact ABI this code relies on is visible at the use site.
#[repr(C)]
#[derive(Clone, Copy)]
struct SockFilter {
    code: u16,
    jt: u8,
    jf: u8,
    k: u32,
}

/// `struct sock_fprog` from `<linux/filter.h>`.
#[repr(C)]
struct SockFprog {
    len: libc::c_ushort,
    filter: *const SockFilter,
}

// --- BPF opcodes (<linux/bpf_common.h>) ---
/// `BPF_LD | BPF_W | BPF_ABS`
const LD_W_ABS: u16 = 0x00 | 0x00 | 0x20;
/// `BPF_JMP | BPF_JEQ | BPF_K`
const JEQ_K: u16 = 0x05 | 0x10 | 0x00;
/// `BPF_RET | BPF_K`
const RET_K: u16 = 0x06 | 0x00;

// --- seccomp constants (<linux/seccomp.h>, <linux/prctl.h>) ---
/// Byte offset of `seccomp_data.nr`.
const OFFSET_NR: u32 = 0;
/// Byte offset of `seccomp_data.arch`.
const OFFSET_ARCH: u32 = 4;
const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
const SECCOMP_MODE_FILTER: libc::c_int = 2;
const PR_SET_SECCOMP: libc::c_int = 22;
const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;

/// `AUDIT_ARCH_*` from `<linux/audit.h>`, for the arch check that stops
/// a 32-bit compat entry point from bypassing the syscall-number list.
#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xc000_003e;
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xc000_00b7;

/// Installed at most once per process; a second call is a no-op success
/// because the filter is already in force and cannot be removed.
static INSTALLED: AtomicBool = AtomicBool::new(false);

/// The syscalls a CeleRS worker never makes and that an exploited task
/// would want. Everything here is available on both Linux targets this
/// crate is type-checked against (`x86_64` and `aarch64`).
fn denied_syscalls() -> Vec<libc::c_long> {
    // `mut` is used only on x86_64, which appends three arch-specific
    // entries below; aarch64 has no equivalent syscalls.
    #[allow(unused_mut)]
    let mut denied: Vec<libc::c_long> = vec![
        // Debugging / cross-process memory access
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        // Kernel module and kernel image manipulation
        libc::SYS_init_module,
        libc::SYS_finit_module,
        libc::SYS_delete_module,
        libc::SYS_kexec_load,
        libc::SYS_kexec_file_load,
        // Mount / namespace manipulation
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_chroot,
        libc::SYS_setns,
        libc::SYS_unshare,
        // Tracing / eBPF subsystems
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        // Kernel keyring
        libc::SYS_add_key,
        libc::SYS_keyctl,
        libc::SYS_request_key,
        // Filesystem handles that bypass path resolution
        libc::SYS_name_to_handle_at,
        libc::SYS_open_by_handle_at,
        // Whole-machine state
        libc::SYS_swapon,
        libc::SYS_swapoff,
        libc::SYS_reboot,
        libc::SYS_acct,
        libc::SYS_quotactl,
        // Misc privilege / memory-management escape hatches
        libc::SYS_personality,
        libc::SYS_userfaultfd,
    ];
    #[cfg(target_arch = "x86_64")]
    {
        // x86-only I/O port and LDT access.
        denied.push(libc::SYS_iopl);
        denied.push(libc::SYS_ioperm);
        denied.push(libc::SYS_modify_ldt);
    }
    denied
}

/// Build the filter program.
///
/// Layout (indices are instruction slots):
///
/// ```text
///   0            load seccomp_data.arch
///   1            if arch != AUDIT_ARCH -> deny
///   2            load seccomp_data.nr
///   3 .. 3+n-1   if nr == denied[i]    -> deny
///   3+n          return ALLOW
///   4+n          deny: return ERRNO(EPERM)
/// ```
///
/// Jump offsets are relative to the *following* instruction and are
/// single bytes, which caps the list at 253 entries — far above the ~30
/// used here, and asserted below so a future addition cannot silently
/// produce a mis-encoded program.
fn build_program(denied: &[libc::c_long]) -> Vec<SockFilter> {
    let n = denied.len();
    assert!(
        n <= 253,
        "seccomp deny-list must stay within the 8-bit BPF jump range"
    );
    let n_u8 = n as u8;

    let mut prog = Vec::with_capacity(n + 5);
    // 0: A = seccomp_data.arch
    prog.push(SockFilter {
        code: LD_W_ABS,
        jt: 0,
        jf: 0,
        k: OFFSET_ARCH,
    });
    // 1: if A == AUDIT_ARCH fall through, else jump to deny.
    prog.push(SockFilter {
        code: JEQ_K,
        jt: 0,
        jf: n_u8 + 2,
        k: AUDIT_ARCH,
    });
    // 2: A = seccomp_data.nr
    prog.push(SockFilter {
        code: LD_W_ABS,
        jt: 0,
        jf: 0,
        k: OFFSET_NR,
    });
    // 3..: one equality test per denied syscall.
    for (i, nr) in denied.iter().enumerate() {
        prog.push(SockFilter {
            code: JEQ_K,
            jt: n_u8 - i as u8,
            jf: 0,
            k: *nr as u32,
        });
    }
    // 3+n: nothing matched -> allow.
    prog.push(SockFilter {
        code: RET_K,
        jt: 0,
        jf: 0,
        k: SECCOMP_RET_ALLOW,
    });
    // 4+n: deny -> EPERM (never KILL; see the module docs).
    prog.push(SockFilter {
        code: RET_K,
        jt: 0,
        jf: 0,
        k: SECCOMP_RET_ERRNO | (libc::EPERM as u32 & 0x0000_ffff),
    });
    prog
}

/// Install the filter for this process.
///
/// Idempotent: the second and later calls succeed without touching the
/// kernel, because a seccomp filter can never be uninstalled.
pub(super) fn install() -> Result<(), SandboxError> {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return Ok(());
    }

    // `PR_SET_NO_NEW_PRIVS` is mandatory for an unprivileged
    // `PR_SET_SECCOMP`, and is what stops a set-uid binary from being
    // used to shed the filter.
    //
    // The trailing arguments are cast to `c_ulong` rather than left as
    // untyped integer literals: `prctl` is variadic and the C library
    // reads arg2..arg5 as `unsigned long`, so passing 32-bit `int`s would
    // leave the upper half of each 8-byte variadic slot undefined — and
    // the kernel rejects `PR_SET_NO_NEW_PRIVS` with `EINVAL` unless
    // arg3..arg5 are exactly zero.
    //
    // SAFETY: `prctl` with these constants takes no pointers.
    let rc = unsafe {
        libc::prctl(
            PR_SET_NO_NEW_PRIVS,
            1 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
        )
    };
    if rc != 0 {
        INSTALLED.store(false, Ordering::SeqCst);
        return Err(SandboxError::LimitFailed(format!(
            "prctl(PR_SET_NO_NEW_PRIVS) failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    let program = build_program(&denied_syscalls());
    let fprog = SockFprog {
        len: program.len() as libc::c_ushort,
        filter: program.as_ptr(),
    };
    // SAFETY: `fprog` points at `program`, which outlives this call, and
    // `len` is exactly its length. The kernel copies the program in.
    // See the `c_ulong` note above for why the trailing zeros are typed.
    let rc = unsafe {
        libc::prctl(
            PR_SET_SECCOMP,
            SECCOMP_MODE_FILTER as libc::c_ulong,
            &fprog as *const SockFprog,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
        )
    };
    if rc != 0 {
        INSTALLED.store(false, Ordering::SeqCst);
        return Err(SandboxError::LimitFailed(format!(
            "prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER) failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_layout_is_wellformed() {
        let denied = denied_syscalls();
        let prog = build_program(&denied);
        let n = denied.len();
        assert_eq!(prog.len(), n + 5);

        // The arch mismatch branch must land exactly on the deny slot.
        let deny_index = n + 4;
        assert_eq!(1 + 1 + prog[1].jf as usize, deny_index);
        // Every syscall test must land exactly on the deny slot too.
        for (i, insn) in prog[3..3 + n].iter().enumerate() {
            assert_eq!(3 + i + 1 + insn.jt as usize, deny_index);
            assert_eq!(insn.jf, 0);
        }
        assert_eq!(prog[n + 3].k, SECCOMP_RET_ALLOW);
        assert_eq!(prog[deny_index].k, SECCOMP_RET_ERRNO | libc::EPERM as u32);
    }

    #[test]
    fn deny_list_has_no_duplicates() {
        let denied = denied_syscalls();
        let mut sorted = denied.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), denied.len(), "duplicate syscall in deny-list");
    }
}
