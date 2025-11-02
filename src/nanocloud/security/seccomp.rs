use crate::nanocloud::util::error::{new_error, with_context};
use libc::{sock_filter, sock_fprog};
use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::str::FromStr;

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone)]
pub struct SeccompFilter {
    denied_syscalls: Vec<i64>,
}

#[derive(Deserialize)]
struct SeccompProfileData {
    deny: Vec<String>,
}

impl SeccompFilter {
    pub fn from_path(path: &Path) -> Result<Self, DynError> {
        let contents = fs::read_to_string(path)
            .map_err(|err| with_context(err, format!("Failed to read {}", path.display())))?;
        Self::from_str(&contents)
    }

    fn parse(raw: &str) -> Result<Self, DynError> {
        let data: SeccompProfileData = serde_json::from_str(raw)
            .map_err(|err| new_error(format!("Invalid seccomp profile: {}", err)))?;
        let mut denied = Vec::with_capacity(data.deny.len());
        for entry in data.deny {
            let normalized = entry.trim().to_lowercase();
            let number = syscall_number(&normalized).ok_or_else(|| {
                new_error(format!(
                    "Unknown syscall '{}' referenced by seccomp profile",
                    normalized
                ))
            })?;
            denied.push(number);
        }
        Ok(Self {
            denied_syscalls: denied,
        })
    }

    pub fn apply(&self) -> Result<(), DynError> {
        if self.denied_syscalls.is_empty() {
            return Ok(());
        }

        const BPF_LD: u16 = 0x00;
        const BPF_W: u16 = 0x00;
        const BPF_ABS: u16 = 0x20;
        const BPF_JMP: u16 = 0x05;
        const BPF_JEQ: u16 = 0x10;
        const BPF_K: u16 = 0x00;
        const BPF_RET: u16 = 0x06;

        const LD_SYSCALL_NR: u16 = BPF_LD | BPF_W | BPF_ABS;
        const JMP_EQ: u16 = BPF_JMP | BPF_JEQ | BPF_K;
        const RET: u16 = BPF_RET | BPF_K;
        const ERR_ACTION: u32 = libc::SECCOMP_RET_ERRNO | ((libc::EPERM as u32) & 0xFFFF);
        const ALLOW_ACTION: u32 = libc::SECCOMP_RET_ALLOW;

        let mut filters = Vec::with_capacity(self.denied_syscalls.len() * 2 + 2);
        filters.push(bpf_stmt(LD_SYSCALL_NR, 0));
        for sysno in &self.denied_syscalls {
            filters.push(bpf_jump(JMP_EQ, *sysno as u32, 0, 1));
            filters.push(bpf_stmt(RET, ERR_ACTION));
        }
        filters.push(bpf_stmt(RET, ALLOW_ACTION));

        let mut prog = sock_fprog {
            len: filters.len() as u16,
            filter: filters.as_mut_ptr(),
        };

        unsafe {
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(new_error("Failed to enable no_new_privs before seccomp"));
            }
            if libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                &mut prog as *mut _,
            ) != 0
            {
                return Err(new_error("Failed to install seccomp filter"));
            }
        }

        Ok(())
    }
}

impl FromStr for SeccompFilter {
    type Err = DynError;

    fn from_str(raw: &str) -> Result<Self, DynError> {
        Self::parse(raw)
    }
}

fn bpf_stmt(code: u16, k: u32) -> sock_filter {
    sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

fn bpf_jump(code: u16, k: u32, jt: u8, jf: u8) -> sock_filter {
    sock_filter { code, jt, jf, k }
}

fn syscall_number(name: &str) -> Option<i64> {
    match name {
        "add_key" => Some(libc::SYS_add_key),
        "bpf" => Some(libc::SYS_bpf),
        "delete_module" => Some(libc::SYS_delete_module),
        "finit_module" => Some(libc::SYS_finit_module),
        "init_module" => Some(libc::SYS_init_module),
        "keyctl" => Some(libc::SYS_keyctl),
        "kexec_load" => Some(libc::SYS_kexec_load),
        "move_pages" => Some(libc::SYS_move_pages),
        "open_by_handle_at" => Some(libc::SYS_open_by_handle_at),
        "perf_event_open" => Some(libc::SYS_perf_event_open),
        "pivot_root" => Some(libc::SYS_pivot_root),
        "process_vm_readv" => Some(libc::SYS_process_vm_readv),
        "process_vm_writev" => Some(libc::SYS_process_vm_writev),
        "ptrace" => Some(libc::SYS_ptrace),
        "reboot" => Some(libc::SYS_reboot),
        "request_key" => Some(libc::SYS_request_key),
        "setns" => Some(libc::SYS_setns),
        "swapon" => Some(libc::SYS_swapon),
        "swapoff" => Some(libc::SYS_swapoff),
        "syslog" => Some(libc::SYS_syslog),
        "umount2" => Some(libc::SYS_umount2),
        "unshare" => Some(libc::SYS_unshare),
        "mount" => Some(libc::SYS_mount),
        _ => None,
    }
}
