use std::error::Error;
use std::io;
use std::os::fd::{BorrowedFd, RawFd};

use crate::nanocloud::api::client::{ExecRequest, NanocloudClient};
use crate::nanocloud::cli::args::ExecArgs;

use libc;
use nix::sys::termios::{self, SetArg, Termios};
use nix::unistd::isatty;

fn exec_request_from_args(args: &ExecArgs) -> ExecRequest {
    ExecRequest {
        namespace: args
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        pod: args.pod.clone(),
        container: args.container.clone(),
        command: args.command.clone(),
        stdin: args.stdin,
        stdout: true,
        stderr: true,
        tty: args.tty,
    }
}

struct TerminalModeGuard {
    fd: RawFd,
    previous: Termios,
}

impl TerminalModeGuard {
    fn enable_raw(fd: RawFd) -> io::Result<Option<Self>> {
        match unsafe { isatty(BorrowedFd::borrow_raw(fd)) } {
            Ok(true) => {
                let previous = termios::tcgetattr(unsafe { BorrowedFd::borrow_raw(fd) })
                    .map_err(io::Error::from)?;
                let mut raw = previous.clone();
                termios::cfmakeraw(&mut raw);
                termios::tcsetattr(unsafe { BorrowedFd::borrow_raw(fd) }, SetArg::TCSANOW, &raw)
                    .map_err(io::Error::from)?;
                Ok(Some(TerminalModeGuard { fd, previous }))
            }
            Ok(false) => Ok(None),
            Err(err) => Err(io::Error::from(err)),
        }
    }
}

impl Drop for TerminalModeGuard {
    fn drop(&mut self) {
        let _ = termios::tcsetattr(
            unsafe { BorrowedFd::borrow_raw(self.fd) },
            SetArg::TCSANOW,
            &self.previous,
        );
    }
}

pub(super) async fn handle_exec(
    client: &NanocloudClient,
    args: &ExecArgs,
) -> Result<i32, Box<dyn Error + Send + Sync>> {
    // Audit notes for the current CLI surface live in docs/cli/exec-command.md.
    // The handler currently only validates required arguments and prints a
    // placeholder until the streaming plumbing lands.
    if args.command.is_empty() {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "command must be provided after '--'",
        )));
    }

    let request = exec_request_from_args(args);
    let _tty_guard = if request.tty {
        TerminalModeGuard::enable_raw(libc::STDIN_FILENO)
            .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?
    } else {
        None
    };
    client.exec(&request).await
}
