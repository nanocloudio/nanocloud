/*
 * Copyright (C) 2024 The Nanocloud Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::fmt;
use std::io::{self, Write};

const COLOR_ERROR: &str = "\x1b[31m";
const COLOR_RESET: &str = "\x1b[0m";

#[derive(Clone, Copy)]
enum Stream {
    Stdout,
    Stderr,
}

pub struct Terminal;

impl Terminal {
    pub fn stdout(args: fmt::Arguments<'_>) {
        let _ = Self::write_line(Stream::Stdout, args);
    }

    pub fn stderr(args: fmt::Arguments<'_>) {
        let _ = Self::write_line(Stream::Stderr, args);
    }

    pub fn error(args: fmt::Arguments<'_>) {
        let _ = Self::write_colored_line(Stream::Stderr, COLOR_ERROR, args);
    }

    fn write_line(stream: Stream, args: fmt::Arguments<'_>) -> io::Result<()> {
        Self::with_stream(stream, move |handle| {
            handle.write_fmt(args)?;
            handle.write_all(b"\n")
        })
    }

    fn write_colored_line(stream: Stream, color: &str, args: fmt::Arguments<'_>) -> io::Result<()> {
        Self::with_stream(stream, move |handle| {
            handle.write_all(color.as_bytes())?;
            handle.write_fmt(args)?;
            handle.write_all(COLOR_RESET.as_bytes())?;
            handle.write_all(b"\n")
        })
    }

    fn with_stream<F>(stream: Stream, mut f: F) -> io::Result<()>
    where
        F: FnMut(&mut dyn Write) -> io::Result<()>,
    {
        match stream {
            Stream::Stdout => {
                let mut handle = io::stdout().lock();
                f(&mut handle)
            }
            Stream::Stderr => {
                let mut handle = io::stderr().lock();
                f(&mut handle)
            }
        }
    }
}
