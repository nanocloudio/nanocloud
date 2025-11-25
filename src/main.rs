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

mod nanocloud;

use clap::Parser;

use nanocloud::cli::{bootstrap, run, NanoCtl, Terminal};

#[tokio::main]
async fn main() {
    let cli = NanoCtl::parse();

    let context = match bootstrap(&cli.command) {
        Ok(value) => value,
        Err(err) => {
            Terminal::error(format_args!("Error: {}", err));
            std::process::exit(1);
        }
    };

    match run(&cli.command, context).await {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            Terminal::error(format_args!("Error: {}", e));
            std::process::exit(1);
        }
    };
}
