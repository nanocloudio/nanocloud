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

use std::str::FromStr;

/// Deletion propagation policy used when removing workload controllers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DeletionPropagation {
    /// Dependents are deleted before the parent resource is fully removed.
    Foreground,
    /// The parent resource is removed immediately while dependents are cleaned up in the background.
    #[default]
    Background,
    /// Dependents are orphaned and left untouched.
    Orphan,
}

impl DeletionPropagation {
    /// Returns true when dependents should be removed.
    pub fn cascades(self) -> bool {
        !matches!(self, DeletionPropagation::Orphan)
    }
}

impl FromStr for DeletionPropagation {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "Foreground" => Ok(DeletionPropagation::Foreground),
            "Background" => Ok(DeletionPropagation::Background),
            "Orphan" => Ok(DeletionPropagation::Orphan),
            other => Err(format!("unsupported propagationPolicy '{other}'")),
        }
    }
}
