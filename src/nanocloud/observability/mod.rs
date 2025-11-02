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

//! Observability primitives shared across the Nanocloud control plane.
//!
//! The metrics exposed here follow the upstream Kubernetes Prometheus
//! conventions: snake_case names prefixed with the project (`nanocloud`),
//! counters ending with `_total`, and duration histograms ending with
//! `_seconds`. Label keys mirror Kubernetes resource identifiers such as
//! `namespace` and `workload` so metrics can be correlated with familiar
//! dashboards and alerting rules.

pub mod health;
pub mod metrics;
