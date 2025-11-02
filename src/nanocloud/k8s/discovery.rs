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

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct VersionInfo {
    pub major: String,
    pub minor: String,
    #[serde(rename = "gitVersion")]
    pub git_version: String,
    #[serde(rename = "gitCommit")]
    pub git_commit: String,
    #[serde(rename = "gitTreeState")]
    pub git_tree_state: String,
    #[serde(rename = "buildDate")]
    pub build_date: String,
    pub compiler: String,
    pub platform: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct APIVersions {
    pub kind: String,
    pub versions: Vec<String>,
    #[serde(
        rename = "serverAddressByClientCIDRs",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub server_address_by_client_cidrs: Vec<ServerAddressByClientCIDR>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ServerAddressByClientCIDR {
    #[serde(rename = "clientCIDR")]
    pub client_cidr: String,
    #[serde(rename = "serverAddress")]
    pub server_address: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct APIGroupList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub groups: Vec<APIGroup>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct APIGroup {
    pub name: String,
    pub versions: Vec<GroupVersion>,
    #[serde(rename = "preferredVersion")]
    pub preferred_version: GroupVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GroupVersion {
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct APIResourceList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    pub resources: Vec<APIResource>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct APIResource {
    pub name: String,
    #[serde(rename = "singularName")]
    pub singular_name: String,
    pub namespaced: bool,
    pub kind: String,
    pub verbs: Vec<String>,
    #[serde(rename = "shortNames", default, skip_serializing_if = "Vec::is_empty")]
    pub short_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub categories: Option<Vec<String>>,
}

impl VersionInfo {
    pub fn nanocloud() -> Self {
        VersionInfo {
            major: "1".to_string(),
            minor: "0".to_string(),
            git_version: format!("v{}-nanocloud", env!("CARGO_PKG_VERSION")),
            git_commit: option_env!("NANOCLOUD_GIT_COMMIT")
                .unwrap_or("unknown")
                .to_string(),
            git_tree_state: option_env!("NANOCLOUD_GIT_TREE_STATE")
                .unwrap_or("clean")
                .to_string(),
            build_date: option_env!("NANOCLOUD_BUILD_DATE")
                .unwrap_or("1970-01-01T00:00:00Z")
                .to_string(),
            compiler: rustc_version(),
            platform: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
        }
    }
}

impl APIVersions {
    pub fn core(server_address: Option<String>) -> Self {
        APIVersions {
            kind: "APIVersions".to_string(),
            versions: vec!["v1".to_string()],
            server_address_by_client_cidrs: server_address
                .map(|addr| {
                    vec![ServerAddressByClientCIDR {
                        client_cidr: "0.0.0.0/0".to_string(),
                        server_address: addr,
                    }]
                })
                .unwrap_or_default(),
        }
    }
}

impl APIGroupList {
    pub fn nanocloud() -> Self {
        APIGroupList {
            api_version: "v1".to_string(),
            kind: "APIGroupList".to_string(),
            groups: vec![APIGroup::apps(), APIGroup::nanocloud()],
        }
    }
}

impl APIGroup {
    pub fn nanocloud() -> Self {
        let group_version = GroupVersion {
            group_version: "nanocloud.io/v1".to_string(),
            version: "v1".to_string(),
        };
        APIGroup {
            name: "nanocloud.io".to_string(),
            versions: vec![group_version.clone()],
            preferred_version: group_version,
        }
    }

    pub fn apps() -> Self {
        let group_version = GroupVersion {
            group_version: "apps/v1".to_string(),
            version: "v1".to_string(),
        };
        APIGroup {
            name: "apps".to_string(),
            versions: vec![group_version.clone()],
            preferred_version: group_version,
        }
    }
}

impl APIResourceList {
    pub fn core() -> Self {
        APIResourceList {
            api_version: "v1".to_string(),
            kind: "APIResourceList".to_string(),
            group_version: "v1".to_string(),
            resources: vec![
                APIResource {
                    name: "pods".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "Pod".to_string(),
                    verbs: vec!["get".into(), "list".into(), "watch".into()],
                    short_names: vec!["po".into()],
                    categories: None,
                },
                APIResource {
                    name: "pods/log".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "PodLogOptions".to_string(),
                    verbs: vec!["get".into()],
                    short_names: Vec::new(),
                    categories: None,
                },
                // TODO(http2-exec): advertise pods/exec once the HTTP/2 exec implementation ships.
                APIResource {
                    name: "configmaps".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "ConfigMap".to_string(),
                    verbs: vec![
                        "get".into(),
                        "list".into(),
                        "watch".into(),
                        "create".into(),
                        "delete".into(),
                        "update".into(),
                        "deletecollection".into(),
                    ],
                    short_names: vec!["cm".into()],
                    categories: None,
                },
                APIResource {
                    name: "events".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "Event".to_string(),
                    verbs: vec!["get".into(), "list".into(), "watch".into()],
                    short_names: vec!["ev".into()],
                    categories: None,
                },
            ],
        }
    }

    pub fn apps() -> Self {
        APIResourceList {
            api_version: "v1".to_string(),
            kind: "APIResourceList".to_string(),
            group_version: "apps/v1".to_string(),
            resources: vec![
                APIResource {
                    name: "statefulsets".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "StatefulSet".to_string(),
                    verbs: vec!["get".into(), "list".into()],
                    short_names: vec!["sts".into()],
                    categories: None,
                },
                APIResource {
                    name: "replicasets".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "ReplicaSet".to_string(),
                    verbs: vec!["get".into(), "list".into()],
                    short_names: vec!["rs".into()],
                    categories: None,
                },
            ],
        }
    }

    pub fn nanocloud() -> Self {
        APIResourceList {
            api_version: "v1".to_string(),
            kind: "APIResourceList".to_string(),
            group_version: "nanocloud.io/v1".to_string(),
            resources: vec![
                APIResource {
                    name: "certificates".to_string(),
                    singular_name: "".to_string(),
                    namespaced: false,
                    kind: "Certificate".to_string(),
                    verbs: vec!["create".into()],
                    short_names: Vec::new(),
                    categories: None,
                },
                APIResource {
                    name: "bundles".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "Bundle".to_string(),
                    verbs: vec![
                        "get".into(),
                        "list".into(),
                        "watch".into(),
                        "create".into(),
                        "update".into(),
                        "patch".into(),
                        "delete".into(),
                        "deletecollection".into(),
                    ],
                    short_names: vec!["bdl".into()],
                    categories: Some(vec!["nanocloud".into()]),
                },
                APIResource {
                    name: "volumesnapshots".to_string(),
                    singular_name: "".to_string(),
                    namespaced: true,
                    kind: "VolumeSnapshot".to_string(),
                    verbs: vec![
                        "get".into(),
                        "list".into(),
                        "watch".into(),
                        "create".into(),
                        "delete".into(),
                    ],
                    short_names: vec!["vsnap".into()],
                    categories: Some(vec!["nanocloud".into()]),
                },
            ],
        }
    }
}

fn rustc_version() -> String {
    option_env!("RUSTC_VERSION")
        .unwrap_or("rustc unknown")
        .to_string()
}
