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

use axum::Json;

use crate::nanocloud::k8s::discovery::{
    APIGroup, APIGroupList, APIResourceList, APIVersions, VersionInfo,
};

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/version",
    responses((status = 200, description = "Server version", body = VersionInfo)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn version() -> Json<VersionInfo> {
    Json(VersionInfo::nanocloud())
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api",
    responses((status = 200, description = "Core API versions", body = APIVersions)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn core_api_versions() -> Json<APIVersions> {
    Json(APIVersions::core(None))
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis",
    responses((status = 200, description = "Registered API groups", body = APIGroupList)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn api_groups() -> Json<APIGroupList> {
    Json(APIGroupList::nanocloud())
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/nanocloud.io",
    responses((status = 200, description = "Nanocloud API group", body = APIGroup)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn nanocloud_api_group() -> Json<APIGroup> {
    Json(APIGroup::nanocloud())
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps",
    responses((status = 200, description = "Apps API group", body = APIGroup)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn apps_api_group() -> Json<APIGroup> {
    Json(APIGroup::apps())
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/api/v1",
    responses((status = 200, description = "Core API resources", body = APIResourceList)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn core_api_resources() -> Json<APIResourceList> {
    Json(APIResourceList::core())
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/apps/v1",
    responses((status = 200, description = "Apps/v1 API resources", body = APIResourceList)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn apps_api_resources() -> Json<APIResourceList> {
    Json(APIResourceList::apps())
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/apis/nanocloud.io/v1",
    responses((status = 200, description = "Nanocloud API resources", body = APIResourceList)),
    security(
        ("NanocloudCertificate" = []),
        (
            "NanocloudBearer" = ["discovery.read"]
        )
    ),
    tag = "kubernetes"
))]
pub async fn nanocloud_api_resources() -> Json<APIResourceList> {
    Json(APIResourceList::nanocloud())
}
