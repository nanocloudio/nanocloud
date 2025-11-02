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

use crate::nanocloud::engine::Profile;
use crate::nanocloud::k8s::pod::{
    ContainerEnvVar, ContainerPort, ContainerProbe, ContainerSpec, EncryptedVolumeSource,
    EnvVarSource, ObjectMeta, Pod, PodSpec, ProbeExec, SecretKeySelector, VolumeMount, VolumeSpec,
};
use crate::nanocloud::oci::{OciImage, OciManifest, Registry};
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::{EncryptionKey, TlsInfo};

use serde_json::value::Value;
use serde_json::Map;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::ffi::CStr;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::Ipv4Addr;
use std::ptr;

pub struct Config {
    pub options: HashMap<String, String>,
    pub bindings: HashMap<String, Vec<Vec<String>>>,
    pub tls_info: TlsInfo,
}

pub struct Image {
    pub oci_image: OciImage,
    pub oci_manifest: OciManifest,
    pub config: Config,
}

impl Image {
    pub async fn load(
        namespace: Option<&str>,
        app: &str,
        provided_options: HashMap<String, String>,
        force_update: bool,
    ) -> Result<Image, Box<dyn Error + Send + Sync>> {
        // Pull image
        let oci_manifest =
            Registry::pull(&format!("registry.nanocloud.io/{}", app), force_update).await?;
        let oci_image = OciImage::load(&oci_manifest.config.digest)?;

        // Parse options
        let options = oci_image
            .config
            .labels
            .as_ref()
            .cloned()
            .unwrap_or_default()
            .get("io.nanocloud.options")
            .and_then(|opt| serde_json::from_str::<serde_json::Value>(opt).ok())
            .and_then(|v| v.as_object().cloned())
            .map(Image::parse_options_map)
            .unwrap_or_default();

        let image_config = process_options(namespace, app, &options, &provided_options)
            .await
            .map_err(|e| with_context(e, "Failed to prepare image configuration from options"))?;

        Ok(Image {
            oci_image,
            oci_manifest,
            config: image_config,
        })
    }

    fn parse_options_map(map: Map<String, Value>) -> HashMap<String, Vec<Setting>> {
        map.into_iter()
            .flat_map(|(key, value)| {
                value
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect::<Vec<_>>()
                    })
                    .map(|values| {
                        let setting = process_setting(&key, &values);
                        (setting.name.clone(), setting)
                    })
            })
            .fold(HashMap::new(), |mut acc, (key, value)| {
                acc.entry(key).or_insert_with(Vec::new).push(value);
                acc
            })
    }

    fn get_by_prefix(&self, prefix: &str) -> HashMap<String, String> {
        self.config
            .options
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix(prefix)
                    .map(|s| (s.to_string(), v.to_string()))
            })
            .collect()
    }

    pub fn get_ports(&self) -> HashMap<String, String> {
        self.get_by_prefix("port_")
    }

    pub fn get_volumes(&self) -> HashMap<String, String> {
        self.get_by_prefix("volume_")
    }

    pub fn to_pod_spec(&self, container_name: &str, image_reference: Option<&str>) -> Pod {
        let image_ref = image_reference
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.oci_manifest.config.digest.clone());

        let mut env_vars: Vec<ContainerEnvVar> = self
            .oci_image
            .config
            .env
            .as_ref()
            .map(|vars| {
                vars.iter()
                    .map(|entry| {
                        if let Some((key, value)) = entry.split_once('=') {
                            ContainerEnvVar {
                                name: key.to_string(),
                                value: Some(value.to_string()),
                                value_from: None,
                            }
                        } else {
                            ContainerEnvVar {
                                name: entry.to_string(),
                                value: None,
                                value_from: None,
                            }
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut seen_env_names: HashSet<String> =
            env_vars.iter().map(|entry| entry.name.clone()).collect();

        for (env_key, mapping) in self.get_by_prefix("secret_env_") {
            if let Some((secret_name, secret_key, optional)) =
                parse_secret_env_target(mapping.as_str())
            {
                let env_name = sanitize_env_var_name(&env_key);
                if seen_env_names.contains(&env_name) {
                    continue;
                }
                env_vars.push(ContainerEnvVar {
                    name: env_name.clone(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        config_map_key_ref: None,
                        secret_key_ref: Some(SecretKeySelector {
                            key: secret_key,
                            name: Some(secret_name),
                            optional: optional.then_some(true),
                        }),
                    }),
                });
                seen_env_names.insert(env_name);
            }
        }

        let image_command = self.oci_image.config.entrypoint.clone().unwrap_or_default();
        let image_args = self.oci_image.config.cmd.clone().unwrap_or_default();
        let command = self
            .config
            .options
            .get("runtime_command")
            .map(|value| parse_runtime_list(value))
            .unwrap_or_default();
        let args = self
            .config
            .options
            .get("runtime_args")
            .map(|value| parse_runtime_list(value))
            .unwrap_or_default();

        let volumes_map = self.get_volumes();

        let volume_mounts: Vec<VolumeMount> = volumes_map
            .iter()
            .map(|(name, path)| VolumeMount {
                name: name.to_string(),
                mount_path: path.to_string(),
                read_only: None,
            })
            .collect();

        let volumes: Vec<VolumeSpec> = volumes_map
            .keys()
            .map(|name| {
                let claim_name = name.to_string();
                let encrypted = self
                    .config
                    .options
                    .get(&format!("volume_encrypted_{}", name))
                    .and_then(|value| parse_volume_encryption_option(value).ok())
                    .map(|(key_name, filesystem)| EncryptedVolumeSource {
                        device: String::new(),
                        key_name,
                        filesystem,
                    });
                VolumeSpec {
                    name: claim_name.clone(),
                    host_path: None,
                    empty_dir: None,
                    secret: None,
                    config_map: None,
                    projected: None,
                    persistent_volume_claim: Some(
                        crate::nanocloud::k8s::pod::PersistentVolumeClaimVolumeSource {
                            claim_name,
                            read_only: None,
                        },
                    ),
                    encrypted,
                }
            })
            .collect();

        let ports: Vec<ContainerPort> = self
            .get_ports()
            .iter()
            .filter_map(|(name, value)| parse_port_option(name, value))
            .collect();

        let readiness_probe = Some(ContainerProbe {
            exec: Some(ProbeExec {
                command: vec!["ready.sh".to_string()],
            }),
            http_get: None,
            initial_delay_seconds: Some(5),
            period_seconds: Some(5),
        });

        let container = ContainerSpec {
            name: container_name.to_string(),
            image: Some(image_ref),
            command,
            args,
            image_command,
            image_args,
            env_from: Vec::new(),
            env: env_vars,
            ports,
            volume_mounts,
            resources: None,
            working_dir: self.oci_image.config.working_dir.clone(),
            lifecycle: None,
            user: self.oci_image.config.user.clone(),
            liveness_probe: None,
            readiness_probe,
        };

        let host_network = false;

        let mut labels = HashMap::new();
        labels.insert(
            "app.kubernetes.io/name".to_string(),
            container_name.to_string(),
        );

        let pod_spec = PodSpec {
            init_containers: Vec::new(),
            containers: vec![container],
            volumes,
            restart_policy: None,
            service_account_name: None,
            node_name: None,
            host_network,
        };

        Pod {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            metadata: ObjectMeta {
                name: Some(container_name.to_string()),
                namespace: None,
                labels,
                annotations: HashMap::new(),
                resource_version: None,
            },
            spec: pod_spec,
            status: None,
        }
    }
}

fn parse_port_option(name: &str, value: &str) -> Option<ContainerPort> {
    let (mapping, proto) = value
        .split_once('/')
        .map(|(m, p)| (m.trim(), p.trim()))
        .unwrap_or((value.trim(), "tcp"));

    let (host_part, container_part) = if let Some((left, right)) = mapping.split_once("->") {
        (Some(left.trim()), right.trim())
    } else {
        (None, mapping)
    };

    let (_container_ip, container_port) = parse_endpoint(container_part)?;
    let (host_ip, host_port) = match host_part {
        Some(spec) => {
            let (ip, port) = parse_endpoint(spec)?;
            (ip, Some(port))
        }
        None => (None, Some(container_port)),
    };

    Some(ContainerPort {
        name: Some(name.to_string()),
        container_port,
        protocol: Some(proto.to_uppercase()),
        host_ip,
        host_port,
    })
}

fn parse_volume_encryption_option(value: &str) -> Result<(String, Option<String>), String> {
    let mut parts = value.splitn(2, ':');
    let key_name = parts
        .next()
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| "encryption option must include a key name".to_string())?;
    let filesystem = parts
        .next()
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_string());
    Ok((key_name.to_string(), filesystem))
}

fn parse_endpoint(spec: &str) -> Option<(Option<String>, u16)> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with('[') {
        let end = trimmed.find(']')?;
        let addr = &trimmed[1..end];
        let remainder = trimmed[end + 1..].trim_start();
        let port = remainder.strip_prefix(':')?.parse().ok()?;
        return Some((Some(addr.to_string()), port));
    }

    if let Some((ip, port)) = trimmed.rsplit_once(':') {
        if let Ok(num) = port.parse() {
            let host_ip = if ip.is_empty() {
                None
            } else {
                Some(ip.to_string())
            };
            return Some((host_ip, num));
        }
    }

    let port = trimmed.parse().ok()?;
    Some((None, port))
}

#[derive(Default, Clone, Debug)]
struct Setting {
    pub name: String,
    pub is_optional: bool,
    pub is_service: bool,
    pub match_value: Option<String>,
    pub default_value: Option<String>, // Macro, either raw string or beings with !
    pub required_options: HashMap<String, Option<String>>, // Bounded value if option is_some
    pub required_bindings: Vec<String>, // Services and bindings
}

fn evaluate_macro(
    macro_str: &String,
    options: &HashMap<String, String>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut args = macro_str.split_whitespace();
    let macro_name = args
        .next()
        .ok_or_else(|| new_error("Macro expression is empty"))?;

    match macro_name {
        "!dns" => get_dns_servers()
            .first()
            .map(|s| s.to_string())
            .ok_or_else(|| new_error("No DNS servers found")),
        "!if" => {
            let condition = args
                .next()
                .ok_or_else(|| new_error("Missing condition for !if macro"))?;
            let (key, expected) = condition.split_once('=').ok_or_else(|| {
                new_error(format!(
                    "Invalid !if condition '{}'; expected key=value",
                    condition
                ))
            })?;
            let matches = options
                .get(key)
                .map(|value| value == expected)
                .unwrap_or(false);
            let option_true = args.next().unwrap_or_default().to_string();
            let option_false = args.next().unwrap_or_default().to_string();
            Ok(if matches { option_true } else { option_false })
        }
        "!local_ip" => Ok(get_local_ip()?),
        "!rand" => {
            let len: usize = args
                .next()
                .unwrap_or("16")
                .parse()
                .map_err(|_| new_error("Length parameter for !rand must be a number"))?;
            let charset = args.next().unwrap_or("hex");
            let random_bytes = EncryptionKey::gen_random_bytes(len, charset);
            String::from_utf8(random_bytes.to_vec())
                .map_err(|_| new_error("Failed to convert random bytes to String"))
        }
        "!tls" => {
            let arg = args
                .next()
                .ok_or_else(|| new_error("Missing argument for !tls macro"))?;
            let path = match arg {
                "key" => "/var/run/nanocloud.io/tls/key.pem",
                "cert" => "/var/run/nanocloud.io/tls/cert.pem",
                "ca" => "/var/run/nanocloud.io/tls/ca.pem",
                _ => return Err(new_error(format!("Unknown !tls argument: {}", arg))),
            };
            Ok(path.to_string())
        }
        _ => Err(new_error(format!("Unsupported macro: {}", macro_str))),
    }
}

fn process_setting(key: &str, tokens: &[String]) -> Setting {
    let is_optional = key.starts_with('?');
    let trimmed = if is_optional { &key[1..] } else { key };
    let mut parts = trimmed.splitn(2, '=');
    let name = parts.next().unwrap_or_default().to_string();
    let remainder = parts.next();
    let match_value = remainder.map(|value| value.trim_start_matches('&').to_string());

    let mut required_options = HashMap::new();
    let mut required_bindings = Vec::new();
    let mut default_value = None;
    for token in tokens {
        if let Some(inner) = token.strip_prefix('*') {
            let (key, value) = inner
                .split_once('=')
                .map(|(k, v)| (k.to_string(), Some(v.to_string())))
                .unwrap_or((inner.to_string(), None));
            required_options.insert(key, value);
        } else if let Some(inner) = token.strip_prefix('&') {
            required_bindings.push(inner.to_string());
        } else {
            default_value = Some(token.clone());
        }
    }

    Setting {
        name,
        is_optional,
        is_service: key.contains('&'),
        match_value,
        required_options,
        required_bindings,
        default_value,
    }
}

async fn get_service_options(
    namespace: Option<&str>,
    prefix: &str,
    service: &str,
    required_options: &HashMap<String, Option<String>>,
) -> Result<HashMap<String, String>, Box<dyn Error + Send + Sync>> {
    // Load the profile for service and filter required options.
    let profile = Profile::load(namespace, service).await?;
    let mut filtered = HashMap::new();
    for (key, value) in &profile.config {
        let prefixed_key = format!("{}_{}", prefix, key);
        if !required_options.contains_key(&prefixed_key) {
            continue;
        }
        let value_str = String::from_utf8(value.clone()).map_err(|e| {
            with_context(
                e,
                format!(
                    "Service '{}' option '{}' contains invalid UTF-8",
                    service, prefixed_key
                ),
            )
        })?;
        filtered.insert(prefixed_key, value_str);
    }
    Ok(filtered)
}

fn process_missing_options(
    image_options: &HashMap<String, Vec<Setting>>,
    provided_options: &HashMap<String, String>,
) -> HashMap<String, Vec<Setting>> {
    // Filter image_options for anything not in provided_options and is marked as not optional.
    image_options
        .iter()
        .filter_map(|(name, items)| {
            (!provided_options.contains_key(name)).then(|| {
                let required = items
                    .iter()
                    .filter(|setting| !setting.is_optional)
                    .cloned()
                    .collect::<Vec<_>>();
                (!required.is_empty()).then_some((name.clone(), required))
            })?
        })
        .collect()
}

fn process_bound_options(
    image_options: &HashMap<String, Vec<Setting>>,
    provided_options: &HashMap<String, String>,
) -> Result<HashMap<String, String>, Box<dyn Error + Send + Sync>> {
    // Identify bound options as those having more that one setting.
    // If the name of a bound option is not a key in provided_options then map to the match_value in the first setting.
    // If the name of a bound option is a key in provided_options but the value is not equal to the match_value on any of the settings then map to an error.
    image_options
        .iter()
        .filter(|(_, settings)| settings.iter().any(|setting| setting.match_value.is_some()))
        .map(|(name, settings)| {
            settings
                .iter()
                .find(|setting| {
                    provided_options
                        .get(name)
                        .is_none_or(|value| setting.match_value.as_ref() == Some(value))
                })
                .and_then(|setting| {
                    setting
                        .match_value
                        .clone()
                        .map(|value| (name.clone(), value))
                })
                .ok_or_else(|| new_error(format!("Invalid or missing value for '{}'", name)))
        })
        .collect()
}

fn process_required_services(
    image_options: &HashMap<String, Vec<Setting>>,
    options: &HashMap<String, String>,
) -> Vec<Setting> {
    // Filter image_options for settings that match options and are marked as services.
    image_options
        .iter()
        .filter_map(|(name, settings)| {
            settings
                .iter()
                .find(|setting| {
                    setting.is_service && setting.match_value == options.get(name).cloned()
                })
                .cloned()
        })
        .collect()
}

fn process_binding_templates(required_services: &[Setting]) -> HashMap<String, Vec<Vec<String>>> {
    // Filter image_options for settings that match options and are marked as services.
    required_services
        .iter()
        .filter_map(|setting| {
            setting.match_value.as_ref().map(|value| {
                let templates = setting
                    .required_bindings
                    .iter()
                    .map(|binding| binding.split_whitespace().map(String::from).collect())
                    .collect();
                (value.clone(), templates)
            })
        })
        .collect()
}

async fn process_valid_service_options(
    namespace: Option<&str>,
    provided_options: &HashMap<String, String>,
    required_services: &[Setting],
) -> Result<HashMap<String, String>, Box<dyn Error + Send + Sync>> {
    let mut options: HashMap<String, String> = HashMap::new();
    for setting in required_services {
        let service = setting.match_value.as_ref().ok_or_else(|| {
            new_error(format!(
                "Service option '{}' is missing a concrete value in image options",
                setting.name
            ))
        })?;
        for (key, service_value) in
            get_service_options(namespace, &setting.name, service, &setting.required_options)
                .await?
        {
            if let Some(provided_value) = provided_options.get(&key) {
                if provided_value != &service_value {
                    return Err(new_error(format!(
                        "Provided or defaulted option {}={} doesn't match required service configuration {}",
                        &key, &provided_value, &service_value
                    )));
                }
            }
            options.insert(key, service_value);
        }
    }
    Ok(options)
}

fn process_invalid_bound_options(
    image_options: &HashMap<String, Vec<Setting>>,
    options: &HashMap<String, String>,
) -> Result<Vec<()>, Box<dyn Error + Send + Sync>> {
    // Match settings against provided options and compare against required values.
    options
        .iter()
        .filter_map(|(name, value)| {
            image_options
                .get(name)?
                .iter()
                .find(|setting| setting.match_value.as_ref() == Some(value))
                .and_then(|setting| {
                    setting
                        .required_options
                        .iter()
                        .find_map(|(required_name, required_value)| {
                            (required_value.is_some()
                                && options.get(required_name) != required_value.as_ref())
                            .then(|| {
                                format!(
                                    "Option {}={} requires {}={}",
                                    name,
                                    value,
                                    required_name,
                                    required_value.clone().unwrap_or_default()
                                )
                            })
                        })
                })
        })
        .map(|message| Err::<(), Box<dyn Error + Send + Sync>>(new_error(message)))
        .collect()
}

fn parse_runtime_list(raw: &str) -> Vec<String> {
    raw.split_whitespace()
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_string())
        .collect()
}

fn sanitize_env_var_name(raw: &str) -> String {
    let mut result = String::with_capacity(raw.len());
    for ch in raw.chars() {
        let upper = if ch.is_ascii_lowercase() {
            ch.to_ascii_uppercase()
        } else {
            ch
        };
        if upper.is_ascii_alphanumeric() || upper == '_' {
            result.push(upper);
        } else {
            result.push('_');
        }
    }
    if result.is_empty() {
        return "_SECRET".to_string();
    }
    if !result
        .chars()
        .next()
        .map(|c| c.is_ascii_alphabetic() || c == '_')
        .unwrap_or(false)
    {
        result.insert(0, '_');
    }
    result
}

fn parse_secret_env_target(raw: &str) -> Option<(String, String, bool)> {
    let optional = raw.starts_with('?');
    let trimmed = raw.trim_start_matches('?');
    let (secret_name, key) = trimmed
        .split_once(':')
        .or_else(|| trimmed.split_once('/'))?;
    if secret_name.is_empty() || key.is_empty() {
        return None;
    }
    Some((secret_name.to_string(), key.to_string(), optional))
}

fn process_options_to_default(
    image_options: &HashMap<String, Vec<Setting>>,
    options: &HashMap<String, String>,
) -> HashMap<String, Vec<Setting>> {
    let mut required: HashMap<String, Vec<Setting>> = HashMap::new();

    for (name, value) in options {
        let settings = match image_options.get(name) {
            Some(settings) => settings,
            None => continue,
        };

        if let Some(setting) = settings.iter().find(|setting| {
            setting
                .match_value
                .as_ref()
                .is_none_or(|match_value| match_value == value)
        }) {
            for (required_name, required_value) in &setting.required_options {
                if required_value.is_some() {
                    continue;
                }

                if options.contains_key(required_name) || required.contains_key(required_name) {
                    continue;
                }

                if let Some(required_settings) = image_options.get(required_name) {
                    required.insert(required_name.clone(), required_settings.clone());
                }
            }
        }
    }

    required
}

fn process_default_options(
    provided_options: &HashMap<String, String>,
    missing_options: &HashMap<String, Vec<Setting>>,
) -> Result<HashMap<String, String>, Box<dyn Error + Send + Sync>> {
    let mut options: HashMap<String, String> = HashMap::new();
    for (name, items) in missing_options {
        if provided_options.contains_key(name) {
            continue;
        }

        let default_setting = items
            .iter()
            .find(|setting| setting.default_value.is_some())
            .ok_or_else(|| {
                new_error(format!(
                    "Option '{}' is required but has no default value defined in image labels",
                    name
                ))
            })?;

        let default_value = default_setting
            .default_value
            .as_ref()
            .ok_or_else(|| new_error(format!("Default for option '{}' is not set", name)))?;

        let resolved = if default_value.starts_with('!') {
            evaluate_macro(default_value, provided_options)?
        } else {
            default_value.to_string()
        };

        options.insert(name.to_string(), resolved);
    }
    Ok(options)
}

fn process_bindings(
    binding_templates: &HashMap<String, Vec<Vec<String>>>,
    options: &HashMap<String, String>,
) -> HashMap<String, Vec<Vec<String>>> {
    binding_templates
        .iter()
        .map(|(service, bindings)| {
            (
                service.clone(),
                bindings
                    .iter()
                    .map(|binding| {
                        binding
                            .iter()
                            .map(|profile| {
                                profile
                                    .strip_prefix('$')
                                    .and_then(|key| options.get(key).cloned())
                                    .unwrap_or_else(|| profile.clone())
                            })
                            .collect()
                    })
                    .collect(),
            )
        })
        .collect()
}

async fn process_options(
    namespace: Option<&str>,
    app: &str,
    image_options: &HashMap<String, Vec<Setting>>,
    provided_options: &HashMap<String, String>,
) -> Result<Config, Box<dyn Error + Send + Sync>> {
    let mut missing_options = process_missing_options(image_options, provided_options);
    let bound_options = process_bound_options(image_options, provided_options)
        .map_err(|e| with_context(e, "Failed to resolve bound options"))?;
    let mut options = provided_options
        .clone()
        .into_iter()
        .chain(bound_options)
        .collect();
    let required_services = process_required_services(image_options, &options);
    let binding_templates = process_binding_templates(&required_services);
    options.extend(
        process_valid_service_options(namespace, &options, &required_services)
            .await
            .map_err(|e| with_context(e, "Failed to resolve service-backed options"))?,
    );
    process_invalid_bound_options(image_options, &options)
        .map_err(|e| with_context(e, "Invalid bound option combination"))?;
    missing_options.extend(process_options_to_default(image_options, &options));
    options.extend(
        process_default_options(&options, &missing_options)
            .map_err(|e| with_context(e, "Failed to compute defaulted options"))?,
    );
    let bindings = process_bindings(&binding_templates, &options);
    let mut san_entries = vec![format!("{}.nanocloud.local", app)];
    let local_ip = get_local_ip()
        .map_err(|e| with_context(e, "Failed to determine host IP address for TLS"))?;
    san_entries.push(local_ip);
    let tls_info = TlsInfo::create(app, Some(&san_entries))
        .map_err(|e| with_context(e, "Failed to create TLS assets"))?;

    Ok(Config {
        options,
        bindings,
        tls_info,
    })
}

/// Get the DNS servers in use on the host
fn get_dns_servers() -> Vec<String> {
    let mut dns_servers = Vec::new();

    if let Ok(file) = File::open("/etc/resolv.conf") {
        let reader = BufReader::new(file);

        for line in reader.lines().map_while(Result::ok) {
            if line.starts_with("nameserver") {
                if let Some(ip) = line.split_whitespace().nth(1) {
                    dns_servers.push(ip.to_string());
                }
            }
        }
    }

    dns_servers
}

/// Get the IP address of the host by inspecting the first non-loopback network interface with an IPv4 address assigned
fn get_local_ip() -> Result<String, Box<dyn Error + Send + Sync>> {
    unsafe {
        let mut ifap: *mut libc::ifaddrs = ptr::null_mut();

        // Call getifaddrs() to get the network interfaces list
        if libc::getifaddrs(&mut ifap) != 0 {
            return Err(new_error("Failed to list network interfaces"));
        }

        let mut current = ifap;
        let mut selected_ip: Option<String> = None;

        while !current.is_null() {
            let iface = &*current;

            // Check if the interface has an IPv4 address
            if !iface.ifa_addr.is_null() && (*iface.ifa_addr).sa_family as i32 == libc::AF_INET {
                let name = CStr::from_ptr(iface.ifa_name).to_str().unwrap_or("");

                // Skip loopback interface (lo)
                if name == "lo" {
                    current = (*current).ifa_next;
                    continue;
                }

                // Extract IPv4 address
                let sockaddr: *const libc::sockaddr_in = iface.ifa_addr as *const libc::sockaddr_in;
                let ip = Ipv4Addr::from((*sockaddr).sin_addr.s_addr.to_be());

                selected_ip = Some(ip.to_string());
                break; // Stop after finding the first non-loopback address
            }

            current = (*current).ifa_next;
        }

        // Free allocated memory
        libc::freeifaddrs(ifap);
        selected_ip.ok_or_else(|| new_error("Unable to detect local IP address"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nanocloud::oci::distribution::{ConfigDescriptor, LayerDescriptor, OciManifest};
    use crate::nanocloud::oci::image::{Config as OciConfig, HistoryEntry, OciImage, RootFs};
    use crate::nanocloud::util::security::TlsInfo;

    fn build_test_image(options: HashMap<String, String>) -> Image {
        let oci_image = OciImage {
            created: "2024-01-01T00:00:00Z".to_string(),
            architecture: "amd64".to_string(),
            variant: None,
            os: "linux".to_string(),
            config: OciConfig {
                user: None,
                env: Some(vec![]),
                entrypoint: None,
                cmd: None,
                working_dir: None,
                labels: None,
            },
            rootfs: RootFs {
                type_field: "layers".to_string(),
                diff_ids: Vec::new(),
            },
            history: vec![HistoryEntry {
                created: "2024-01-01T00:00:00Z".to_string(),
                created_by: "unit-test".to_string(),
                comment: None,
                empty_layer: Some(true),
            }],
        };

        let oci_manifest = OciManifest {
            schema_version: 2,
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            config: ConfigDescriptor {
                media_type: "application/vnd.oci.image.config.v1+json".to_string(),
                digest: "sha256:dummydigest".to_string(),
                size: 0,
            },
            layers: vec![LayerDescriptor {
                media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
                digest: "sha256:layerdigest".to_string(),
                size: 0,
            }],
            annotations: None,
        };

        Image {
            oci_image,
            oci_manifest,
            config: Config {
                options,
                bindings: HashMap::new(),
                tls_info: TlsInfo {
                    key: Vec::new(),
                    cert: Vec::new(),
                    ca: Vec::new(),
                },
            },
        }
    }

    #[test]
    fn to_pod_spec_adds_secret_backed_env_vars() {
        let mut options = HashMap::new();
        options.insert(
            "secret_env_db_password".to_string(),
            "db-credentials:password".to_string(),
        );
        options.insert(
            "secret_env_optional_token".to_string(),
            "?service-token:key".to_string(),
        );

        let image = build_test_image(options);
        let pod = image.to_pod_spec("svc", Some("registry.nanocloud.io/svc"));
        let container = pod
            .spec
            .containers
            .first()
            .expect("expected container entry");

        let password_var = container
            .env
            .iter()
            .find(|env| env.name == "DB_PASSWORD")
            .expect("missing DB_PASSWORD secret env");
        let password_ref = password_var
            .value_from
            .as_ref()
            .and_then(|src| src.secret_key_ref.as_ref())
            .expect("secret reference missing");
        assert_eq!(password_ref.name.as_deref(), Some("db-credentials"));
        assert_eq!(password_ref.key, "password");
        assert_eq!(password_ref.optional, None);

        let optional_var = container
            .env
            .iter()
            .find(|env| env.name == "OPTIONAL_TOKEN")
            .expect("missing OPTIONAL_TOKEN secret env");
        let optional_ref = optional_var
            .value_from
            .as_ref()
            .and_then(|src| src.secret_key_ref.as_ref())
            .expect("optional secret reference missing");
        assert_eq!(optional_ref.name.as_deref(), Some("service-token"));
        assert_eq!(optional_ref.key, "key");
        assert_eq!(optional_ref.optional, Some(true));
    }
}
