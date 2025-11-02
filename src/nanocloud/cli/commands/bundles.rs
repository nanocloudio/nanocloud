use std::error::Error;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

use reqwest::StatusCode;

use crate::nanocloud::api::client::{
    ApplyBundleOptions, BundleExportOptions, HttpError, NanocloudClient,
};
use crate::nanocloud::cli::args::{
    BundleApplyArgs, BundleApplyFormat, BundleArgs, BundleCommands, BundleExportArgs,
};
use crate::nanocloud::cli::curl::{print_curl_request, print_curl_request_with_type};
use crate::nanocloud::cli::Terminal;

pub async fn handle_bundle(
    client: &NanocloudClient,
    args: &BundleArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    match &args.command {
        BundleCommands::Apply(apply_args) => handle_apply(client, apply_args).await,
        BundleCommands::Export(export_args) => handle_export(client, export_args).await,
    }
}

async fn handle_apply(
    client: &NanocloudClient,
    args: &BundleApplyArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (payload, format) = read_payload(args)?;

    if args.curl {
        emit_curl_request(client, args, &payload, format)?;
        return Ok(());
    }

    match client
        .apply_bundle(
            args.namespace.as_deref(),
            &args.service,
            ApplyBundleOptions {
                payload: &payload,
                content_type: format.content_type(),
                field_manager: &args.field_manager,
                force: args.force,
                dry_run: args.dry_run,
            },
        )
        .await
    {
        Ok(bundle) => {
            let namespace = bundle
                .metadata
                .namespace
                .as_deref()
                .or(args.namespace.as_deref())
                .unwrap_or("default");
            let name = bundle.metadata.name.as_deref().unwrap_or(&args.service);
            let resource_version = bundle.metadata.resource_version.as_deref().unwrap_or("-");
            if args.dry_run {
                Terminal::stdout(format_args!(
                    "Dry-run succeeded for '{}/{}' (resourceVersion would become {}).",
                    namespace, name, resource_version
                ));
            } else {
                Terminal::stdout(format_args!(
                    "Bundle '{}/{}' updated (resourceVersion={}).",
                    namespace, name, resource_version
                ));
            }
            Ok(())
        }
        Err(err) => match err.downcast::<HttpError>() {
            Ok(http_err) => {
                if http_err.status == StatusCode::CONFLICT {
                    emit_conflict_help(&http_err, args.force);
                }
                Err(http_err)
            }
            Err(other) => Err(other),
        },
    }
}

async fn handle_export(
    client: &NanocloudClient,
    args: &BundleExportArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if args.curl {
        emit_export_curl(client, args)?;
        return Ok(());
    }

    let target =
        export_target(args).map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
    let namespace = args.namespace.as_deref();
    let artifact = client
        .export_bundle_profile(
            namespace,
            &args.service,
            BundleExportOptions {
                include_secrets: args.include_secrets,
            },
        )
        .await?;
    match target {
        ExportTarget::Stdout => {
            let mut stdout = io::stdout().lock();
            stdout.write_all(&artifact)?;
            stdout.flush()?;
        }
        ExportTarget::File(path) => {
            write_export_file(path, &artifact)?;
            Terminal::stdout(format_args!(
                "Saved bundle profile export to {}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn emit_export_curl(
    client: &NanocloudClient,
    args: &BundleExportArgs,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let owned_segments =
        NanocloudClient::bundle_export_segments(args.namespace.as_deref(), &args.service);
    let segments: Vec<&str> = owned_segments
        .iter()
        .map(|segment| segment.as_str())
        .collect();
    let mut url = client.url_from_segments(&segments)?;
    if args.include_secrets {
        url.query_pairs_mut().append_pair("includeSecrets", "true");
    }
    print_curl_request(client, "POST", url.as_ref(), None)?;
    Ok(())
}

#[derive(Debug)]
enum ExportTarget<'a> {
    Stdout,
    File(&'a Path),
}

fn export_target(args: &BundleExportArgs) -> io::Result<ExportTarget<'_>> {
    match (args.stdout, args.output.as_ref()) {
        (true, Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--stdout cannot be combined with --output",
        )),
        (true, None) => Ok(ExportTarget::Stdout),
        (false, Some(path)) => Ok(ExportTarget::File(path)),
        (false, None) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "either --output or --stdout must be provided",
        )),
    }
}

fn write_export_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)
}

fn emit_conflict_help(err: &HttpError, force_requested: bool) {
    if let Some(conflicts) = err.conflicts() {
        Terminal::stderr(format_args!(
            "Apply rejected because these paths are managed by other field managers:"
        ));
        for item in conflicts {
            Terminal::stderr(format_args!(
                "  {} (owned by {})",
                item.path, item.existing_manager
            ));
        }
        if !force_requested {
            Terminal::stderr(format_args!(
                "Re-run with --force to override the recorded managers."
            ));
        }
    }
}

fn emit_curl_request(
    client: &NanocloudClient,
    args: &BundleApplyArgs,
    payload: &[u8],
    format: BundleApplyFormat,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut url = client.url_from_segments(&NanocloudClient::bundle_segments(
        args.namespace.as_deref(),
        &args.service,
    ))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("fieldManager", args.field_manager.trim());
        if args.force {
            pairs.append_pair("force", "true");
        }
        if args.dry_run {
            pairs.append_pair("dryRun", "true");
        }
    }
    let body = String::from_utf8(payload.to_vec()).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Manifest is not valid UTF-8: {err}"),
        )
    })?;
    print_curl_request_with_type(
        client,
        "PATCH",
        url.as_ref(),
        Some(&body),
        format.content_type(),
    )
}

fn read_payload(
    args: &BundleApplyArgs,
) -> Result<(Vec<u8>, BundleApplyFormat), Box<dyn Error + Send + Sync>> {
    if !args.stdin && args.file.is_none() {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "either --file or --stdin must be provided",
        )));
    }

    let format = args
        .format
        .or_else(|| infer_format(args.file.as_deref()))
        .unwrap_or(BundleApplyFormat::Yaml);

    let mut payload = Vec::new();
    if args.stdin {
        io::stdin().read_to_end(&mut payload)?;
    } else if let Some(path) = args.file.as_ref() {
        payload = fs::read(path)?;
    }

    if payload.is_empty() {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "apply payload is empty",
        )));
    }

    Ok((payload, format))
}

fn infer_format(path: Option<&Path>) -> Option<BundleApplyFormat> {
    let ext = path?.extension()?.to_str()?.to_ascii_lowercase();
    match ext.as_str() {
        "json" => Some(BundleApplyFormat::Json),
        "yaml" | "yml" => Some(BundleApplyFormat::Yaml),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn infers_format_from_extension() {
        let yaml = infer_format(Some(Path::new("bundle.yaml")));
        assert!(matches!(yaml, Some(BundleApplyFormat::Yaml)));

        let yml = infer_format(Some(Path::new("bundle.YML")));
        assert!(matches!(yml, Some(BundleApplyFormat::Yaml)));

        let json = infer_format(Some(Path::new("spec.json")));
        assert!(matches!(json, Some(BundleApplyFormat::Json)));

        let none = infer_format(Some(Path::new("spec.txt")));
        assert!(none.is_none());
    }

    #[test]
    fn read_payload_rejects_missing_source() {
        let args = BundleApplyArgs {
            service: "svc".into(),
            namespace: None,
            file: None,
            stdin: false,
            format: None,
            field_manager: "fm".into(),
            force: false,
            dry_run: false,
            curl: false,
        };
        let err = read_payload(&args).unwrap_err();
        assert_eq!(
            err.downcast_ref::<io::Error>()
                .map(io::Error::kind)
                .unwrap_or(io::ErrorKind::Other),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn export_target_requires_destination() {
        let args = sample_export_args();
        let err = export_target(&args).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn export_target_rejects_conflicting_destination_flags() {
        let mut args = sample_export_args();
        args.stdout = true;
        args.output = Some(PathBuf::from("bundle.tar"));
        let err = export_target(&args).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn export_target_accepts_stdout() {
        let mut args = sample_export_args();
        args.stdout = true;
        assert!(matches!(export_target(&args), Ok(ExportTarget::Stdout)));
    }

    #[test]
    fn export_target_accepts_path() {
        let mut args = sample_export_args();
        args.output = Some(PathBuf::from("bundle.tar"));
        match export_target(&args).expect("path target") {
            ExportTarget::File(path) => assert_eq!(path, Path::new("bundle.tar")),
            ExportTarget::Stdout => panic!("expected file target"),
        }
    }

    fn sample_export_args() -> BundleExportArgs {
        BundleExportArgs {
            service: "svc".into(),
            namespace: None,
            output: None,
            stdout: false,
            include_secrets: false,
            curl: false,
        }
    }
}
