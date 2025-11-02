use std::error::Error;
use std::fs;
use std::path::PathBuf;

use crate::nanocloud::cli::args::{VolumeArgs, VolumeCommands, VolumeLockArgs, VolumeUnlockArgs};
use crate::nanocloud::cli::Terminal;
use crate::nanocloud::util::error::{new_error, with_context};
use crate::nanocloud::util::security::volume::{
    cleanup_mount_dir, close_mapper, encrypted_host_mount, encrypted_mapper_name,
    ensure_encrypted_volume_root, ensure_luks_device, mount_mapper, open_mapper, read_volume_key,
    unmount_if_mounted,
};

pub async fn handle_volume(args: &VolumeArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    match &args.command {
        VolumeCommands::Unlock(unlock) => unlock_volume(unlock),
        VolumeCommands::Lock(lock) => lock_volume(lock),
    }
}

fn unlock_volume(args: &VolumeUnlockArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    ensure_encrypted_volume_root()?;

    let mapper = derive_mapper(
        args.mapper.as_deref(),
        args.container.as_deref(),
        args.volume.as_deref(),
    )?;
    let mount_path = derive_mount_path(
        args.mount_path.as_deref(),
        args.container.as_deref(),
        args.volume.as_deref(),
    )?;

    let key = read_volume_key(&args.key_name)?;
    ensure_luks_device(&args.device, &key)?;
    open_mapper(&args.device, &mapper, &key)?;

    fs::create_dir_all(&mount_path).map_err(|e| {
        with_context(
            e,
            format!("Failed to create mount directory {}", mount_path.display()),
        )
    })?;

    mount_mapper(&mapper, &mount_path, &args.filesystem)?;

    Terminal::stdout(format_args!(
        "Unlocked mapper '{}' mounted at {}",
        mapper,
        mount_path.display()
    ));

    Ok(())
}

fn lock_volume(args: &VolumeLockArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mapper = derive_mapper(
        args.mapper.as_deref(),
        args.container.as_deref(),
        args.volume.as_deref(),
    )?;
    let mount_path = derive_mount_path(
        args.mount_path.as_deref(),
        args.container.as_deref(),
        args.volume.as_deref(),
    )?;

    unmount_if_mounted(&mount_path)?;
    cleanup_mount_dir(&mount_path);
    close_mapper(&mapper)?;

    Terminal::stdout(format_args!(
        "Locked mapper '{}' and cleaned {}",
        mapper,
        mount_path.display()
    ));

    Ok(())
}

fn derive_mapper(
    mapper: Option<&str>,
    container: Option<&str>,
    volume: Option<&str>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    if let Some(mapper) = mapper {
        return Ok(mapper.to_string());
    }

    if let (Some(container), Some(volume)) = (container, volume) {
        return Ok(encrypted_mapper_name(container, volume));
    }

    Err(new_error(
        "Provide --mapper or both --container and --volume to derive mapper name",
    ))
}

fn derive_mount_path(
    mount: Option<&str>,
    container: Option<&str>,
    volume: Option<&str>,
) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    if let Some(path) = mount {
        return Ok(PathBuf::from(path));
    }

    if let (Some(container), Some(volume)) = (container, volume) {
        return Ok(encrypted_host_mount(container, volume));
    }

    Err(new_error(
        "Provide --mount or both --container and --volume to derive mount path",
    ))
}
