# nanocloud CLI applet registry. Points fluxor at the
# system applet catalogue postinst installs the `nanocloud` applet into, so
# `nanocloud <cmd>` (the busybox symlink /usr/bin/nanocloud → fluxor) resolves it
# in an interactive shell.
export FLUXOR_APPLETS=/var/lib/nanocloud.io/fluxor/applets.toml
