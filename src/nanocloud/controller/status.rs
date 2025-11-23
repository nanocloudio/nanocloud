use crate::nanocloud::api::types::BundleConditionKind;
use std::fmt::{Display, Formatter};

impl BundleConditionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            BundleConditionKind::InstallReady => "InstallReady",
            BundleConditionKind::BindingsReady => "BindingsReady",
            BundleConditionKind::BackupHealthy => "BackupHealthy",
        }
    }

    #[allow(dead_code)]
    pub const fn summary(self) -> &'static str {
        match self {
            BundleConditionKind::InstallReady => {
                "Bundle workloads are installed on the node and ready to run."
            }
            BundleConditionKind::BindingsReady => "All binding steps finished successfully.",
            BundleConditionKind::BackupHealthy => {
                "Backups and snapshots are available and not degraded."
            }
        }
    }
}

impl Display for BundleConditionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Machine-readable reasons emitted with each condition transition.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BundleConditionReason {
    InstallPending,
    InstallReady,
    InstallFailed,
    BindingsPending,
    BindingsReady,
    BindingsFailed,
    BackupPending,
    BackupHealthy,
    BackupFailed,
    Uninstalling,
}

impl BundleConditionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            BundleConditionReason::InstallPending => "InstallPending",
            BundleConditionReason::InstallReady => "InstallReady",
            BundleConditionReason::InstallFailed => "InstallFailed",
            BundleConditionReason::BindingsPending => "BindingsPending",
            BundleConditionReason::BindingsReady => "BindingsReady",
            BundleConditionReason::BindingsFailed => "BindingsFailed",
            BundleConditionReason::BackupPending => "BackupPending",
            BundleConditionReason::BackupHealthy => "BackupHealthy",
            BundleConditionReason::BackupFailed => "BackupFailed",
            BundleConditionReason::Uninstalling => "Uninstalling",
        }
    }

    #[allow(dead_code)]
    pub const fn description(self) -> &'static str {
        match self {
            BundleConditionReason::InstallPending => "Controller is preparing bundle installation.",
            BundleConditionReason::InstallReady => "Workload install/start completed successfully.",
            BundleConditionReason::InstallFailed => "Install or start failed.",
            BundleConditionReason::BindingsPending => "Bindings are queued but not finished yet.",
            BundleConditionReason::BindingsReady => "All bindings executed successfully.",
            BundleConditionReason::BindingsFailed => "One or more bindings failed.",
            BundleConditionReason::BackupPending => "Backup and snapshot health not yet assessed.",
            BundleConditionReason::BackupHealthy => "Backups and snapshots are available.",
            BundleConditionReason::BackupFailed => "Backups or snapshots failed or are missing.",
            BundleConditionReason::Uninstalling => "Bundle is being uninstalled and cleaned up.",
        }
    }
}

impl Display for BundleConditionReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returns the upstream conditions that must be satisfied before a given
/// condition can transition to `True`.
#[allow(dead_code)]
pub const fn dependencies(kind: BundleConditionKind) -> &'static [BundleConditionKind] {
    match kind {
        BundleConditionKind::BindingsReady => &[BundleConditionKind::InstallReady],
        BundleConditionKind::BackupHealthy => &[BundleConditionKind::InstallReady],
        BundleConditionKind::InstallReady => &[],
    }
}

/// Logical ordering for presenting bundle conditions in tables/CLI handlers.
#[allow(dead_code)]
pub const fn default_condition_order() -> &'static [BundleConditionKind] {
    &[
        BundleConditionKind::InstallReady,
        BundleConditionKind::BindingsReady,
        BundleConditionKind::BackupHealthy,
    ]
}
