use crate::nanocloud::api::types::BundleConditionKind;
use std::fmt::{Display, Formatter};

impl BundleConditionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            BundleConditionKind::Ready => "Ready",
            BundleConditionKind::Bound => "Bound",
            BundleConditionKind::SecretsProvisioned => "SecretsProvisioned",
            BundleConditionKind::ProfilePrepared => "ProfilePrepared",
        }
    }

    #[allow(dead_code)]
    pub const fn summary(self) -> &'static str {
        match self {
            BundleConditionKind::Ready => {
                "Workload applied and running (or start was intentionally skipped)."
            }
            BundleConditionKind::Bound => {
                "Bundle manifest reconciled into workloads and persistent resources."
            }
            BundleConditionKind::SecretsProvisioned => {
                "Profile encryption keys and binding outputs are available."
            }
            BundleConditionKind::ProfilePrepared => {
                "Profile/options resolved and persisted; bindings may execute next."
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
    ProfilePending,
    ProfilePersisted,
    ProfileFailed,
    SecretsPending,
    SecretsDistributed,
    SecretsFailed,
    BindingsPending,
    BindingsCompleted,
    BindingsFailed,
    WorkloadPending,
    WorkloadApplied,
    WorkloadFailed,
    StartPending,
    StartCompleted,
    StartSkipped,
    StartFailed,
}

impl BundleConditionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            BundleConditionReason::ProfilePending => "ProfilePending",
            BundleConditionReason::ProfilePersisted => "ProfilePersisted",
            BundleConditionReason::ProfileFailed => "ProfileFailed",
            BundleConditionReason::SecretsPending => "SecretsPending",
            BundleConditionReason::SecretsDistributed => "SecretsDistributed",
            BundleConditionReason::SecretsFailed => "SecretsFailed",
            BundleConditionReason::BindingsPending => "BindingsPending",
            BundleConditionReason::BindingsCompleted => "BindingsCompleted",
            BundleConditionReason::BindingsFailed => "BindingsFailed",
            BundleConditionReason::WorkloadPending => "WorkloadPending",
            BundleConditionReason::WorkloadApplied => "WorkloadApplied",
            BundleConditionReason::WorkloadFailed => "WorkloadFailed",
            BundleConditionReason::StartPending => "StartPending",
            BundleConditionReason::StartCompleted => "StartCompleted",
            BundleConditionReason::StartSkipped => "StartSkipped",
            BundleConditionReason::StartFailed => "StartFailed",
        }
    }

    #[allow(dead_code)]
    pub const fn description(self) -> &'static str {
        match self {
            BundleConditionReason::ProfilePending => {
                "Controller is resolving Dockyard options and loading snapshots."
            }
            BundleConditionReason::ProfilePersisted => {
                "Resolved options were stored for future reconciliations."
            }
            BundleConditionReason::ProfileFailed => {
                "Profile resolution or snapshot handling failed."
            }
            BundleConditionReason::SecretsPending => {
                "Secrets or encryption keys are being generated."
            }
            BundleConditionReason::SecretsDistributed => {
                "Secrets and encryption material are available for workloads."
            }
            BundleConditionReason::SecretsFailed => {
                "Secrets or encryption material failed to materialize."
            }
            BundleConditionReason::BindingsPending => "Bindings are queued but not finished yet.",
            BundleConditionReason::BindingsCompleted => {
                "All required bindings executed successfully."
            }
            BundleConditionReason::BindingsFailed => "One or more bindings failed.",
            BundleConditionReason::WorkloadPending => {
                "Workload manifests are being rendered/applied."
            }
            BundleConditionReason::WorkloadApplied => "Workload manifests applied successfully.",
            BundleConditionReason::WorkloadFailed => "Workload apply failed.",
            BundleConditionReason::StartPending => {
                "Controller is waiting for workloads to reach Running."
            }
            BundleConditionReason::StartCompleted => "Workloads reported Ready.",
            BundleConditionReason::StartSkipped => {
                "Bundle requested start=false; workloads left stopped."
            }
            BundleConditionReason::StartFailed => "Workloads failed to start.",
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
        BundleConditionKind::Ready => &[BundleConditionKind::Bound],
        BundleConditionKind::Bound => &[
            BundleConditionKind::ProfilePrepared,
            BundleConditionKind::SecretsProvisioned,
        ],
        BundleConditionKind::SecretsProvisioned => &[BundleConditionKind::ProfilePrepared],
        BundleConditionKind::ProfilePrepared => &[],
    }
}

/// Logical ordering for presenting bundle conditions in tables/CLI handlers.
#[allow(dead_code)]
pub const fn default_condition_order() -> &'static [BundleConditionKind] {
    &[
        BundleConditionKind::Ready,
        BundleConditionKind::Bound,
        BundleConditionKind::SecretsProvisioned,
        BundleConditionKind::ProfilePrepared,
    ]
}
