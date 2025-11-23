use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

/// Generates a Kubernetes-style UID string.
/// This is not a true UUID but follows the same entropy and length expectations.
pub fn new_uid() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(26)
        .map(char::from)
        .collect()
}
