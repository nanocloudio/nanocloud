use std::error::Error;

use tokio::runtime::Handle;

#[allow(dead_code)]
/// Runs a blocking security helper on a dedicated thread pool when an async
/// runtime is available.
///
/// ```
/// # use nanocloud::nanocloud::util::security::run_blocking_security;
/// # let runtime = tokio::runtime::Runtime::new().expect("runtime");
/// # runtime.block_on(async {
/// let value = run_blocking_security("noop", || -> Result<_, std::io::Error> {
///     Ok(42)
/// })
/// .await
/// .expect("blocking call");
/// assert_eq!(value, 42);
/// # });
/// ```
pub async fn run_blocking_security<F, R, E>(operation: &'static str, work: F) -> Result<R, E>
where
    F: FnOnce() -> Result<R, E> + Send + 'static,
    R: Send + 'static,
    E: Error + Send + 'static,
{
    match Handle::try_current() {
        Ok(handle) => handle
            .spawn_blocking(work)
            .await
            .unwrap_or_else(|err| panic!("Security helper '{operation}' panicked: {err}")),
        Err(_) => work(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_blocking_security_executes_work() {
        let result = run_blocking_security("test", || -> Result<_, std::io::Error> {
            Ok(String::from("value"))
        })
        .await
        .expect("blocking result");
        assert_eq!(result, "value");
    }
}
