use std::fmt;

use serde::Deserialize;

use super::error::ApiError;

pub const CHANNEL_STDIN: u8 = 0;
pub const CHANNEL_STDOUT: u8 = 1;
pub const CHANNEL_STDERR: u8 = 2;
pub const CHANNEL_STATUS: u8 = 3;
pub const CHANNEL_RESIZE: u8 = 4;
pub const CHANNEL_CLOSE: u8 = 255;

#[derive(Debug, Clone)]
pub struct ResizeEvent {
    pub width: u16,
    pub height: u16,
}

#[derive(Debug, Clone)]
pub struct ExecOptions {
    pub namespace: String,
    pub pod: String,
    pub container: Option<String>,
    pub command: Vec<String>,
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

#[derive(Debug)]
pub struct RawExecQuery {
    pub namespace: String,
    pub pod: String,
    pub container: Option<String>,
    pub command: Vec<String>,
    pub stdin: Option<bool>,
    pub stdout: Option<bool>,
    pub stderr: Option<bool>,
    pub tty: Option<bool>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CommandField {
    Single(String),
    Many(Vec<String>),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum MaybeBool {
    Bool(bool),
    String(String),
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum RawExecField {
    Namespace,
    Pod,
    Container,
    Command,
    Stdin,
    Stdout,
    Stderr,
    Tty,
}

impl<'de> Deserialize<'de> for RawExecQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RawExecQuery;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("exec query parameters")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut namespace: Option<String> = None;
                let mut pod: Option<String> = None;
                let mut container: Option<Option<String>> = None;
                let mut command: Vec<String> = Vec::new();
                let mut stdin: Option<Option<bool>> = None;
                let mut stdout: Option<Option<bool>> = None;
                let mut stderr: Option<Option<bool>> = None;
                let mut tty: Option<Option<bool>> = None;

                while let Some(field) = map.next_key::<RawExecField>()? {
                    match field {
                        RawExecField::Namespace => {
                            if namespace.is_some() {
                                return Err(serde::de::Error::duplicate_field("namespace"));
                            }
                            namespace = Some(map.next_value()?);
                        }
                        RawExecField::Pod => {
                            if pod.is_some() {
                                return Err(serde::de::Error::duplicate_field("pod"));
                            }
                            pod = Some(map.next_value()?);
                        }
                        RawExecField::Container => {
                            if container.is_some() {
                                return Err(serde::de::Error::duplicate_field("container"));
                            }
                            container = Some(map.next_value()?);
                        }
                        RawExecField::Command => {
                            let value: CommandField = map.next_value()?;
                            match value {
                                CommandField::Single(cmd) => command.push(cmd),
                                CommandField::Many(list) => command.extend(list),
                            }
                        }
                        RawExecField::Stdin => {
                            if stdin.is_some() {
                                return Err(serde::de::Error::duplicate_field("stdin"));
                            }
                            let value: MaybeBool = map.next_value()?;
                            stdin = Some(parse_bool_param::<A::Error>(value)?);
                        }
                        RawExecField::Stdout => {
                            if stdout.is_some() {
                                return Err(serde::de::Error::duplicate_field("stdout"));
                            }
                            let value: MaybeBool = map.next_value()?;
                            stdout = Some(parse_bool_param::<A::Error>(value)?);
                        }
                        RawExecField::Stderr => {
                            if stderr.is_some() {
                                return Err(serde::de::Error::duplicate_field("stderr"));
                            }
                            let value: MaybeBool = map.next_value()?;
                            stderr = Some(parse_bool_param::<A::Error>(value)?);
                        }
                        RawExecField::Tty => {
                            if tty.is_some() {
                                return Err(serde::de::Error::duplicate_field("tty"));
                            }
                            let value: MaybeBool = map.next_value()?;
                            tty = Some(parse_bool_param::<A::Error>(value)?);
                        }
                    }
                }

                // The path segment carries the pod/namespace; allow them to be absent on the query itself.
                let namespace = namespace.unwrap_or_else(default_namespace);
                let pod = pod.unwrap_or_default();
                let container = container.unwrap_or(None);

                Ok(RawExecQuery {
                    namespace,
                    pod,
                    container,
                    command,
                    stdin: stdin.unwrap_or(None),
                    stdout: stdout.unwrap_or(None),
                    stderr: stderr.unwrap_or(None),
                    tty: tty.unwrap_or(None),
                })
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecValidationError {
    MissingPod,
    MissingCommand,
    NoStreamsRequested,
}

impl fmt::Display for ExecValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecValidationError::MissingPod => {
                write!(f, "`pod` query parameter is required")
            }
            ExecValidationError::MissingCommand => {
                write!(f, "at least one `command` query parameter must be provided")
            }
            ExecValidationError::NoStreamsRequested => {
                write!(
                    f,
                    "at least one of stdin, stdout, stderr, or tty must be enabled"
                )
            }
        }
    }
}

impl From<ExecValidationError> for ApiError {
    fn from(value: ExecValidationError) -> Self {
        ApiError::bad_request(value.to_string())
    }
}

pub fn validate_query(raw: RawExecQuery) -> Result<ExecOptions, ExecValidationError> {
    if raw.pod.trim().is_empty() {
        return Err(ExecValidationError::MissingPod);
    }

    if raw.command.is_empty() {
        return Err(ExecValidationError::MissingCommand);
    }

    let tty = raw.tty.unwrap_or(false);
    let stdin = raw.stdin.unwrap_or(false);
    let stdout = if tty {
        true
    } else {
        raw.stdout.unwrap_or(false)
    };
    let stderr = if tty {
        false
    } else {
        raw.stderr.unwrap_or(false)
    };

    if !(stdin || stdout || stderr || tty) {
        return Err(ExecValidationError::NoStreamsRequested);
    }

    Ok(ExecOptions {
        namespace: raw.namespace,
        pod: raw.pod,
        container: raw.container,
        command: raw.command,
        stdin,
        stdout,
        stderr,
        tty,
    })
}

pub fn parse_resize_payload(payload: &[u8]) -> Result<ResizeEvent, String> {
    #[derive(Deserialize)]
    struct Body {
        #[serde(alias = "Width")]
        width: u16,
        #[serde(alias = "Height")]
        height: u16,
    }

    let body: Body = serde_json::from_slice(payload)
        .map_err(|err| format!("invalid resize payload: {}", err))?;
    if body.width == 0 || body.height == 0 {
        return Err("resize width and height must be greater than zero".into());
    }
    Ok(ResizeEvent {
        width: body.width,
        height: body.height,
    })
}

fn default_namespace() -> String {
    "default".to_string()
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

fn parse_bool_param<E>(value: MaybeBool) -> Result<Option<bool>, E>
where
    E: serde::de::Error,
{
    match value {
        MaybeBool::Bool(b) => Ok(Some(b)),
        MaybeBool::String(s) => parse_bool(&s).map(Some).ok_or_else(|| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&s),
                &"a boolean value such as true/false/1/0",
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_urlencoded;

    #[test]
    fn rejects_missing_command() {
        let raw = RawExecQuery {
            namespace: "default".into(),
            pod: "mypod".into(),
            container: None,
            command: Vec::new(),
            stdin: Some(true),
            stdout: Some(true),
            stderr: Some(false),
            tty: Some(false),
        };
        let err = validate_query(raw).unwrap_err();
        assert_eq!(err, ExecValidationError::MissingCommand);
    }

    #[test]
    fn ensures_stream_requested() {
        let raw = RawExecQuery {
            namespace: "default".into(),
            pod: "mypod".into(),
            container: None,
            command: vec!["/bin/true".into()],
            stdin: Some(false),
            stdout: Some(false),
            stderr: Some(false),
            tty: Some(false),
        };
        let err = validate_query(raw).unwrap_err();
        assert_eq!(err, ExecValidationError::NoStreamsRequested);
    }

    #[test]
    fn tty_forces_stdout() {
        let raw = RawExecQuery {
            namespace: "default".into(),
            pod: "mypod".into(),
            container: None,
            command: vec!["sh".into()],
            stdin: Some(true),
            stdout: Some(false),
            stderr: Some(true),
            tty: Some(true),
        };
        let opts = validate_query(raw).expect("valid options");
        assert!(opts.tty);
        assert!(opts.stdout);
        assert!(!opts.stderr);
    }

    #[test]
    fn parses_resize_payload() {
        let payload = br#"{"width":80,"height":24}"#;
        let event = parse_resize_payload(payload).expect("expected resize event");
        assert_eq!(event.width, 80);
        assert_eq!(event.height, 24);
    }

    #[test]
    fn rejects_zero_dimensions() {
        let payload = br#"{"width":0,"height":10}"#;
        assert!(parse_resize_payload(payload).is_err());
    }

    #[test]
    fn parses_multiple_command_query_params() {
        let raw: RawExecQuery = serde_urlencoded::from_str(
            "pod=kafka&namespace=default&command=ls&command=%2F&stdin=true&stdout=true&tty=true",
        )
        .expect("should parse query");
        assert_eq!(raw.pod, "kafka");
        assert_eq!(raw.namespace, "default");
        assert_eq!(raw.command, vec!["ls", "/"]);
        assert_eq!(raw.stdin, Some(true));
        assert_eq!(raw.stdout, Some(true));
        assert_eq!(raw.tty, Some(true));
    }
}
