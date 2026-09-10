//! Shared package-relative relationship resolution. No filesystem access.
use std::io;

pub(crate) fn resolve(source: &str, target: &str) -> io::Result<String> {
    let invalid = |message| io::Error::new(io::ErrorKind::InvalidData, message);
    if target.contains(['\\', '\0', '?', '#', ':']) {
        return Err(invalid("invalid internal relationship target"));
    }
    let mut parts: Vec<&str> = if target.starts_with('/') {
        Vec::new()
    } else {
        source
            .rsplit_once('/')
            .map(|(p, _)| p.split('/').collect())
            .unwrap_or_default()
    };
    for part in target.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts
                    .pop()
                    .ok_or_else(|| invalid("relationship escapes archive root"))?;
            }
            _ => parts.push(part),
        }
    }
    if parts.is_empty() {
        return Err(invalid("empty relationship target"));
    }
    Ok(parts.join("/"))
}
