use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    Production,
    Staging,
    Development,
    Local,
    Test,
}

impl Environment {
    pub const ALL: [Self; 5] = [
        Self::Production,
        Self::Staging,
        Self::Development,
        Self::Local,
        Self::Test,
    ];
    pub const DEPLOYED: [Self; 2] = [Self::Production, Self::Staging];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Staging => "staging",
            Self::Development => "development",
            Self::Local => "local",
            Self::Test => "test",
        }
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Environment {
    type Err = ParseEnvironmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.trim().eq_ignore_ascii_case("production") {
            Ok(Self::Production)
        } else if value.trim().eq_ignore_ascii_case("staging") {
            Ok(Self::Staging)
        } else if value.trim().eq_ignore_ascii_case("development") {
            Ok(Self::Development)
        } else if value.trim().eq_ignore_ascii_case("local") {
            Ok(Self::Local)
        } else if value.trim().eq_ignore_ascii_case("test") {
            Ok(Self::Test)
        } else {
            Err(ParseEnvironmentError(value.trim().to_owned()))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unsupported environment `{0}`")]
pub struct ParseEnvironmentError(String);
