use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkspaceScope {
    Local,
    Synced,
    Development,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineInfo {
    pub device_id: String,
    pub workspace_scope: WorkspaceScope,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_scope_uses_wire_safe_names() {
        for (scope, encoded) in [
            (WorkspaceScope::Local, "\"local\""),
            (WorkspaceScope::Synced, "\"synced\""),
            (WorkspaceScope::Development, "\"development\""),
        ] {
            assert_eq!(serde_json::to_string(&scope).unwrap(), encoded);
            assert_eq!(
                serde_json::from_str::<WorkspaceScope>(encoded).unwrap(),
                scope
            );
        }
    }

    #[test]
    fn engine_info_uses_camel_case_fields() {
        let info = EngineInfo {
            device_id: "device-1".into(),
            workspace_scope: WorkspaceScope::Local,
        };
        assert_eq!(
            serde_json::to_value(&info).unwrap(),
            serde_json::json!({
                "deviceId": "device-1",
                "workspaceScope": "local",
            })
        );
    }
}
