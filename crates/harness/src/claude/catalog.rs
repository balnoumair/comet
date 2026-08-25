use zeron_proto::{Model, ModelOption, ModelOptionChoice, ReasoningLevel};

pub(crate) const ULTRATHINK_PREFIX: &str = "Ultrathink:\n";

pub(crate) fn apply_ultrathink(reasoning: Option<ReasoningLevel>, text: &str) -> String {
    if reasoning == Some(ReasoningLevel::Ultrathink) {
        format!("{ULTRATHINK_PREFIX}{text}")
    } else {
        text.to_owned()
    }
}

fn contains_any(hay: &str, needles: &[&str]) -> bool {
    needles.iter().any(|n| hay.contains(n))
}

pub(crate) fn supports_xhigh(model: &str) -> bool {
    contains_any(
        model,
        &[
            "fable-5", "opus-4-7", "opus-4-8", "opus-4-9", "opus-5", "opus-6", "opus-7", "opus-8",
            "opus-9", "sonnet-5", "sonnet-6", "sonnet-7", "sonnet-8", "sonnet-9",
        ],
    )
}

pub(crate) fn to_effort(
    reasoning: Option<ReasoningLevel>,
    model: Option<&str>,
) -> Option<&'static str> {
    let base = match reasoning? {
        ReasoningLevel::Ultrathink => return None,
        ReasoningLevel::Minimal | ReasoningLevel::Low => "low",
        ReasoningLevel::Medium => "medium",
        ReasoningLevel::High => "high",
        ReasoningLevel::XHigh | ReasoningLevel::Ultracode => "xhigh",
        ReasoningLevel::Max | ReasoningLevel::Ultra => "max",
    };
    if base == "xhigh" && !model.is_some_and(supports_xhigh) {
        return Some("max");
    }
    Some(base)
}

fn toggle(id: &str, label: &str) -> ModelOption {
    ModelOption {
        id: id.into(),
        label: label.into(),
        choices: vec![
            ModelOptionChoice {
                id: "off".into(),
                label: "Off".into(),
            },
            ModelOptionChoice {
                id: "on".into(),
                label: "On".into(),
            },
        ],
        default_choice: "off".into(),
    }
}

pub(crate) fn context_window() -> ModelOption {
    ModelOption {
        id: "contextWindow".into(),
        label: "Context Window".into(),
        choices: vec![
            ModelOptionChoice {
                id: "200k".into(),
                label: "200K".into(),
            },
            ModelOptionChoice {
                id: "1m".into(),
                label: "1M".into(),
            },
        ],
        default_choice: "200k".into(),
    }
}

const FULL_LADDER: &[ReasoningLevel] = &[
    ReasoningLevel::Low,
    ReasoningLevel::Medium,
    ReasoningLevel::High,
    ReasoningLevel::XHigh,
    ReasoningLevel::Max,
    ReasoningLevel::Ultracode,
    ReasoningLevel::Ultrathink,
];

const XHIGH_LADDER: &[ReasoningLevel] = &[
    ReasoningLevel::Low,
    ReasoningLevel::Medium,
    ReasoningLevel::High,
    ReasoningLevel::XHigh,
    ReasoningLevel::Max,
    ReasoningLevel::Ultrathink,
];

fn model(
    id: &str,
    label: &str,
    description: &str,
    ladder: &[ReasoningLevel],
    options: Vec<ModelOption>,
) -> Model {
    Model {
        id: id.into(),
        label: label.into(),
        description: (!description.is_empty()).then(|| description.into()),
        reasoning_levels: ladder.to_vec(),
        options,
    }
}

pub fn static_models() -> Vec<Model> {
    vec![
        model(
            "claude-fable-5",
            "Fable 5",
            "Most intelligent model for building agents",
            FULL_LADDER,
            vec![context_window()],
        ),
        model(
            "claude-opus-5",
            "Opus 5",
            "Powerful model for complex work",
            FULL_LADDER,
            vec![context_window(), toggle("fastMode", "Fast Mode")],
        ),
        model(
            "claude-opus-4-8",
            "Opus 4.8",
            "Previous generation Opus",
            FULL_LADDER,
            vec![toggle("fastMode", "Fast Mode")],
        ),
        model(
            "claude-opus-4-7",
            "Opus 4.7",
            "Older generation Opus",
            XHIGH_LADDER,
            vec![toggle("fastMode", "Fast Mode")],
        ),
        model(
            "claude-sonnet-5",
            "Sonnet 5",
            "Balanced speed and intelligence",
            XHIGH_LADDER,
            vec![context_window()],
        ),
        model(
            "claude-haiku-4-5",
            "Haiku 4.5",
            "Fastest model for everyday tasks",
            &[],
            vec![toggle("thinking", "Thinking")],
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effort_maps_special_modes() {
        assert_eq!(to_effort(None, None), None);
        assert_eq!(to_effort(Some(ReasoningLevel::Ultrathink), None), None);
        assert_eq!(
            to_effort(Some(ReasoningLevel::Minimal), Some("claude-fable-5")),
            Some("low")
        );
        assert_eq!(
            to_effort(Some(ReasoningLevel::Ultra), Some("claude-fable-5")),
            Some("max")
        );
        assert_eq!(
            to_effort(Some(ReasoningLevel::Ultracode), Some("claude-fable-5")),
            Some("xhigh")
        );
        assert_eq!(
            to_effort(Some(ReasoningLevel::XHigh), Some("claude-opus-4-5")),
            Some("max")
        );
        assert_eq!(to_effort(Some(ReasoningLevel::XHigh), None), Some("max"));
    }

    #[test]
    fn xhigh_family_matching() {
        assert!(supports_xhigh("claude-fable-5"));
        assert!(supports_xhigh("claude-opus-5"));
        assert!(supports_xhigh("claude-opus-5[1m]"));
        assert!(supports_xhigh("claude-opus-4-7-20260101"));
        assert!(!supports_xhigh("claude-opus-4-5"));
        assert!(!supports_xhigh("claude-sonnet-4-5"));
    }

    #[test]
    fn ultrathink_prefixes_prompt() {
        assert_eq!(
            apply_ultrathink(Some(ReasoningLevel::Ultrathink), "do it"),
            "Ultrathink:\ndo it"
        );
        assert_eq!(
            apply_ultrathink(Some(ReasoningLevel::Max), "do it"),
            "do it"
        );
    }
}
