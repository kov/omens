use crate::config::AnalysisConfig;
use serde_json::Value;

pub struct AnalysisItem {
    pub item_id: i64,
    pub section: String,
    pub stable_key: String,
    pub payload_json: String,
    pub is_new: bool,
}

pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
    Ignore,
}

impl Severity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
            Self::Ignore => "ignore",
        }
    }

    pub fn rank(&self) -> u8 {
        match self {
            Self::Critical => 4,
            Self::High => 3,
            Self::Medium => 2,
            Self::Low => 1,
            Self::Ignore => 0,
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "critical" => Some(Self::Critical),
            "high" => Some(Self::High),
            "medium" => Some(Self::Medium),
            "low" => Some(Self::Low),
            "ignore" => Some(Self::Ignore),
            _ => None,
        }
    }
}

pub struct ItemSignal {
    pub item_id: i64,
    pub kind: String,
    pub severity: Severity,
    pub confidence: f64,
    pub reasons: Vec<String>,
    pub summary: String,
}

/// Parse payload_json ([["key","val"],...]) into key-value pairs.
fn parse_payload_fields(payload_json: &str) -> Vec<(String, String)> {
    let parsed: Result<Vec<Vec<String>>, _> = serde_json::from_str(payload_json);
    match parsed {
        Ok(rows) => rows
            .into_iter()
            .filter_map(|pair| {
                if pair.len() >= 2 {
                    Some((pair[0].clone(), pair[1].clone()))
                } else {
                    None
                }
            })
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Find first matching field value by trying multiple candidate keys.
fn find_field<'a>(fields: &'a [(String, String)], candidates: &[&str]) -> Option<&'a str> {
    for candidate in candidates {
        if let Some((_, v)) = fields.iter().find(|(k, _)| k.as_str() == *candidate) {
            return Some(v.as_str());
        }
    }
    None
}

pub fn score_rules(item: &AnalysisItem) -> Option<ItemSignal> {
    let fields = parse_payload_fields(&item.payload_json);

    match item.section.as_str() {
        "comunicados" => {
            let mut severity = Severity::Medium;
            let mut confidence = 0.75f64;
            let mut reasons: Vec<String> = Vec::new();

            let all_values: String = fields
                .iter()
                .map(|(_, v)| v.to_lowercase())
                .collect::<Vec<_>>()
                .join(" ");

            if all_values.contains("fato relevante") {
                severity = Severity::High;
                confidence = 0.9;
                reasons.push("contains 'fato relevante'".to_string());
            } else if all_values.contains("relatório gerencial")
                || all_values.contains("relatorio gerencial")
            {
                severity = Severity::High;
                confidence = 0.85;
                reasons.push("management report (relatório gerencial)".to_string());
            } else if all_values.contains("assembleia") || all_values.contains("alteração") {
                confidence = 0.8;
                if all_values.contains("assembleia") {
                    reasons.push("contains 'assembleia'".to_string());
                }
                if all_values.contains("alteração") {
                    reasons.push("contains 'alteração'".to_string());
                }
            }

            let change_label = if item.is_new {
                "new announcement"
            } else {
                "announcement changed"
            };

            if reasons.is_empty() {
                reasons.push(change_label.to_string());
            }

            let summary = format!("{}: {}", change_label, item.stable_key);

            Some(ItemSignal {
                item_id: item.item_id,
                kind: "announcement".to_string(),
                severity,
                confidence,
                reasons,
                summary,
            })
        }

        "proventos" => {
            let valor = find_field(&fields, &["VALOR", "valor", "Valor"]);
            let tipo = find_field(&fields, &["TIPO", "tipo", "Tipo"]);

            let valor_upper = valor.map(|v| v.to_uppercase());
            let tipo_upper = tipo.map(|t| t.to_uppercase());

            let is_nao_distribuicao = tipo_upper.as_deref().map(|t| t.contains("NÃO")).unwrap_or(false)
                || valor_upper
                    .as_deref()
                    .map(|v| v.contains("NÃO") || v == "0,000" || v == "0")
                    .unwrap_or(false);

            if is_nao_distribuicao {
                return Some(ItemSignal {
                    item_id: item.item_id,
                    kind: "dividend".to_string(),
                    severity: Severity::Low,
                    confidence: 0.7,
                    reasons: vec!["NÃO DISTRIBUIÇÃO".to_string()],
                    summary: format!("no distribution declared: {}", item.stable_key),
                });
            }

            if item.is_new {
                let summary = format!(
                    "new dividend: {}{}",
                    item.stable_key,
                    valor.map(|v| format!(" valor={v}")).unwrap_or_default()
                );
                Some(ItemSignal {
                    item_id: item.item_id,
                    kind: "dividend".to_string(),
                    severity: Severity::Medium,
                    confidence: 0.85,
                    reasons: vec!["new positive dividend".to_string()],
                    summary,
                })
            } else {
                let summary = format!(
                    "dividend amount revised: {}{}",
                    item.stable_key,
                    valor.map(|v| format!(" valor={v}")).unwrap_or_default()
                );
                Some(ItemSignal {
                    item_id: item.item_id,
                    kind: "dividend".to_string(),
                    severity: Severity::High,
                    confidence: 0.9,
                    reasons: vec!["dividend amount revised".to_string()],
                    summary,
                })
            }
        }

        "informacoes_basicas" => {
            if item.is_new {
                // New rows are baseline data, not actionable signals
                None
            } else {
                Some(ItemSignal {
                    item_id: item.item_id,
                    kind: "fund_info_change".to_string(),
                    severity: Severity::Medium,
                    confidence: 0.7,
                    reasons: vec!["fund basic information changed".to_string()],
                    summary: format!("fund info changed: {}", item.stable_key),
                })
            }
        }

        "cotacoes" => None,

        _ => {
            if item.is_new {
                Some(ItemSignal {
                    item_id: item.item_id,
                    kind: "unknown".to_string(),
                    severity: Severity::Low,
                    confidence: 0.5,
                    reasons: vec!["new item in unknown section".to_string()],
                    summary: format!("new item: {} ({})", item.stable_key, item.section),
                })
            } else {
                None
            }
        }
    }
}

/// Attempt to enhance the rules signal using LM Studio. Returns None on any failure.
pub fn score_lmstudio(
    item: &AnalysisItem,
    rules_signal: &ItemSignal,
    cfg: &AnalysisConfig,
) -> Option<ItemSignal> {
    if !cfg.lmstudio.enabled || cfg.lmstudio.model.is_empty() {
        return None;
    }

    let max_chars = cfg.lmstudio.max_input_chars as usize;
    let payload_truncated = if item.payload_json.len() > max_chars {
        &item.payload_json[..max_chars]
    } else {
        &item.payload_json
    };

    let prompt = format!(
        "Analyze this financial data item and return a JSON object.\n\
         Section: {}\n\
         Key: {}\n\
         Is new: {}\n\
         Fields: {}\n\
         Rules pre-score: severity={}, confidence={:.2}\n\
         Return only valid JSON: \
         {{\"severity\":\"critical|high|medium|low|ignore\",\
         \"confidence\":0.0-1.0,\"reasons\":[\"...\"],\"summary\":\"...\"}}",
        item.section,
        item.stable_key,
        item.is_new,
        payload_truncated,
        rules_signal.severity.as_str(),
        rules_signal.confidence,
    );

    let request_body = serde_json::json!({
        "model": cfg.lmstudio.model,
        "messages": [
            {
                "role": "system",
                "content": "You are a financial data analyst. Analyze items and return strict JSON with severity, confidence, reasons array, and summary string."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    });

    let request_body_str = serde_json::to_string(&request_body).ok()?;

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .ok()?;

    let url = format!("{}/chat/completions", cfg.lmstudio.base_url);
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(request_body_str)
        .send()
        .ok()?;

    let response_text = response.text().ok()?;
    let response_json: Value = serde_json::from_str(&response_text).ok()?;

    // Extract content from choices[0].message.content
    let content = response_json
        .get("choices")?
        .get(0)?
        .get("message")?
        .get("content")?
        .as_str()?;

    let parsed: Value = serde_json::from_str(content).ok()?;

    let severity_str = parsed.get("severity")?.as_str()?;
    let severity = Severity::from_str(severity_str)?;

    let confidence = parsed.get("confidence")?.as_f64()?;
    if !(0.0..=1.0).contains(&confidence) {
        return None;
    }

    let reasons: Vec<String> = parsed
        .get("reasons")?
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let summary = parsed.get("summary")?.as_str()?.to_string();

    Some(ItemSignal {
        item_id: item.item_id,
        kind: rules_signal.kind.clone(),
        severity,
        confidence,
        reasons,
        summary,
    })
}

pub fn analyze_items(items: &[AnalysisItem], cfg: &AnalysisConfig) -> Vec<ItemSignal> {
    let mut signals: Vec<ItemSignal> = items
        .iter()
        .filter_map(|item| {
            let rules_signal = score_rules(item)?;
            let signal = score_lmstudio(item, &rules_signal, cfg).unwrap_or(rules_signal);
            Some(signal)
        })
        .filter(|sig| sig.confidence >= cfg.thresholds.low_confidence)
        .collect();

    signals.sort_by(|a, b| {
        b.severity
            .rank()
            .cmp(&a.severity.rank())
            .then(b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal))
    });

    signals
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AnalysisConfig, AnalysisThresholds, LmStudioConfig};

    fn default_cfg() -> AnalysisConfig {
        AnalysisConfig {
            lmstudio: LmStudioConfig {
                enabled: false,
                base_url: "http://127.0.0.1:1234/v1".to_string(),
                model: String::new(),
                max_input_chars: 12000,
            },
            thresholds: AnalysisThresholds {
                high_impact: 0.8,
                low_confidence: 0.3,
            },
        }
    }

    fn make_item(section: &str, payload_json: &str, is_new: bool) -> AnalysisItem {
        AnalysisItem {
            item_id: 1,
            section: section.to_string(),
            stable_key: "test_key".to_string(),
            payload_json: payload_json.to_string(),
            is_new,
        }
    }

    #[test]
    fn comunicados_new_item_is_medium() {
        let item = make_item("comunicados", r#"[["titulo","Aviso ao mercado"]]"#, true);
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.75);
        assert_eq!(sig.kind, "announcement");
    }

    #[test]
    fn comunicados_fato_relevante_is_high() {
        let item = make_item(
            "comunicados",
            r#"[["titulo","Fato Relevante: resultado trimestral"]]"#,
            true,
        );
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("fato relevante")));
    }

    #[test]
    fn comunicados_relatorio_gerencial_is_high() {
        let item = make_item(
            "comunicados",
            r#"[["titulo","RelatóriosRelatório Gerencial"]]"#,
            true,
        );
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.85);
        assert!(sig.reasons.iter().any(|r| r.contains("relatório gerencial")));
    }

    #[test]
    fn comunicados_changed_item_is_medium() {
        let item = make_item("comunicados", r#"[["titulo","Comunicado"]]"#, false);
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.75);
    }

    #[test]
    fn proventos_nao_distribuicao_is_low() {
        let item = make_item(
            "proventos",
            r#"[["TIPO","NÃO DISTRIBUIÇÃO"],["VALOR","0,000"]]"#,
            true,
        );
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Low));
        assert_eq!(sig.confidence, 0.7);
        assert!(sig.reasons.iter().any(|r| r.contains("NÃO DISTRIBUIÇÃO")));
    }

    #[test]
    fn proventos_positive_dividend_new_is_medium() {
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,50"]]"#,
            true,
        );
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.85);
        assert!(sig.reasons.iter().any(|r| r.contains("new positive dividend")));
    }

    #[test]
    fn proventos_changed_is_high() {
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,75"]]"#,
            false,
        );
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("revised")));
    }

    #[test]
    fn cotacoes_always_none() {
        let item = make_item("cotacoes", r#"[["data","2024-01"],["valor","100"]]"#, true);
        assert!(score_rules(&item).is_none());
    }

    #[test]
    fn cotacoes_changed_also_none() {
        let item = make_item("cotacoes", r#"[["data","2024-01"],["valor","105"]]"#, false);
        assert!(score_rules(&item).is_none());
    }

    #[test]
    fn informacoes_basicas_new_is_none() {
        let item = make_item("informacoes_basicas", r#"[["campo","valor"]]"#, true);
        assert!(score_rules(&item).is_none());
    }

    #[test]
    fn informacoes_basicas_changed_is_medium() {
        let item = make_item("informacoes_basicas", r#"[["campo","valor_novo"]]"#, false);
        let sig = score_rules(&item).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.7);
        assert_eq!(sig.kind, "fund_info_change");
    }

    #[test]
    fn unknown_section_new_is_low() {
        let item = make_item("other_section", r#"[["field","value"]]"#, true);
        let sig = score_rules(&item).expect("should produce signal for new unknown");
        assert!(matches!(sig.severity, Severity::Low));
        assert_eq!(sig.confidence, 0.5);
    }

    #[test]
    fn unknown_section_changed_is_none() {
        let item = make_item("other_section", r#"[["field","changed"]]"#, false);
        assert!(score_rules(&item).is_none());
    }

    #[test]
    fn analyze_items_filters_by_confidence_threshold() {
        let items = vec![
            AnalysisItem {
                item_id: 1,
                section: "proventos".to_string(),
                stable_key: "key1".to_string(),
                payload_json: r#"[["TIPO","RENDIMENTO"],["VALOR","1,50"]]"#.to_string(),
                is_new: true,
            },
            AnalysisItem {
                item_id: 2,
                section: "cotacoes".to_string(),
                stable_key: "key2".to_string(),
                payload_json: r#"[["data","2024-01"]]"#.to_string(),
                is_new: true,
            },
        ];

        // proventos new → confidence 0.85; cotacoes → None; threshold 0.8
        let mut cfg = default_cfg();
        cfg.thresholds.low_confidence = 0.8;
        let signals = analyze_items(&items, &cfg);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].item_id, 1);
    }

    #[test]
    fn analyze_items_sorted_by_severity_then_confidence() {
        let items = vec![
            AnalysisItem {
                item_id: 10,
                section: "informacoes_basicas".to_string(),
                stable_key: "k1".to_string(),
                payload_json: r#"[["campo","x"]]"#.to_string(),
                is_new: false, // Medium 0.7
            },
            AnalysisItem {
                item_id: 20,
                section: "proventos".to_string(),
                stable_key: "k2".to_string(),
                payload_json: r#"[["TIPO","RENDIMENTO"],["VALOR","2,00"]]"#.to_string(),
                is_new: false, // High 0.9
            },
            AnalysisItem {
                item_id: 30,
                section: "comunicados".to_string(),
                stable_key: "k3".to_string(),
                payload_json: r#"[["titulo","Aviso"]]"#.to_string(),
                is_new: true, // Medium 0.75
            },
        ];

        let cfg = default_cfg();
        let signals = analyze_items(&items, &cfg);

        assert_eq!(signals.len(), 3);
        // First: High (proventos changed)
        assert_eq!(signals[0].item_id, 20);
        // Then Medium 0.75 (comunicados) before Medium 0.7 (informacoes_basicas)
        assert_eq!(signals[1].item_id, 30);
        assert_eq!(signals[2].item_id, 10);
    }

    #[test]
    fn score_lmstudio_skipped_when_model_empty() {
        let item = make_item("comunicados", r#"[["titulo","Test"]]"#, true);
        let rules_sig = score_rules(&item).unwrap();
        let cfg = default_cfg(); // model is empty, enabled=false
        let result = score_lmstudio(&item, &rules_sig, &cfg);
        assert!(result.is_none());
    }

    #[test]
    fn score_lmstudio_returns_none_on_network_failure() {
        // model is set but server is unreachable — should return None
        let item = make_item("comunicados", r#"[["titulo","Test"]]"#, true);
        let rules_sig = score_rules(&item).unwrap();

        let mut cfg = default_cfg();
        cfg.lmstudio.enabled = true;
        cfg.lmstudio.model = "test-model".to_string();
        cfg.lmstudio.base_url = "http://127.0.0.1:1".to_string(); // unreachable

        let result = score_lmstudio(&item, &rules_sig, &cfg);
        assert!(result.is_none());
    }
}
