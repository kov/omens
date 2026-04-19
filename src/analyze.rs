use crate::config::AnalysisConfig;
use serde_json::Value;
use std::collections::HashMap;

pub struct AnalysisItem {
    pub item_id: i64,
    pub section: String,
    pub external_id: String,
    pub stable_key: String,
    pub payload_json: String,
    pub is_new: bool,
    pub prior_payload_json: Option<String>,
    pub published_at: Option<i64>,
}

/// One historical provento entry for a ticker, from a prior run, most-recent-first.
pub struct HistoricalProvento {
    pub is_nao_distribuicao: bool,
    pub valor: Option<String>,
}

impl HistoricalProvento {
    pub fn from_payload(payload_json: &str) -> Self {
        let fields = parse_payload_fields(payload_json);
        let valor = find_field(&fields, &["VALOR", "valor", "Valor"]).map(|s| s.to_string());
        let is_nao_distribuicao = is_nao_distribuicao_fields(&fields);
        Self {
            is_nao_distribuicao,
            valor,
        }
    }
}

/// One historical comunicado entry for a ticker, used to detect republications
/// of the same monthly relatório gerencial within the same calendar month.
pub struct HistoricalComunicado {
    pub published_at: i64,
    pub has_relatorio_gerencial: bool,
}

impl HistoricalComunicado {
    pub fn from_payload(published_at: i64, payload_json: &str) -> Self {
        let lower = payload_json.to_lowercase();
        let has_relatorio_gerencial =
            lower.contains("relatório gerencial") || lower.contains("relatorio gerencial");
        Self {
            published_at,
            has_relatorio_gerencial,
        }
    }
}

#[derive(Default)]
pub struct TickerHistory {
    pub proventos: Vec<HistoricalProvento>,
    pub comunicados: Vec<HistoricalComunicado>,
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

/// Convert epoch seconds (UTC) to (year, month). Only valid for dates after 1970.
fn epoch_year_month(epoch: i64) -> (i64, i64) {
    let days = epoch.div_euclid(86_400);
    let jdn = days + 2_440_588;
    let j = jdn + 32_044;
    let g = j / 146_097;
    let dg = j % 146_097;
    let c = (dg / 36_524 + 1) * 3 / 4;
    let dc = dg - c * 36_524;
    let b = dc / 1_461;
    let db = dc % 1_461;
    let a = (db / 365 + 1) * 3 / 4;
    let da = db - a * 365;
    let y = g * 400 + c * 100 + b * 4 + a;
    let m = (da * 5 + 308) / 153 - 2;
    let year = y - 4800 + (m + 2) / 12;
    let month = (m + 2) % 12 + 1;
    (year, month)
}

fn is_nao_distribuicao_fields(fields: &[(String, String)]) -> bool {
    let valor = find_field(fields, &["VALOR", "valor", "Valor"]);
    let tipo = find_field(fields, &["TIPO", "tipo", "Tipo"]);
    let valor_upper = valor.map(|v| v.to_uppercase());
    let tipo_upper = tipo.map(|t| t.to_uppercase());
    tipo_upper
        .as_deref()
        .map(|t| t.contains("NÃO"))
        .unwrap_or(false)
        || valor_upper
            .as_deref()
            .map(|v| v.contains("NÃO") || v == "0,000" || v == "0")
            .unwrap_or(false)
}

pub fn score_rules(item: &AnalysisItem, history: &TickerHistory) -> Option<ItemSignal> {
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
                // Routine monthly relatório gerencial: HIGH on first occurrence for
                // the ticker in that calendar month, MEDIUM on subsequent ones
                // (republications/retifications of the same reference period).
                let is_republication = item
                    .published_at
                    .map(|cur| {
                        let cur_ym = epoch_year_month(cur);
                        history.comunicados.iter().any(|h| {
                            h.has_relatorio_gerencial && epoch_year_month(h.published_at) == cur_ym
                        })
                    })
                    .unwrap_or(false);
                if is_republication {
                    severity = Severity::Medium;
                    confidence = 0.75;
                    reasons.push("relatório gerencial (republicação no mesmo mês)".to_string());
                } else {
                    severity = Severity::High;
                    confidence = 0.85;
                    reasons.push("management report (relatório gerencial)".to_string());
                }
            } else {
                // Assembleia alone is usually a routine AGO (annual accounts, etc.)
                // and is noisy at MEDIUM. Escalate only when the assembleia also
                // touches a materially decision-bearing keyword.
                const ESCALATORS: &[&str] = &[
                    "alteração",
                    "fusão",
                    "incorporação",
                    "liquidação",
                    "destituição",
                ];
                let has_assembleia = all_values.contains("assembleia");
                let matched_escalators: Vec<&&str> = ESCALATORS
                    .iter()
                    .filter(|k| all_values.contains(*k))
                    .collect();

                if has_assembleia && !matched_escalators.is_empty() {
                    severity = Severity::High;
                    confidence = 0.9;
                    reasons.push("assembleia com matéria material".to_string());
                    for k in &matched_escalators {
                        reasons.push(format!("contains '{}'", k));
                    }
                } else if has_assembleia {
                    severity = Severity::Low;
                    confidence = 0.7;
                    reasons.push("assembleia (rotina)".to_string());
                } else if !matched_escalators.is_empty() {
                    // "alteração" (or other escalator) without assembleia — e.g. a
                    // regulation amendment filed as a standalone comunicado.
                    confidence = 0.8;
                    for k in &matched_escalators {
                        reasons.push(format!("contains '{}'", k));
                    }
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
            let current_nao = is_nao_distribuicao_fields(&fields);

            if current_nao {
                // Count consecutive prior NÃO DISTRIBUIÇÃO months in history.
                let consecutive = history
                    .proventos
                    .iter()
                    .take_while(|h| h.is_nao_distribuicao)
                    .count();
                // Fire CRITICAL exactly once at the transition (4th consecutive
                // NÃO month) and go silent after that: monthly re-notification
                // on an already-known problem distracts from other signals.
                // Reversion to paying is flagged separately as HIGH below.
                let (severity, confidence, reason) = match consecutive {
                    0 => (Severity::High, 0.9, "NÃO DISTRIBUIÇÃO (first occurrence)"),
                    1 | 2 => (Severity::Medium, 0.8, "NÃO DISTRIBUIÇÃO (recurring)"),
                    3 => (
                        Severity::Critical,
                        0.9,
                        "NÃO DISTRIBUIÇÃO (4th consecutive month — pattern cemented)",
                    ),
                    _ => (Severity::Low, 0.7, "NÃO DISTRIBUIÇÃO (established pattern)"),
                };
                return Some(ItemSignal {
                    item_id: item.item_id,
                    kind: "dividend".to_string(),
                    severity,
                    confidence,
                    reasons: vec![reason.to_string()],
                    summary: format!("no distribution declared: {}", item.stable_key),
                });
            }

            if item.is_new {
                // Count how many consecutive prior months were NÃO DISTRIBUIÇÃO.
                let prior_nao = history
                    .proventos
                    .iter()
                    .take_while(|h| h.is_nao_distribuicao)
                    .count();
                // Most recent prior paid value (first non-NÃO entry in history).
                let last_paid = history
                    .proventos
                    .iter()
                    .find(|h| !h.is_nao_distribuicao)
                    .and_then(|h| h.valor.as_deref());

                let current_valor = valor.unwrap_or("").trim().to_string();
                let summary = format!(
                    "new dividend: {}{}",
                    item.stable_key,
                    if current_valor.is_empty() {
                        String::new()
                    } else {
                        format!(" valor={current_valor}")
                    }
                );

                if prior_nao > 0 {
                    // Fund just resumed paying after one or more NÃO months — noteworthy.
                    Some(ItemSignal {
                        item_id: item.item_id,
                        kind: "dividend".to_string(),
                        severity: Severity::High,
                        confidence: 0.9,
                        reasons: vec![format!(
                            "dividend resumed after {prior_nao} month(s) of NÃO DISTRIBUIÇÃO"
                        )],
                        summary,
                    })
                } else if last_paid.map(|p| p == current_valor).unwrap_or(false) {
                    // Same value as last paid month — routine, low informational value.
                    Some(ItemSignal {
                        item_id: item.item_id,
                        kind: "dividend".to_string(),
                        severity: Severity::Low,
                        confidence: 0.75,
                        reasons: vec!["new dividend (same as prior month)".to_string()],
                        summary,
                    })
                } else {
                    // Different value or no history — worth showing.
                    Some(ItemSignal {
                        item_id: item.item_id,
                        kind: "dividend".to_string(),
                        severity: Severity::Medium,
                        confidence: 0.85,
                        reasons: vec!["new dividend (value differs from prior month)".to_string()],
                        summary,
                    })
                }
            } else {
                // Only emit a "revised" signal when the material fields (VALOR or TIPO)
                // actually changed vs. the prior version. Other fields — notably the
                // data-base cotação — shift between runs without any real event, and
                // previously produced spurious HIGH signals.
                if let Some(prior) = item.prior_payload_json.as_deref() {
                    let prior_fields = parse_payload_fields(prior);
                    let prior_valor = find_field(&prior_fields, &["VALOR", "valor", "Valor"]);
                    let prior_tipo = find_field(&prior_fields, &["TIPO", "tipo", "Tipo"]);
                    let cur_tipo = find_field(&fields, &["TIPO", "tipo", "Tipo"]);
                    if prior_valor == valor && prior_tipo == cur_tipo {
                        return None;
                    }
                }
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

/// Extract the ticker from an external_id like "TICKER/section/key".
fn ticker_from_external_id(external_id: &str) -> &str {
    external_id.split('/').next().unwrap_or("")
}

/// Credit-distress markers that deserve a dedicated HIGH signal alongside
/// whatever per-item signal `score_rules` produces. When any of these appears
/// in a comunicado payload, the underlying issue (waiver, covenant breach,
/// payment default, credit-holder assembly, accelerated maturity) tends to be
/// buried inside a routine-looking "Relatório Gerencial" or "Fato Relevante"
/// — splitting it off as its own `credit_event` signal keeps it visible.
const CREDIT_KEYWORDS: &[&str] = &[
    "waiver",
    "covenant",
    "assembleia de credores",
    "inadimplência",
    "vencimento antecipado",
];

/// Emit additional signals beyond the main per-item one from `score_rules`.
/// Returns a (possibly empty) Vec.
pub fn score_extras(item: &AnalysisItem) -> Vec<ItemSignal> {
    let mut out = Vec::new();
    if item.section != "comunicados" {
        return out;
    }
    let all_values: String = parse_payload_fields(&item.payload_json)
        .iter()
        .map(|(_, v)| v.to_lowercase())
        .collect::<Vec<_>>()
        .join(" ");

    let matched: Vec<&&str> = CREDIT_KEYWORDS
        .iter()
        .filter(|k| all_values.contains(*k))
        .collect();
    if matched.is_empty() {
        return out;
    }
    let mut reasons: Vec<String> = vec!["credit event pending".to_string()];
    for k in &matched {
        reasons.push(format!("contains '{}'", k));
    }
    out.push(ItemSignal {
        item_id: item.item_id,
        kind: "credit_event".to_string(),
        severity: Severity::High,
        confidence: 0.9,
        reasons,
        summary: format!("credit event mention: {}", item.stable_key),
    });
    out
}

pub fn analyze_items(
    items: &[AnalysisItem],
    history_map: &HashMap<String, TickerHistory>,
    cfg: &AnalysisConfig,
) -> Vec<ItemSignal> {
    let empty = TickerHistory::default();
    let mut signals: Vec<ItemSignal> = items
        .iter()
        .flat_map(|item| {
            let ticker = ticker_from_external_id(&item.external_id);
            let history = history_map.get(ticker).unwrap_or(&empty);
            let mut per_item: Vec<ItemSignal> = Vec::new();
            if let Some(rules_signal) = score_rules(item, history) {
                let sig = score_lmstudio(item, &rules_signal, cfg).unwrap_or(rules_signal);
                per_item.push(sig);
            }
            per_item.extend(score_extras(item));
            per_item
        })
        .filter(|sig| sig.confidence >= cfg.thresholds.low_confidence)
        .collect();

    signals.sort_by(|a, b| {
        b.severity.rank().cmp(&a.severity.rank()).then(
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
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
            external_id: format!("TEST11/{section}/key"),
            stable_key: "test_key".to_string(),
            payload_json: payload_json.to_string(),
            is_new,
            prior_payload_json: None,
            published_at: None,
        }
    }

    fn make_item_with_prior(
        section: &str,
        payload_json: &str,
        prior_payload_json: &str,
    ) -> AnalysisItem {
        AnalysisItem {
            item_id: 1,
            section: section.to_string(),
            external_id: format!("TEST11/{section}/key"),
            stable_key: "test_key".to_string(),
            payload_json: payload_json.to_string(),
            is_new: false,
            prior_payload_json: Some(prior_payload_json.to_string()),
            published_at: None,
        }
    }

    fn make_comunicado_with_published(
        ticker: &str,
        payload_json: &str,
        published_at: i64,
    ) -> AnalysisItem {
        AnalysisItem {
            item_id: 1,
            section: "comunicados".to_string(),
            external_id: format!("{ticker}/comunicados/key"),
            stable_key: format!("{ticker}_key"),
            payload_json: payload_json.to_string(),
            is_new: true,
            prior_payload_json: None,
            published_at: Some(published_at),
        }
    }

    fn paid(valor: &str) -> HistoricalProvento {
        HistoricalProvento {
            is_nao_distribuicao: false,
            valor: Some(valor.to_string()),
        }
    }

    fn nao() -> HistoricalProvento {
        HistoricalProvento {
            is_nao_distribuicao: true,
            valor: None,
        }
    }

    fn hp(proventos: Vec<HistoricalProvento>) -> TickerHistory {
        TickerHistory {
            proventos,
            comunicados: Vec::new(),
        }
    }

    fn hc(comunicados: Vec<HistoricalComunicado>) -> TickerHistory {
        TickerHistory {
            proventos: Vec::new(),
            comunicados,
        }
    }

    #[test]
    fn comunicados_new_item_is_medium() {
        let item = make_item("comunicados", r#"[["titulo","Aviso ao mercado"]]"#, true);
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
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
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
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
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.85);
        assert!(
            sig.reasons
                .iter()
                .any(|r| r.contains("relatório gerencial"))
        );
    }

    #[test]
    fn epoch_year_month_known_dates() {
        // 2026-03-15 UTC
        assert_eq!(epoch_year_month(1_773_532_800), (2026, 3));
        // 2026-02-15 UTC
        assert_eq!(epoch_year_month(1_771_113_600), (2026, 2));
        // 1970-01-01 UTC
        assert_eq!(epoch_year_month(0), (1970, 1));
    }

    #[test]
    fn comunicados_relatorio_gerencial_republication_same_month_is_medium() {
        // Current relatório published 2026-03-20; a prior relatório for the same
        // ticker was published 2026-03-15 (same calendar month) → republicação.
        let item = make_comunicado_with_published(
            "TEST11",
            r#"[["titulo","Relatório Gerencial jan/26"]]"#,
            1_773_964_800, // 2026-03-20
        );
        let history = hc(vec![HistoricalComunicado {
            published_at: 1_773_532_800, // 2026-03-15
            has_relatorio_gerencial: true,
        }]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.75);
        assert!(sig.reasons.iter().any(|r| r.contains("republicação")));
    }

    #[test]
    fn comunicados_relatorio_gerencial_prior_month_still_high() {
        // Current relatório in March, prior relatório in February → different
        // calendar month, so this is the first-of-March → stays HIGH.
        let item = make_comunicado_with_published(
            "TEST11",
            r#"[["titulo","Relatório Gerencial fev/26"]]"#,
            1_773_532_800, // 2026-03-15
        );
        let history = hc(vec![HistoricalComunicado {
            published_at: 1_771_113_600, // 2026-02-15
            has_relatorio_gerencial: true,
        }]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.85);
    }

    #[test]
    fn comunicados_relatorio_gerencial_no_published_at_falls_back_to_high() {
        // Without a published_at we cannot judge same-month duplication → HIGH
        // (behaves like today's scorer).
        let item = make_item("comunicados", r#"[["titulo","Relatório Gerencial"]]"#, true);
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
    }

    #[test]
    fn comunicados_changed_item_is_medium() {
        let item = make_item("comunicados", r#"[["titulo","Comunicado"]]"#, false);
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.75);
    }

    // --- Assembleia / alteração context-aware tests ---

    #[test]
    fn comunicados_assembleia_alone_is_low() {
        // Bare AGO of annual accounts — routine, should not clutter reports.
        let item = make_item(
            "comunicados",
            r#"[["titulo","Edital de Convocação — Assembleia Geral Ordinária"]]"#,
            true,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Low));
        assert_eq!(sig.confidence, 0.7);
        assert!(sig.reasons.iter().any(|r| r.contains("rotina")));
    }

    #[test]
    fn comunicados_assembleia_with_alteracao_is_high() {
        let item = make_item(
            "comunicados",
            r#"[["titulo","Assembleia Geral Extraordinária — Alteração do Regulamento"]]"#,
            true,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("material")));
    }

    #[test]
    fn comunicados_assembleia_with_liquidacao_is_high() {
        let item = make_item(
            "comunicados",
            r#"[["titulo","Assembleia para deliberação sobre liquidação do fundo"]]"#,
            true,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
    }

    // --- credit_event (score_extras) tests ---

    #[test]
    fn credit_event_fires_on_waiver_mention() {
        let item = make_item(
            "comunicados",
            r#"[["titulo","Relatório Gerencial"],["texto","CRI Café Brasil — concedido waiver de 30 dias para regularização"]]"#,
            true,
        );
        let extras = score_extras(&item);
        assert_eq!(extras.len(), 1);
        assert_eq!(extras[0].kind, "credit_event");
        assert!(matches!(extras[0].severity, Severity::High));
        assert_eq!(extras[0].confidence, 0.9);
        assert!(extras[0].reasons.iter().any(|r| r.contains("waiver")));
    }

    #[test]
    fn credit_event_fires_on_inadimplencia_mention() {
        let item = make_item(
            "comunicados",
            r#"[["titulo","Fato Relevante"],["texto","Inadimplência no pagamento do CRI XYZ"]]"#,
            true,
        );
        let extras = score_extras(&item);
        assert_eq!(extras.len(), 1);
        assert_eq!(extras[0].kind, "credit_event");
    }

    #[test]
    fn credit_event_silent_on_unrelated_comunicado() {
        let item = make_item(
            "comunicados",
            r#"[["titulo","Aviso ao mercado — Relatório Gerencial jan/26"]]"#,
            true,
        );
        assert!(score_extras(&item).is_empty());
    }

    #[test]
    fn credit_event_silent_on_non_comunicado_section() {
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"],["nota","waiver"]]"#,
            true,
        );
        assert!(score_extras(&item).is_empty());
    }

    #[test]
    fn credit_event_coexists_with_main_signal_via_analyze_items() {
        // A Relatório Gerencial mentioning a covenant breach yields TWO signals:
        // the routine HIGH management-report one AND the dedicated credit_event.
        let items = vec![AnalysisItem {
            item_id: 42,
            section: "comunicados".to_string(),
            external_id: "TEST11/comunicados/rel_gerencial".to_string(),
            stable_key: "rel_gerencial".to_string(),
            payload_json: r#"[["titulo","Relatório Gerencial"],["texto","covenant breach informado em 10/04"]]"#.to_string(),
            is_new: true,
            prior_payload_json: None,
            published_at: None,
        }];
        let signals = analyze_items(&items, &HashMap::new(), &default_cfg());
        assert_eq!(signals.len(), 2);
        let kinds: Vec<&str> = signals.iter().map(|s| s.kind.as_str()).collect();
        assert!(kinds.contains(&"announcement"));
        assert!(kinds.contains(&"credit_event"));
    }

    #[test]
    fn comunicados_alteracao_alone_is_medium() {
        // Regulation amendment filed outside an assembleia context — still
        // worth surfacing.
        let item = make_item(
            "comunicados",
            r#"[["titulo","Comunicado de alteração cadastral do administrador"]]"#,
            true,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.8);
    }

    // --- NÃO DISTRIBUIÇÃO context-aware tests ---

    #[test]
    fn proventos_nao_distribuicao_first_is_high() {
        // No prior NÃO in history → HIGH (first occurrence)
        let item = make_item(
            "proventos",
            r#"[["TIPO","NÃO DISTRIBUIÇÃO"],["VALOR","0,000"]]"#,
            true,
        );
        let history = hp(vec![paid("1,00"), paid("1,00")]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("NÃO DISTRIBUIÇÃO")));
    }

    #[test]
    fn proventos_nao_distribuicao_no_history_is_high() {
        // No history at all → HIGH (treat as first occurrence)
        let item = make_item(
            "proventos",
            r#"[["TIPO","NÃO DISTRIBUIÇÃO"],["VALOR","0,000"]]"#,
            true,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
    }

    #[test]
    fn proventos_nao_distribuicao_two_consecutive_is_medium() {
        // 2 prior NÃO → MEDIUM (pattern forming)
        let item = make_item(
            "proventos",
            r#"[["TIPO","NÃO DISTRIBUIÇÃO"],["VALOR","0,000"]]"#,
            true,
        );
        let history = hp(vec![nao(), nao(), paid("1,00")]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.8);
    }

    #[test]
    fn proventos_nao_distribuicao_three_consecutive_is_critical() {
        // 3 prior NÃO means today is the 4th consecutive → one-shot CRITICAL
        // at the transition to "established" regime.
        let item = make_item(
            "proventos",
            r#"[["TIPO","NÃO DISTRIBUIÇÃO"],["VALOR","0,000"]]"#,
            true,
        );
        let history = hp(vec![nao(), nao(), nao(), paid("1,00")]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Critical));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("cemented")));
    }

    #[test]
    fn proventos_nao_distribuicao_four_plus_consecutive_is_low() {
        // 4+ prior NÃO means the pattern was already flagged CRITICAL last
        // month; subsequent months go silent (LOW, hidden from the report).
        let item = make_item(
            "proventos",
            r#"[["TIPO","NÃO DISTRIBUIÇÃO"],["VALOR","0,000"]]"#,
            true,
        );
        let history = hp(vec![nao(), nao(), nao(), nao(), paid("1,00")]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Low));
        assert_eq!(sig.confidence, 0.7);
    }

    // --- New positive dividend context-aware tests ---

    #[test]
    fn proventos_new_dividend_no_history_is_medium() {
        // No history → MEDIUM (can't compare)
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,50"]]"#,
            true,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.85);
    }

    #[test]
    fn proventos_new_dividend_same_as_prior_is_low() {
        // Same value as last paid month → LOW (routine)
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"]]"#,
            true,
        );
        let history = hp(vec![paid("1,00"), paid("1,00"), paid("1,00")]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Low));
        assert_eq!(sig.confidence, 0.75);
    }

    #[test]
    fn proventos_new_dividend_different_from_prior_is_medium() {
        // Different value from last paid month → MEDIUM
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,50"]]"#,
            true,
        );
        let history = hp(vec![paid("1,00"), paid("1,00")]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.85);
    }

    #[test]
    fn proventos_new_dividend_resumed_after_nao_is_high() {
        // Prior month(s) were NÃO DISTRIBUIÇÃO, now paying → HIGH (resumed)
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"]]"#,
            true,
        );
        let history = hp(vec![nao(), nao(), paid("1,00")]);
        let sig = score_rules(&item, &history).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("resumed")));
    }

    #[test]
    fn proventos_changed_is_high() {
        let item = make_item(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,75"]]"#,
            false,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("revised")));
    }

    #[test]
    fn proventos_revised_suppressed_when_only_cotacao_changed() {
        // Real scenario: a new provento is added to the table, index order
        // shifts, and an unrelated row is re-emitted with only its cotação
        // data-base updated. VALOR and TIPO are unchanged → no signal.
        let item = make_item_with_prior(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"],["COTACAO","9,85"]]"#,
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"],["COTACAO","9,72"]]"#,
        );
        assert!(score_rules(&item, &TickerHistory::default()).is_none());
    }

    #[test]
    fn proventos_revised_still_fires_when_valor_changed() {
        let item = make_item_with_prior(
            "proventos",
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,10"],["COTACAO","9,85"]]"#,
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"],["COTACAO","9,85"]]"#,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
        assert_eq!(sig.confidence, 0.9);
        assert!(sig.reasons.iter().any(|r| r.contains("revised")));
    }

    #[test]
    fn proventos_revised_still_fires_when_tipo_changed() {
        // E.g. shifted from RENDIMENTO to AMORTIZAÇÃO — material even at same value.
        let item = make_item_with_prior(
            "proventos",
            r#"[["TIPO","AMORTIZAÇÃO"],["VALOR","1,00"]]"#,
            r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"]]"#,
        );
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::High));
    }

    #[test]
    fn cotacoes_always_none() {
        let item = make_item("cotacoes", r#"[["data","2024-01"],["valor","100"]]"#, true);
        assert!(score_rules(&item, &TickerHistory::default()).is_none());
    }

    #[test]
    fn cotacoes_changed_also_none() {
        let item = make_item("cotacoes", r#"[["data","2024-01"],["valor","105"]]"#, false);
        assert!(score_rules(&item, &TickerHistory::default()).is_none());
    }

    #[test]
    fn informacoes_basicas_new_is_none() {
        let item = make_item("informacoes_basicas", r#"[["campo","valor"]]"#, true);
        assert!(score_rules(&item, &TickerHistory::default()).is_none());
    }

    #[test]
    fn informacoes_basicas_changed_is_medium() {
        let item = make_item("informacoes_basicas", r#"[["campo","valor_novo"]]"#, false);
        let sig = score_rules(&item, &TickerHistory::default()).expect("should produce signal");
        assert!(matches!(sig.severity, Severity::Medium));
        assert_eq!(sig.confidence, 0.7);
        assert_eq!(sig.kind, "fund_info_change");
    }

    #[test]
    fn unknown_section_new_is_low() {
        let item = make_item("other_section", r#"[["field","value"]]"#, true);
        let sig = score_rules(&item, &TickerHistory::default())
            .expect("should produce signal for new unknown");
        assert!(matches!(sig.severity, Severity::Low));
        assert_eq!(sig.confidence, 0.5);
    }

    #[test]
    fn unknown_section_changed_is_none() {
        let item = make_item("other_section", r#"[["field","changed"]]"#, false);
        assert!(score_rules(&item, &TickerHistory::default()).is_none());
    }

    #[test]
    fn analyze_items_filters_by_confidence_threshold() {
        let items = vec![
            AnalysisItem {
                item_id: 1,
                section: "proventos".to_string(),
                external_id: "TEST11/proventos/key1".to_string(),
                stable_key: "key1".to_string(),
                payload_json: r#"[["TIPO","RENDIMENTO"],["VALOR","1,50"]]"#.to_string(),
                is_new: true,
                prior_payload_json: None,
                published_at: None,
            },
            AnalysisItem {
                item_id: 2,
                section: "cotacoes".to_string(),
                external_id: "TEST11/cotacoes/key2".to_string(),
                stable_key: "key2".to_string(),
                payload_json: r#"[["data","2024-01"]]"#.to_string(),
                is_new: true,
                prior_payload_json: None,
                published_at: None,
            },
        ];

        // proventos new, no history → Medium 0.85; cotacoes → None; threshold 0.8
        let mut cfg = default_cfg();
        cfg.thresholds.low_confidence = 0.8;
        let signals = analyze_items(&items, &HashMap::new(), &cfg);
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].item_id, 1);
    }

    #[test]
    fn analyze_items_sorted_by_severity_then_confidence() {
        let items = vec![
            AnalysisItem {
                item_id: 10,
                section: "informacoes_basicas".to_string(),
                external_id: "TEST11/informacoes_basicas/k1".to_string(),
                stable_key: "k1".to_string(),
                payload_json: r#"[["campo","x"]]"#.to_string(),
                is_new: false, // Medium 0.7
                prior_payload_json: None,
                published_at: None,
            },
            AnalysisItem {
                item_id: 20,
                section: "proventos".to_string(),
                external_id: "TEST11/proventos/k2".to_string(),
                stable_key: "k2".to_string(),
                payload_json: r#"[["TIPO","RENDIMENTO"],["VALOR","2,00"]]"#.to_string(),
                is_new: false, // High 0.9 (revised)
                prior_payload_json: Some(r#"[["TIPO","RENDIMENTO"],["VALOR","1,00"]]"#.to_string()),
                published_at: None,
            },
            AnalysisItem {
                item_id: 30,
                section: "comunicados".to_string(),
                external_id: "TEST11/comunicados/k3".to_string(),
                stable_key: "k3".to_string(),
                payload_json: r#"[["titulo","Aviso"]]"#.to_string(),
                is_new: true, // Medium 0.75
                prior_payload_json: None,
                published_at: None,
            },
        ];

        let cfg = default_cfg();
        let signals = analyze_items(&items, &HashMap::new(), &cfg);

        assert_eq!(signals.len(), 3);
        // First: High (proventos revised)
        assert_eq!(signals[0].item_id, 20);
        // Then Medium 0.75 (comunicados) before Medium 0.7 (informacoes_basicas)
        assert_eq!(signals[1].item_id, 30);
        assert_eq!(signals[2].item_id, 10);
    }

    #[test]
    fn score_lmstudio_skipped_when_model_empty() {
        let item = make_item("comunicados", r#"[["titulo","Test"]]"#, true);
        let rules_sig = score_rules(&item, &TickerHistory::default()).unwrap();
        let cfg = default_cfg(); // model is empty, enabled=false
        let result = score_lmstudio(&item, &rules_sig, &cfg);
        assert!(result.is_none());
    }

    #[test]
    fn score_lmstudio_returns_none_on_network_failure() {
        // model is set but server is unreachable — should return None
        let item = make_item("comunicados", r#"[["titulo","Test"]]"#, true);
        let rules_sig = score_rules(&item, &TickerHistory::default()).unwrap();

        let mut cfg = default_cfg();
        cfg.lmstudio.enabled = true;
        cfg.lmstudio.model = "test-model".to_string();
        cfg.lmstudio.base_url = "http://127.0.0.1:1".to_string(); // unreachable

        let result = score_lmstudio(&item, &rules_sig, &cfg);
        assert!(result.is_none());
    }
}
