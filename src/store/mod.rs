use rusqlite::{Connection, OptionalExtension, params};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MIGRATIONS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at INTEGER NOT NULL,
        ended_at INTEGER,
        status TEXT NOT NULL,
        sections_csv TEXT NOT NULL,
        error_message TEXT
    )",
    "CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        section TEXT NOT NULL,
        url TEXT,
        external_id TEXT,
        stable_key TEXT NOT NULL UNIQUE,
        title TEXT,
        published_at INTEGER,
        issuer TEXT,
        raw_hash TEXT,
        content_hash TEXT,
        normalized_json TEXT,
        first_seen_at INTEGER NOT NULL,
        last_seen_at INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS item_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        run_id INTEGER NOT NULL,
        content_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        UNIQUE(item_id, content_hash)
    )",
    "CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        run_id INTEGER NOT NULL,
        kind TEXT NOT NULL,
        severity TEXT NOT NULL,
        confidence REAL NOT NULL,
        reasons_json TEXT,
        summary TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS recipes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        section TEXT NOT NULL,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        confidence REAL,
        selector_json TEXT NOT NULL,
        diagnostics_json TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS item_key_aliases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alias_key TEXT NOT NULL UNIQUE,
        stable_key TEXT NOT NULL,
        item_id INTEGER,
        created_at INTEGER NOT NULL
    )",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    Success,
    Failed,
}

impl RunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug)]
pub struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
pub enum LockError {
    Contended(String),
    Runtime(String),
}

pub struct Store {
    conn: Connection,
}

pub struct RetentionPlan {
    pub run_ids_to_delete: Vec<i64>,
    pub version_ids_to_delete: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct ItemVersionForAnalysis {
    pub item_id: i64,
    pub section: String,
    pub stable_key: String,
    pub payload_json: String,
    pub version_count: i64, // 1 = new item, >1 = changed item
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SignalRow {
    pub id: i64,
    pub item_id: i64,
    pub run_id: i64,
    pub kind: String,
    pub severity: String,
    pub confidence: f64,
    pub reasons_json: Option<String>,
    pub summary: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipeStatus {
    PendingReview,
    Active,
    Degraded,
    Retired,
}

impl RecipeStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PendingReview => "pending_review",
            Self::Active => "active",
            Self::Degraded => "degraded",
            Self::Retired => "retired",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending_review" => Some(Self::PendingReview),
            "active" => Some(Self::Active),
            "degraded" => Some(Self::Degraded),
            "retired" => Some(Self::Retired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RecipeRow {
    pub id: i64,
    pub section: String,
    pub name: String,
    pub status: RecipeStatus,
    pub confidence: Option<f64>,
    pub selector_json: String,
    pub diagnostics_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Store {
    pub fn open(db_path: &Path) -> Result<Self, String> {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create {}: {err}", parent.display()))?;
        }
        let conn = Connection::open(db_path)
            .map_err(|err| format!("failed to open sqlite db {}: {err}", db_path.display()))?;
        Ok(Self { conn })
    }

    pub fn migrate(&self) -> Result<(), String> {
        self.conn
            .execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(|err| format!("failed to enable sqlite foreign keys: {err}"))?;

        self.conn
            .execute_batch(MIGRATIONS[0])
            .map_err(|err| format!("failed applying migration 1: {err}"))?;
        self.conn
            .execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (1)",
                [],
            )
            .map_err(|err| format!("failed recording migration 1: {err}"))?;

        for (idx, migration) in MIGRATIONS.iter().enumerate().skip(1) {
            let version = (idx as i64) + 1;
            let already_applied = self
                .conn
                .query_row(
                    "SELECT 1 FROM schema_migrations WHERE version = ?1",
                    params![version],
                    |_| Ok(true),
                )
                .optional()
                .map_err(|err| format!("failed reading migration metadata: {err}"))?
                .unwrap_or(false);
            if already_applied {
                continue;
            }
            self.conn
                .execute_batch(migration)
                .map_err(|err| format!("failed applying migration {version}: {err}"))?;
            self.conn
                .execute(
                    "INSERT INTO schema_migrations(version) VALUES (?1)",
                    params![version],
                )
                .map_err(|err| format!("failed recording migration {version}: {err}"))?;
        }

        Ok(())
    }

    pub fn start_run(&self, sections_csv: &str, started_at_epoch: i64) -> Result<i64, String> {
        self.conn
            .execute(
                "INSERT INTO runs(started_at, status, sections_csv) VALUES (?1, ?2, ?3)",
                params![started_at_epoch, RunStatus::Running.as_str(), sections_csv],
            )
            .map_err(|err| format!("failed to insert run row: {err}"))?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn finish_run(
        &self,
        run_id: i64,
        status: RunStatus,
        ended_at_epoch: i64,
        error_message: Option<&str>,
    ) -> Result<(), String> {
        self.conn
            .execute(
                "UPDATE runs SET ended_at = ?1, status = ?2, error_message = ?3 WHERE id = ?4",
                params![ended_at_epoch, status.as_str(), error_message, run_id],
            )
            .map_err(|err| format!("failed to finalize run {run_id}: {err}"))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn run_row(&self, run_id: i64) -> Result<Option<(String, Option<i64>)>, String> {
        self.conn
            .query_row(
                "SELECT status, ended_at FROM runs WHERE id = ?1",
                params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|err| format!("failed loading run row {run_id}: {err}"))
    }

    pub fn build_retention_plan(
        &self,
        now_epoch: i64,
        keep_runs_days: u32,
        keep_versions_per_item: u32,
    ) -> Result<RetentionPlan, String> {
        let run_cutoff = now_epoch - i64::from(keep_runs_days) * 24 * 60 * 60;
        let mut run_stmt = self
            .conn
            .prepare("SELECT id FROM runs WHERE ended_at IS NOT NULL AND ended_at < ?1")
            .map_err(|err| format!("failed preparing run retention query: {err}"))?;
        let run_rows = run_stmt
            .query_map(params![run_cutoff], |row| row.get::<_, i64>(0))
            .map_err(|err| format!("failed querying run retention candidates: {err}"))?;
        let mut run_ids_to_delete = Vec::new();
        for row in run_rows {
            run_ids_to_delete
                .push(row.map_err(|err| format!("failed reading run retention row: {err}"))?);
        }

        let mut version_ids_to_delete = Vec::new();
        let mut item_stmt = self
            .conn
            .prepare("SELECT DISTINCT item_id FROM item_versions")
            .map_err(|err| format!("failed preparing item versions query: {err}"))?;
        let item_rows = item_stmt
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(|err| format!("failed listing item ids for retention: {err}"))?;
        for item_row in item_rows {
            let item_id =
                item_row.map_err(|err| format!("failed reading item_id for retention: {err}"))?;
            let mut v_stmt = self
                .conn
                .prepare(
                    "SELECT id FROM item_versions WHERE item_id = ?1 ORDER BY created_at DESC, id DESC",
                )
                .map_err(|err| format!("failed preparing item version query: {err}"))?;
            let v_rows = v_stmt
                .query_map(params![item_id], |row| row.get::<_, i64>(0))
                .map_err(|err| format!("failed listing versions for item {item_id}: {err}"))?;

            for (idx, v_row) in v_rows.enumerate() {
                let version_id = v_row.map_err(|err| {
                    format!("failed reading version row for item {item_id}: {err}")
                })?;
                if idx >= keep_versions_per_item as usize {
                    version_ids_to_delete.push(version_id);
                }
            }
        }

        Ok(RetentionPlan {
            run_ids_to_delete,
            version_ids_to_delete,
        })
    }

    pub fn insert_recipe(
        &self,
        section: &str,
        name: &str,
        confidence: Option<f64>,
        selector_json: &str,
        diagnostics_json: Option<&str>,
        now_epoch: i64,
    ) -> Result<i64, String> {
        self.conn
            .execute(
                "INSERT INTO recipes(section, name, status, confidence, selector_json, diagnostics_json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
                params![
                    section,
                    name,
                    RecipeStatus::PendingReview.as_str(),
                    confidence,
                    selector_json,
                    diagnostics_json,
                    now_epoch,
                ],
            )
            .map_err(|err| format!("failed to insert recipe: {err}"))?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn list_recipes(&self, section: Option<&str>) -> Result<Vec<RecipeRow>, String> {
        let mut rows = Vec::new();
        if let Some(section) = section {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT id, section, name, status, confidence, selector_json, diagnostics_json, created_at, updated_at
                     FROM recipes WHERE section = ?1 ORDER BY updated_at DESC",
                )
                .map_err(|err| format!("failed preparing recipe list query: {err}"))?;
            let mapped = stmt
                .query_map(params![section], map_recipe_row)
                .map_err(|err| format!("failed listing recipes: {err}"))?;
            for row in mapped {
                rows.push(row.map_err(|err| format!("failed reading recipe row: {err}"))?);
            }
        } else {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT id, section, name, status, confidence, selector_json, diagnostics_json, created_at, updated_at
                     FROM recipes ORDER BY section, updated_at DESC",
                )
                .map_err(|err| format!("failed preparing recipe list query: {err}"))?;
            let mapped = stmt
                .query_map([], map_recipe_row)
                .map_err(|err| format!("failed listing recipes: {err}"))?;
            for row in mapped {
                rows.push(row.map_err(|err| format!("failed reading recipe row: {err}"))?);
            }
        }
        Ok(rows)
    }

    pub fn get_active_recipe(&self, section: &str) -> Result<Option<RecipeRow>, String> {
        self.conn
            .query_row(
                "SELECT id, section, name, status, confidence, selector_json, diagnostics_json, created_at, updated_at
                 FROM recipes WHERE section = ?1 AND status = ?2",
                params![section, RecipeStatus::Active.as_str()],
                map_recipe_row,
            )
            .optional()
            .map_err(|err| format!("failed querying active recipe for {section}: {err}"))
    }

    pub fn promote_recipe(&self, recipe_id: i64, now_epoch: i64) -> Result<RecipeRow, String> {
        let recipe: RecipeRow = self
            .conn
            .query_row(
                "SELECT id, section, name, status, confidence, selector_json, diagnostics_json, created_at, updated_at
                 FROM recipes WHERE id = ?1",
                params![recipe_id],
                map_recipe_row,
            )
            .optional()
            .map_err(|err| format!("failed loading recipe {recipe_id}: {err}"))?
            .ok_or_else(|| format!("recipe {recipe_id} not found"))?;

        // Demote current active recipe for this section
        self.conn
            .execute(
                "UPDATE recipes SET status = ?1, updated_at = ?2 WHERE section = ?3 AND status = ?4",
                params![
                    RecipeStatus::Retired.as_str(),
                    now_epoch,
                    recipe.section,
                    RecipeStatus::Active.as_str(),
                ],
            )
            .map_err(|err| format!("failed demoting active recipe: {err}"))?;

        self.conn
            .execute(
                "UPDATE recipes SET status = ?1, updated_at = ?2 WHERE id = ?3",
                params![RecipeStatus::Active.as_str(), now_epoch, recipe_id],
            )
            .map_err(|err| format!("failed promoting recipe {recipe_id}: {err}"))?;

        self.conn
            .query_row(
                "SELECT id, section, name, status, confidence, selector_json, diagnostics_json, created_at, updated_at
                 FROM recipes WHERE id = ?1",
                params![recipe_id],
                map_recipe_row,
            )
            .optional()
            .map_err(|err| format!("failed reloading promoted recipe: {err}"))?
            .ok_or_else(|| format!("recipe {recipe_id} not found after promote"))
    }

    /// Insert or update an item. Returns (item_id, is_new).
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_item(
        &self,
        source: &str,
        section: &str,
        url: Option<&str>,
        external_id: Option<&str>,
        stable_key: &str,
        title: Option<&str>,
        raw_hash: Option<&str>,
        content_hash: &str,
        normalized_json: &str,
        now_epoch: i64,
    ) -> Result<(i64, bool), String> {
        let existing: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM items WHERE stable_key = ?1",
                params![stable_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| format!("failed querying item {stable_key}: {err}"))?;

        if let Some(id) = existing {
            self.conn
                .execute(
                    "UPDATE items SET last_seen_at = ?1, content_hash = ?2, normalized_json = ?3, raw_hash = ?4 WHERE id = ?5",
                    params![now_epoch, content_hash, normalized_json, raw_hash, id],
                )
                .map_err(|err| format!("failed updating item {id}: {err}"))?;
            Ok((id, false))
        } else {
            self.conn
                .execute(
                    "INSERT INTO items(source, section, url, external_id, stable_key, title, raw_hash, content_hash, normalized_json, first_seen_at, last_seen_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
                    params![
                        source, section, url, external_id, stable_key,
                        title, raw_hash, content_hash, normalized_json, now_epoch
                    ],
                )
                .map_err(|err| format!("failed inserting item {stable_key}: {err}"))?;
            Ok((self.conn.last_insert_rowid(), true))
        }
    }

    /// Insert an item_version if this content_hash is new for this item. Returns true if inserted.
    pub fn insert_item_version_on_change(
        &self,
        item_id: i64,
        run_id: i64,
        content_hash: &str,
        payload_json: &str,
        now_epoch: i64,
    ) -> Result<bool, String> {
        let changed = self
            .conn
            .execute(
                "INSERT OR IGNORE INTO item_versions(item_id, run_id, content_hash, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![item_id, run_id, content_hash, payload_json, now_epoch],
            )
            .map_err(|err| {
                format!("failed inserting item version for item {item_id}: {err}")
            })?;
        Ok(changed > 0)
    }

    /// Execute a retention plan, deleting old runs and excess item_versions.
    pub fn apply_retention(&self, plan: &RetentionPlan) -> Result<(), String> {
        for &run_id in &plan.run_ids_to_delete {
            self.conn
                .execute("DELETE FROM runs WHERE id = ?1", params![run_id])
                .map_err(|err| format!("failed deleting run {run_id}: {err}"))?;
        }
        for &version_id in &plan.version_ids_to_delete {
            self.conn
                .execute(
                    "DELETE FROM item_versions WHERE id = ?1",
                    params![version_id],
                )
                .map_err(|err| format!("failed deleting item_version {version_id}: {err}"))?;
        }
        Ok(())
    }

    /// Return all item versions inserted in this run, with how many total versions each item has.
    pub fn items_for_analysis(
        &self,
        run_id: i64,
    ) -> Result<Vec<ItemVersionForAnalysis>, String> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT i.id, i.section, i.stable_key, iv.payload_json,
                        (SELECT COUNT(*) FROM item_versions WHERE item_id = i.id) AS version_count
                 FROM item_versions iv
                 JOIN items i ON i.id = iv.item_id
                 WHERE iv.run_id = ?1",
            )
            .map_err(|err| format!("failed preparing items_for_analysis query: {err}"))?;

        let rows = stmt
            .query_map(params![run_id], |row| {
                Ok(ItemVersionForAnalysis {
                    item_id: row.get(0)?,
                    section: row.get(1)?,
                    stable_key: row.get(2)?,
                    payload_json: row.get(3)?,
                    version_count: row.get(4)?,
                })
            })
            .map_err(|err| format!("failed querying items_for_analysis: {err}"))?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(|err| format!("failed reading analysis row: {err}"))?);
        }
        Ok(result)
    }

    /// Insert a signal for an item version.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_signal(
        &self,
        item_id: i64,
        run_id: i64,
        kind: &str,
        severity: &str,
        confidence: f64,
        reasons: &[String],
        summary: &str,
        now: i64,
    ) -> Result<(), String> {
        let reasons_json = serde_json::to_string(reasons)
            .map_err(|err| format!("failed serializing signal reasons: {err}"))?;
        self.conn
            .execute(
                "INSERT INTO signals(item_id, run_id, kind, severity, confidence, reasons_json, summary, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![item_id, run_id, kind, severity, confidence, reasons_json, summary, now],
            )
            .map_err(|err| format!("failed inserting signal: {err}"))?;
        Ok(())
    }

    /// Return all signals for a given run. Used by Phase 8 `report latest`.
    #[allow(dead_code)]
    pub fn signals_for_run(&self, run_id: i64) -> Result<Vec<SignalRow>, String> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, item_id, run_id, kind, severity, confidence, reasons_json, summary, created_at
                 FROM signals WHERE run_id = ?1 ORDER BY id",
            )
            .map_err(|err| format!("failed preparing signals_for_run query: {err}"))?;

        let rows = stmt
            .query_map(params![run_id], |row| {
                Ok(SignalRow {
                    id: row.get(0)?,
                    item_id: row.get(1)?,
                    run_id: row.get(2)?,
                    kind: row.get(3)?,
                    severity: row.get(4)?,
                    confidence: row.get(5)?,
                    reasons_json: row.get(6)?,
                    summary: row.get(7)?,
                    created_at: row.get(8)?,
                })
            })
            .map_err(|err| format!("failed querying signals_for_run: {err}"))?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(|err| format!("failed reading signal row: {err}"))?);
        }
        Ok(result)
    }

    /// Return the id of the most recently started run, or None if no runs exist.
    /// Used by Phase 8 `report latest`.
    #[allow(dead_code)]
    pub fn latest_run_id(&self) -> Result<Option<i64>, String> {
        self.conn
            .query_row(
                "SELECT id FROM runs ORDER BY started_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| format!("failed querying latest run_id: {err}"))
    }

    pub fn update_recipe_status(
        &self,
        recipe_id: i64,
        status: RecipeStatus,
        now_epoch: i64,
    ) -> Result<(), String> {
        let changed = self
            .conn
            .execute(
                "UPDATE recipes SET status = ?1, updated_at = ?2 WHERE id = ?3",
                params![status.as_str(), now_epoch, recipe_id],
            )
            .map_err(|err| format!("failed updating recipe {recipe_id} status: {err}"))?;
        if changed == 0 {
            return Err(format!("recipe {recipe_id} not found"));
        }
        Ok(())
    }
}

fn map_recipe_row(row: &rusqlite::Row) -> rusqlite::Result<RecipeRow> {
    let status_str: String = row.get(3)?;
    let status = RecipeStatus::parse(&status_str).unwrap_or(RecipeStatus::PendingReview);
    Ok(RecipeRow {
        id: row.get(0)?,
        section: row.get(1)?,
        name: row.get(2)?,
        status,
        confidence: row.get(4)?,
        selector_json: row.get(5)?,
        diagnostics_json: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

pub fn acquire_collect_lock(lock_path: &Path) -> Result<LockGuard, LockError> {
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            LockError::Runtime(format!("failed to create {}: {err}", parent.display()))
        })?;
    }

    let pid = std::process::id();
    let now = now_epoch_seconds().map_err(LockError::Runtime)?;
    let body = format!("pid={pid}\ncreated_at={now}\n");

    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(lock_path)
    {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(body.as_bytes()).map_err(|err| {
                LockError::Runtime(format!(
                    "failed writing lock {}: {err}",
                    lock_path.display()
                ))
            })?;
            Ok(LockGuard {
                path: lock_path.to_path_buf(),
            })
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let message = stale_or_contended_message(lock_path);
            Err(LockError::Contended(message))
        }
        Err(err) => Err(LockError::Runtime(format!(
            "failed creating lock {}: {err}",
            lock_path.display()
        ))),
    }
}

fn stale_or_contended_message(lock_path: &Path) -> String {
    let text = match fs::read_to_string(lock_path) {
        Ok(v) => v,
        Err(_) => {
            return format!(
                "collect lock is already held at {}; another run may be active",
                lock_path.display()
            );
        }
    };

    let mut locked_pid = None::<u32>;
    for line in text.lines() {
        let mut parts = line.splitn(2, '=');
        if parts.next().unwrap_or("").trim() == "pid" {
            locked_pid = parts.next().unwrap_or("").trim().parse::<u32>().ok();
            break;
        }
    }

    if let Some(pid) = locked_pid {
        if !PathBuf::from(format!("/proc/{pid}")).exists() {
            return format!(
                "collect lock exists at {} but pid {} is not alive; remove stale lock to continue",
                lock_path.display(),
                pid
            );
        }
        return format!(
            "collect lock is already held by pid {} at {}",
            pid,
            lock_path.display()
        );
    }

    format!(
        "collect lock is already held at {}; another run may be active",
        lock_path.display()
    )
}

/// FNV-1a 64-bit hash — deterministic across runs, no external dependency.
pub fn content_hash_fnv(data: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub fn now_epoch_seconds() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock appears before UNIX epoch: {err}"))?;
    i64::try_from(duration.as_secs())
        .map_err(|_| "current time does not fit in i64 epoch seconds".to_string())
}

#[cfg(test)]
mod tests {
    use super::{LockError, RecipeStatus, RunStatus, Store, acquire_collect_lock};
    use rusqlite::params;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::current_dir()
            .expect("cwd should exist")
            .join(".test-tmp")
            .join(format!("store-{name}-{nanos}"))
    }

    #[test]
    fn migrations_are_idempotent() {
        let root = unique_temp_dir("migrate");
        fs::create_dir_all(&root).expect("root should exist");
        let db = root.join("omens.db");
        let store = Store::open(&db).expect("store should open");
        store.migrate().expect("first migration should work");
        store.migrate().expect("second migration should work");
    }

    #[test]
    fn lock_contention_returns_error() {
        let root = unique_temp_dir("lock");
        fs::create_dir_all(&root).expect("root should exist");
        let lock = root.join("collect.lock");
        let _guard = acquire_collect_lock(&lock).expect("first lock should succeed");
        let second = acquire_collect_lock(&lock).expect_err("second lock should fail");
        match second {
            LockError::Contended(msg) => assert!(msg.contains("collect lock")),
            LockError::Runtime(msg) => panic!("expected contention, got runtime error: {msg}"),
        }
    }

    #[test]
    fn run_status_transitions_persist() {
        let root = unique_temp_dir("runs");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let run_id = store
            .start_run("news", 10)
            .expect("run row should be inserted");
        store
            .finish_run(run_id, RunStatus::Success, 20, None)
            .expect("run should be finalized");

        let row = store
            .run_row(run_id)
            .expect("run row should load")
            .expect("run row should exist");
        assert_eq!(row.0, "success");
        assert_eq!(row.1, Some(20));
    }

    #[test]
    fn retention_plan_keeps_latest_versions_per_item() {
        let root = unique_temp_dir("retention");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        store
            .conn
            .execute(
                "INSERT INTO item_versions(item_id, run_id, content_hash, payload_json, created_at)
                 VALUES (?1, 1, ?2, '{}', ?3)",
                params![1, "a", 100],
            )
            .expect("insert v1");
        store
            .conn
            .execute(
                "INSERT INTO item_versions(item_id, run_id, content_hash, payload_json, created_at)
                 VALUES (?1, 1, ?2, '{}', ?3)",
                params![1, "b", 101],
            )
            .expect("insert v2");
        store
            .conn
            .execute(
                "INSERT INTO item_versions(item_id, run_id, content_hash, payload_json, created_at)
                 VALUES (?1, 1, ?2, '{}', ?3)",
                params![1, "c", 102],
            )
            .expect("insert v3");

        let plan = store
            .build_retention_plan(200, 180, 2)
            .expect("retention plan should compute");
        assert!(plan.run_ids_to_delete.is_empty());
        assert_eq!(plan.version_ids_to_delete.len(), 1);
    }

    #[test]
    fn recipe_insert_and_list() {
        let root = unique_temp_dir("recipe-insert");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let id = store
            .insert_recipe(
                "news",
                "news-v1",
                Some(0.85),
                r#"{"listing":".item"}"#,
                None,
                100,
            )
            .expect("insert should work");
        assert!(id > 0);

        let all = store.list_recipes(None).expect("list should work");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].section, "news");
        assert_eq!(all[0].name, "news-v1");
        assert_eq!(all[0].status, RecipeStatus::PendingReview);
        assert_eq!(all[0].confidence, Some(0.85));

        let by_section = store
            .list_recipes(Some("news"))
            .expect("section list should work");
        assert_eq!(by_section.len(), 1);

        let empty = store
            .list_recipes(Some("material-facts"))
            .expect("empty section should work");
        assert!(empty.is_empty());
    }

    #[test]
    fn promote_recipe_demotes_previous_active() {
        let root = unique_temp_dir("recipe-promote");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let r1 = store
            .insert_recipe("news", "v1", None, "{}", None, 100)
            .expect("insert r1");
        let r2 = store
            .insert_recipe("news", "v2", None, "{}", None, 200)
            .expect("insert r2");

        let promoted = store.promote_recipe(r1, 300).expect("promote r1");
        assert_eq!(promoted.status, RecipeStatus::Active);

        let active = store
            .get_active_recipe("news")
            .expect("query active")
            .expect("should have active");
        assert_eq!(active.id, r1);

        // Promoting r2 should demote r1
        let promoted2 = store.promote_recipe(r2, 400).expect("promote r2");
        assert_eq!(promoted2.status, RecipeStatus::Active);

        let recipes = store.list_recipes(Some("news")).expect("list");
        let r1_row = recipes
            .iter()
            .find(|r| r.id == r1)
            .expect("r1 should exist");
        assert_eq!(r1_row.status, RecipeStatus::Retired);
    }

    #[test]
    fn upsert_item_creates_then_updates() {
        let root = unique_temp_dir("upsert-item");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let (id1, is_new1) = store
            .upsert_item(
                "clubefii",
                "proventos",
                Some("https://example.com#proventos"),
                Some("BRCR11/proventos/2024-01"),
                "external_id:BRCR11/proventos/2024-01",
                Some("2024-01"),
                None,
                "deadbeef00000001",
                r#"[["mes","2024-01"],["valor","R$1.00"]]"#,
                1000,
            )
            .expect("upsert should succeed");
        assert!(id1 > 0);
        assert!(is_new1);

        // Same stable_key → update
        let (id2, is_new2) = store
            .upsert_item(
                "clubefii",
                "proventos",
                Some("https://example.com#proventos"),
                Some("BRCR11/proventos/2024-01"),
                "external_id:BRCR11/proventos/2024-01",
                Some("2024-01"),
                None,
                "deadbeef00000002",
                r#"[["mes","2024-01"],["valor","R$1.05"]]"#,
                2000,
            )
            .expect("second upsert should succeed");
        assert_eq!(id1, id2);
        assert!(!is_new2);
    }

    #[test]
    fn item_version_on_change_inserts_once_per_hash() {
        let root = unique_temp_dir("version-change");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let run_id = store.start_run("proventos", 100).expect("run");
        let (item_id, _) = store
            .upsert_item(
                "clubefii",
                "proventos",
                None,
                None,
                "external_id:T/s/k",
                None,
                None,
                "hash1",
                "{}",
                100,
            )
            .expect("upsert");

        let inserted1 = store
            .insert_item_version_on_change(item_id, run_id, "hash1", "{}", 100)
            .expect("version insert 1");
        assert!(inserted1);

        // Same hash → no duplicate (OR IGNORE)
        let inserted2 = store
            .insert_item_version_on_change(item_id, run_id, "hash1", "{}", 200)
            .expect("version insert 2 same hash");
        assert!(!inserted2);

        // Different hash → new version
        let inserted3 = store
            .insert_item_version_on_change(item_id, run_id, "hash2", "{\"v\":2}", 300)
            .expect("version insert 3 new hash");
        assert!(inserted3);
    }

    #[test]
    fn apply_retention_deletes_planned_entries() {
        let root = unique_temp_dir("apply-retention");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let run_id = store.start_run("test", 10).expect("run");
        store
            .finish_run(run_id, super::RunStatus::Success, 20, None)
            .expect("finish");

        let plan = store
            .build_retention_plan(20 + 180 * 24 * 3600 + 1, 180, 20)
            .expect("plan");
        assert_eq!(plan.run_ids_to_delete.len(), 1);

        store.apply_retention(&plan).expect("apply");

        let remaining = store.run_row(run_id).expect("query");
        assert!(remaining.is_none());
    }

    #[test]
    fn items_for_analysis_returns_version_count() {
        let root = unique_temp_dir("items-for-analysis");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let run1 = store.start_run("proventos", 100).expect("run1");
        let run2 = store.start_run("proventos", 200).expect("run2");

        let (item_id, _) = store
            .upsert_item("clubefii", "proventos", None, None, "key:A", None, None, "h1", "{}", 100)
            .expect("upsert item");

        // Insert first version in run1
        store
            .insert_item_version_on_change(item_id, run1, "h1", r#"[["v","1"]]"#, 100)
            .expect("version 1");
        // Insert second version in run2 (changed)
        store
            .insert_item_version_on_change(item_id, run2, "h2", r#"[["v","2"]]"#, 200)
            .expect("version 2");

        // items_for_analysis for run2 should return the item with version_count=2
        let rows = store.items_for_analysis(run2).expect("analysis rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].item_id, item_id);
        assert_eq!(rows[0].version_count, 2);
        assert_eq!(rows[0].section, "proventos");

        // items_for_analysis for run1 should show version_count=2 (total across all runs)
        let rows1 = store.items_for_analysis(run1).expect("analysis rows run1");
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0].version_count, 2);
    }

    #[test]
    fn insert_signal_and_signals_for_run_roundtrip() {
        let root = unique_temp_dir("signals-roundtrip");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let run_id = store.start_run("proventos", 100).expect("run");

        store
            .insert_signal(
                42,
                run_id,
                "dividend",
                "high",
                0.9,
                &["dividend amount revised".to_string()],
                "dividend changed: key:A",
                100,
            )
            .expect("insert signal");

        let sigs = store.signals_for_run(run_id).expect("signals_for_run");
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].item_id, 42);
        assert_eq!(sigs[0].run_id, run_id);
        assert_eq!(sigs[0].kind, "dividend");
        assert_eq!(sigs[0].severity, "high");
        assert!((sigs[0].confidence - 0.9).abs() < 1e-9);
        assert_eq!(sigs[0].summary, "dividend changed: key:A");
        assert!(sigs[0].reasons_json.as_deref().unwrap_or("").contains("revised"));
    }

    #[test]
    fn signals_for_run_returns_empty_for_unknown_run() {
        let root = unique_temp_dir("signals-empty");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let sigs = store.signals_for_run(9999).expect("signals_for_run");
        assert!(sigs.is_empty());
    }

    #[test]
    fn latest_run_id_returns_most_recent() {
        let root = unique_temp_dir("latest-run");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        // No runs yet
        let none = store.latest_run_id().expect("latest_run_id");
        assert!(none.is_none());

        let r1 = store.start_run("a", 100).expect("run1");
        let r2 = store.start_run("b", 200).expect("run2");
        let r3 = store.start_run("c", 50).expect("run3"); // started_at lower than r2

        let latest = store.latest_run_id().expect("latest_run_id").expect("should have a run");
        // r2 has started_at=200, which is highest
        assert_eq!(latest, r2);
        let _ = (r1, r3); // suppress unused warnings
    }

    #[test]
    fn content_hash_fnv_is_deterministic() {
        let h1 = super::content_hash_fnv("hello world");
        let h2 = super::content_hash_fnv("hello world");
        let h3 = super::content_hash_fnv("hello world!");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
        assert_eq!(h1.len(), 16);
    }

    #[test]
    fn update_recipe_status_to_degraded() {
        let root = unique_temp_dir("recipe-degrade");
        fs::create_dir_all(&root).expect("root should exist");
        let store = Store::open(&root.join("omens.db")).expect("store should open");
        store.migrate().expect("migrations should work");

        let id = store
            .insert_recipe("news", "v1", None, "{}", None, 100)
            .expect("insert");
        store.promote_recipe(id, 200).expect("promote");
        store
            .update_recipe_status(id, RecipeStatus::Degraded, 300)
            .expect("degrade");

        let recipes = store.list_recipes(Some("news")).expect("list");
        assert_eq!(recipes[0].status, RecipeStatus::Degraded);
    }
}
