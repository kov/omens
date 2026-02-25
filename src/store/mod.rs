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
        section TEXT NOT NULL,
        canonical_key TEXT NOT NULL UNIQUE,
        title TEXT,
        url TEXT,
        published_at INTEGER,
        first_seen_run_id INTEGER,
        last_seen_run_id INTEGER
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
        severity INTEGER NOT NULL,
        confidence REAL NOT NULL,
        summary TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS recipes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        section TEXT NOT NULL,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        selector_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS item_key_aliases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alias_key TEXT NOT NULL UNIQUE,
        canonical_key TEXT NOT NULL,
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

            let mut idx = 0u32;
            for v_row in v_rows {
                let version_id = v_row.map_err(|err| {
                    format!("failed reading version row for item {item_id}: {err}")
                })?;
                if idx >= keep_versions_per_item {
                    version_ids_to_delete.push(version_id);
                }
                idx += 1;
            }
        }

        Ok(RetentionPlan {
            run_ids_to_delete,
            version_ids_to_delete,
        })
    }
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

pub fn now_epoch_seconds() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("system clock appears before UNIX epoch: {err}"))?;
    i64::try_from(duration.as_secs())
        .map_err(|_| "current time does not fit in i64 epoch seconds".to_string())
}

#[cfg(test)]
mod tests {
    use super::{LockError, RunStatus, Store, acquire_collect_lock};
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
}
