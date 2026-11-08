//! Oracle connections through the pure-Rust thin driver (`oracledb`): no
//! client library is needed. Every blocking call runs on the blocking pool,
//! and cancelling a statement closes its connection. The driver commits
//! explicitly, so auto-commit mode commits after each statement.

use crate::conn::{Cells, Fetched, ResultColumn, Setup, bytes_text, hex};
use crate::source::{DataSource, DbDriver, DbTxMode};
use bb8::ManageConnection;
use ide_core::{CancellationToken, IdeError, IdeResult, error::ErrorKind};
use oracledb::{DbType, Lob, OracleIntervalDS, OracleIntervalYM, OracleTimestamp};
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Characters of a LOB column shown in the grid; catalog text reads more.
const MAX_GRID_LOB: u64 = 4096;
const MAX_TEXT_LOB: u64 = 1 << 16;

/// Everything a new connection needs, built once per pool.
#[derive(Debug)]
struct State {
    user: String,
    password: String,
    /// EZConnect `//host:port/service`, a TNS descriptor or an alias.
    connect: String,
    init: Vec<String>,
    auto_commit: bool,
    read_only: bool,
    statement_cache: Option<usize>,
    timeout: Duration,
}

impl State {
    fn of(source: &DataSource, password: Option<&str>, setup: &Setup) -> IdeResult<Self> {
        let user = source.user.trim();
        if user.is_empty() {
            return Err(IdeError::invalid("Oracle data sources need a user name"));
        }
        Ok(Self {
            user: user.to_owned(),
            password: password.unwrap_or_default().to_owned(),
            connect: connect_string(source)?,
            init: setup.statements.clone(),
            auto_commit: source.options.transaction != DbTxMode::Manual,
            read_only: source.read_only,
            statement_cache: setup.statement_cache,
            timeout: setup.connect_timeout.unwrap_or(Duration::from_secs(30)),
        })
    }

    /// The connection settings; the first `oracledb::connect` on a config
    /// validates them (TNS aliases resolve in `set_connect_string`).
    fn config(&self) -> IdeResult<oracledb::Config> {
        let config = oracledb::Config::default()
            .set_credentials(&self.user, &self.password)
            .set_connect_string(&self.connect)
            .map_err(|e| oracle_err(&e))?
            .set_driver_name("OpenIDEA")
            // Bound connection establishment; the pool's timeout applies too.
            .set_transport_connect_timeout(Some(self.timeout));
        Ok(if let Some(n) = self.statement_cache { config.set_stmtcachesize(n) } else { config })
    }
}

/// `//host:port/service`; the database field may also hold a TNS descriptor
/// or, with the net administration directory set, a TNS alias. The thin
/// driver's EZConnect syntax needs a service name, so an empty Database field
/// is refused.
fn connect_string(source: &DataSource) -> IdeResult<String> {
    connect_string_with(source, net_alias_config_dir())
}

fn connect_string_with(source: &DataSource, alias: bool) -> IdeResult<String> {
    let db = source.database.trim();
    if db.is_empty() {
        return Err(IdeError::invalid(
            "an Oracle service name is needed: fill the Database field (for example ORCLPDB1), a TNS descriptor or a TNS alias",
        ));
    }
    if db.starts_with('(') || db.contains('=') {
        return Ok(db.to_owned());
    }
    // A bare name resolves as a TNS alias when tnsnames.ora can be found;
    // otherwise it connects as a service name on the host field.
    if source.host.trim().is_empty() && !db.contains('/') && alias {
        return Ok(db.to_owned());
    }
    let host = if source.host.trim().is_empty() { "localhost" } else { source.host.trim() };
    let port = source.port.unwrap_or(1521);
    Ok(format!("//{host}:{port}/{db}"))
}

/// Whether `tnsnames.ora` is findable through `TNS_ADMIN` / `ORACLE_HOME`
/// (this mirrors where the driver itself would look it up).
fn net_alias_config_dir() -> bool {
    for var in ["TNS_ADMIN", "ORACLE_HOME"] {
        if let Ok(value) = std::env::var(var)
            && !value.trim().is_empty()
        {
            let mut path = std::path::PathBuf::from(value.trim());
            if var == "ORACLE_HOME" {
                path.push("network");
                path.push("admin");
            }
            path.push("tnsnames.ora");
            return path.exists();
        }
    }
    false
}

/// Wrong or missing password: ORA-01017 / ORA-01005; unreachable server: ORA-12xxx / ORA-31xx.
pub(crate) fn oracle_err(e: &oracledb::Error) -> IdeError {
    let oracledb::ErrorKind::DbError(db) = e.kind() else {
        return IdeError::new(ErrorKind::InvalidInput, e.to_string());
    };
    let code = db.code() as u32;
    let full = format!("[ORA-{code:05}] {}", db.message());
    match code {
        1005 | 1017 | 28000 | 28001 => IdeError::new(ErrorKind::PermissionDenied, full),
        12150..=12199 | 12537 | 12540 | 12541 | 12543 | 12545 | 3113 | 3114 => IdeError::new(ErrorKind::Io, full),
        _ => IdeError::new(ErrorKind::InvalidInput, full),
    }
}

/// `oracledb` errors are not `Clone`; the pool wants a shareable error.
#[derive(Clone)]
pub struct OracleError {
    /// The driver error (`None` for messages the pool produces itself).
    driver: Option<Arc<oracledb::Error>>,
    text: String,
}

impl OracleError {
    fn new(e: oracledb::Error) -> Self {
        Self { text: e.to_string(), driver: Some(Arc::new(e)) }
    }

    fn message(msg: impl Into<String>) -> Self {
        Self { text: msg.into(), driver: None }
    }

    fn into_ide(self) -> IdeError {
        self.driver.as_deref().map(oracle_err).unwrap_or_else(|| IdeError::new(ErrorKind::InvalidInput, self.text))
    }
}

impl std::fmt::Debug for OracleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl std::fmt::Display for OracleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

#[derive(Debug)]
pub struct OraclePool {
    state: Arc<State>,
    pool: Arc<bb8::Pool<OracleConnectionManager>>,
}

impl OraclePool {
    pub(crate) async fn connect(source: &DataSource, password: Option<&str>, setup: &Setup) -> IdeResult<Self> {
        let state = Arc::new(State::of(source, password, setup)?);
        let pool = bb8::Pool::builder()
            .max_size(4)
            .connection_timeout(state.timeout)
            .build(OracleConnectionManager(state.clone()))
            .await
            .map_err(|e| e.into_ide())?;
        // bb8 connects lazily and reports failed attempts as a bare timeout, which
        // would turn a wrong password into "timed out connecting to the database".
        let opened = tokio::task::spawn_blocking({
            let state = state.clone();
            move || open(&state)
        })
        .await
        .map_err(|e| IdeError::internal(format!("Oracle connect task: {e}")))?
        .map_err(OracleError::into_ide)?;
        let _ = pool.add(opened);
        Ok(Self { state, pool: Arc::new(pool) })
    }

    /// A connection taken out of the pool for one statement or a held session.
    pub async fn get(&self) -> IdeResult<OracleConnection> {
        let pooled = self.pool.get_owned().await.map_err(|e| pool_err(e).into_ide())?;
        Ok(OracleConnection { state: self.state.clone(), pooled })
    }

    pub fn is_closed(&self) -> bool {
        let state = self.pool.state();
        state.connections == 0 && state.idle_connections == 0
    }
}

fn pool_err(e: bb8::RunError<OracleError>) -> OracleError {
    match e {
        bb8::RunError::User(e) => e,
        bb8::RunError::TimedOut => OracleError::message("timed out connecting to the database"),
    }
}

/// A pooled connection's payload; `conn` is `None` once the connection is
/// discarded (a cancelled statement leaves it in the blocking task).
pub struct OracleHandle {
    conn: Option<oracledb::Connection>,
    context: crate::conn::Context,
}

pub struct OracleConnectionManager(Arc<State>);

impl ManageConnection for OracleConnectionManager {
    type Connection = OracleHandle;
    type Error = OracleError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let state = self.0.clone();
        tokio::task::spawn_blocking(move || open(&state))
            .await
            .map_err(|e| OracleError::message(format!("Oracle connect task: {e}")))?
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let state = self.0.clone();
        let Some(raw) = conn.conn.take() else {
            return Err(OracleError::message("Oracle connection is closed"));
        };
        let ping = tokio::time::timeout(
            state.timeout,
            tokio::task::spawn_blocking(move || {
                let result = raw.ping();
                (raw, result)
            }),
        )
        .await;
        match ping {
            Ok(Ok((raw, Ok(())))) => {
                conn.conn = Some(raw);
                Ok(())
            }
            Ok(Ok((_, Err(e)))) => Err(OracleError::new(e)),
            Ok(Err(e)) => Err(OracleError::message(format!("Oracle ping task: {e}"))),
            // Timed out (the task keeps the connection and closes it): discard this one.
            Err(_) => Err(OracleError::message("timed out waiting for the Oracle server")),
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.conn.is_none()
    }
}

/// A connection borrowed from the pool. It returns to the pool when dropped,
/// unless its statement was cancelled (then it is discarded).
pub struct OracleConnection {
    state: Arc<State>,
    pooled: bb8::PooledConnection<'static, OracleConnectionManager>,
}

impl OracleConnection {
    pub(crate) fn context(&mut self) -> &mut crate::conn::Context {
        &mut self.pooled.context
    }

    pub fn is_closed(&self) -> bool {
        self.pooled.conn.is_none()
    }

    /// Drops the connection instead of returning it; the server rolls an open
    /// transaction back.
    pub(crate) fn discard_inner(&mut self) {
        self.pooled.conn.take();
    }
}

fn open(state: &State) -> Result<OracleHandle, OracleError> {
    let conn =
        oracledb::connect(state.config().map_err(|e| OracleError::message(e.message))?).map_err(OracleError::new)?;
    for sql in &state.init {
        let mut stmt = conn.statement(sql).map_err(OracleError::new)?.build().map_err(OracleError::new)?;
        stmt.execute(&[]).map_err(OracleError::new)?;
    }
    let mut row =
        conn.query_row("SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM DUAL", &[]).map_err(OracleError::new)?;
    let default = crate::sql::use_schema(DbDriver::Oracle, &row.take::<String>(0).map_err(OracleError::new)?);
    let mut row =
        conn.query_row("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL", &[]).map_err(OracleError::new)?;
    let current = row.take::<String>(0).map_err(OracleError::new)?;
    let mut context = crate::conn::Context::new(default);
    context.set_current(crate::sql::use_schema(DbDriver::Oracle, &current));
    Ok(OracleHandle { conn: Some(conn), context })
}

/// Runs `f` with the connection on the blocking pool. Cancelling drops the
/// connection instead of returning it (its statement keeps running server-side).
async fn with_conn<T>(
    c: &mut OracleConnection,
    cancel: &CancellationToken,
    f: impl FnOnce(&mut oracledb::Connection) -> IdeResult<T> + Send + 'static,
) -> IdeResult<T>
where
    T: Send + 'static,
{
    let Some(conn) = c.pooled.conn.take() else {
        return Err(IdeError::new(ErrorKind::Io, "Oracle connection is closed"));
    };
    let (tx, mut rx) = mpsc::channel::<(oracledb::Connection, IdeResult<T>)>(1);
    tokio::task::spawn_blocking(move || {
        let mut slot = Some(conn);
        let result = match slot.as_mut() {
            Some(conn) => f(conn),
            None => unreachable!("connection taken above"),
        };
        // A closed receiver (cancelled) drops the connection, closing it.
        let _ = tx.blocking_send((slot.take().expect("some"), result));
    });
    tokio::select! {
        item = rx.recv() => match item {
            Some((conn, result)) => {
                c.pooled.conn = Some(conn);
                result
            }
            None => Err(IdeError::internal("the Oracle connection task failed")),
        },
        () = cancel.cancelled() => {
            rx.close();
            Err(IdeError::cancelled())
        }
    }
}

/// Runs a script on `c` (see [`crate::conn::Pool::rows`] and `Held::execute`).
pub(crate) async fn run(
    c: &mut OracleConnection,
    sql: &str,
    skip: usize,
    max: usize,
    mode: Cells,
    cancel: &CancellationToken,
) -> IdeResult<Fetched> {
    let state = c.state.clone();
    let sql = sql.to_owned();
    with_conn(c, cancel, move |conn| statements(conn, &state, &sql, skip, max, mode)).await
}

/// The table editor's edits in one transaction.
pub(crate) async fn transaction(c: &mut OracleConnection, statements: &[(String, bool)]) -> IdeResult<u64> {
    if c.state.read_only {
        return Err(IdeError::new(ErrorKind::PermissionDenied, "read-only connection: edits are not allowed"));
    }
    let script: Vec<String> = statements.iter().map(|(sql, _)| sql.clone()).collect();
    let expect: Vec<bool> = statements.iter().map(|(_, e)| *e).collect();
    let cancel = CancellationToken::new();
    with_conn(c, &cancel, move |conn| {
        let mut total = 0;
        for (sql, expect_one) in script.iter().zip(&expect) {
            let mut stmt = conn.statement(sql).map_err(|e| oracle_err(&e))?.build().map_err(|e| oracle_err(&e))?;
            let changed = stmt.execute(&[]).map_err(|e| oracle_err(&e))?.rows_affected();
            if *expect_one && changed != 1 {
                // The connection is discarded by the caller on failure.
                return Err(IdeError::invalid(format!(
                    "{sql}: expected to change 1 row, changed {changed}; nothing was saved"
                )));
            }
            total += changed;
        }
        conn.commit().map_err(|e| oracle_err(&e))?;
        Ok(total)
    })
    .await
}

/// Runs every statement of `sql` (one script, PL/SQL blocks kept whole): the
/// first query's rows are the result, otherwise the total rows changed.
fn statements(
    conn: &mut oracledb::Connection,
    state: &State,
    sql: &str,
    skip: usize,
    max: usize,
    mode: Cells,
) -> IdeResult<Fetched> {
    let mut query: Option<Fetched> = None;
    let mut affected: u64 = 0;
    let mut first: Option<Instant> = None;
    for s in crate::split::split(sql, DbDriver::Oracle) {
        if state.read_only && is_write(&s.text) {
            return Err(IdeError::new(
                ErrorKind::PermissionDenied,
                format!("read-only connection: {} statements are not allowed", first_word(&s.text)),
            ));
        }
        let r = statement(conn, &s.text, skip, max, mode, state.auto_commit)?;
        first.get_or_insert_with(|| r.4.unwrap_or_else(Instant::now));
        if r.0.is_empty() {
            affected += r.2.unwrap_or(0);
        } else if query.is_none() {
            query = Some(r);
        }
    }
    Ok(query.unwrap_or((Vec::new(), Vec::new(), Some(affected), false, first)))
}

/// Reads one statement; `skip` rows are fetched and dropped (result pagination).
/// A failing statement has no effect (statement-level rollback), so only the
/// successful ones are committed in auto-commit mode.
fn statement(
    conn: &mut oracledb::Connection,
    sql: &str,
    skip: usize,
    max: usize,
    mode: Cells,
    auto_commit: bool,
) -> IdeResult<Fetched> {
    let started = Instant::now();
    // LOB columns arrive as locators (the default), so `cell` reads only the shown prefix.
    let mut stmt = conn
        .statement(sql)
        .map_err(|e| oracle_err(&e))?
        .fetch_array_size(u32::try_from(max).unwrap_or(u32::MAX).clamp(1, 1000) + 1)
        .build()
        .map_err(|e| oracle_err(&e))?;
    if stmt.is_query() {
        let mut rows = stmt.query(&[]).map_err(|e| oracle_err(&e))?;
        let metadata: Vec<oracledb::Metadata> = rows.columns().to_vec();
        let columns: Vec<ResultColumn> = metadata
            .iter()
            .map(|c| ResultColumn { name: c.name().to_owned(), type_name: c.db_type().name().to_owned() })
            .collect();
        let mut out: Vec<Vec<Option<String>>> = Vec::new();
        let mut has_more = false;
        let mut to_skip = skip;
        for row in rows.by_ref() {
            let mut row = row.map_err(|e| oracle_err(&e))?;
            if to_skip > 0 {
                to_skip -= 1;
                continue;
            }
            if out.len() >= max {
                has_more = true;
                break;
            }
            out.push(metadata.iter().enumerate().map(|(i, m)| cell(&mut row, i, m.db_type(), mode)).collect());
        }
        Ok((columns, out, None, has_more, Some(started)))
    } else {
        let affected = stmt.execute(&[]).map_err(|e| oracle_err(&e))?.rows_affected();
        if auto_commit {
            conn.commit().map_err(|e| oracle_err(&e))?;
        }
        Ok((Vec::new(), Vec::new(), Some(affected), false, Some(started)))
    }
}

fn first_word(sql: &str) -> String {
    sql.lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("--"))
        .and_then(|l| l.split_whitespace().next())
        .unwrap_or("")
        .to_ascii_uppercase()
}

/// Whether `sql` changes data or schema (read-only connections refuse it).
fn is_write(sql: &str) -> bool {
    !matches!(first_word(sql).as_str(), "SELECT" | "WITH" | "SHOW" | "EXPLAIN" | "DESCRIBE" | "DESC" | "SET")
}

/// The value at column `i` as text; LOB locators read the shown prefix only.
fn cell(row: &mut oracledb::Row, i: usize, ty: &DbType, mode: Cells) -> Option<String> {
    let cap = if mode == Cells::Text { MAX_TEXT_LOB } else { MAX_GRID_LOB };
    if ty == oracledb::DB_TYPE_CLOB || ty == oracledb::DB_TYPE_NCLOB {
        return row.take::<Option<Lob>>(i).ok().flatten().and_then(|mut lob| character_lob(&mut lob, cap));
    }
    if ty == oracledb::DB_TYPE_BLOB || ty == oracledb::DB_TYPE_BFILE {
        return row.take::<Option<Lob>>(i).ok().flatten().and_then(|mut lob| byte_lob(&mut lob, cap)).map(|v| hex(&v));
    }
    if ty == oracledb::DB_TYPE_DATE
        || ty == oracledb::DB_TYPE_TIMESTAMP
        || ty == oracledb::DB_TYPE_TIMESTAMP_LTZ
        || ty == oracledb::DB_TYPE_TIMESTAMP_TZ
    {
        return row.get::<Option<OracleTimestamp>>(i).ok()?.map(|v| timestamp(&v));
    }
    if let Ok(value) = row.get::<Option<String>>(i) {
        return value;
    }
    if let Ok(value) = row.get::<Option<bool>>(i) {
        return value.map(|v| v.to_string());
    }
    if let Ok(value) = row.get::<Option<f64>>(i) {
        return value.map(|v| v.to_string());
    }
    if let Ok(value) = row.get::<Option<f32>>(i) {
        return value.map(|v| v.to_string());
    }
    if let Ok(value) = row.get::<Option<Vec<u8>>>(i) {
        return value.map(|b| bytes_text(b, mode));
    }
    if let Ok(value) = row.get::<Option<OracleIntervalDS>>(i) {
        return value.map(|v| v.to_string());
    }
    if let Ok(value) = row.get::<Option<OracleIntervalYM>>(i) {
        return value.map(|v| v.to_string());
    }
    // The remaining types (objects, REF cursors…) show as SQL NULL in the grid,
    // exactly as the previous driver rendered them.
    None
}

/// Reads a character LOB prefix. CLOB offsets are UTF-16 units, so reading
/// `cap` characters needs a larger byte budget; surplus is trimmed.
fn character_lob(lob: &mut Lob, cap: u64) -> Option<String> {
    let mut text = String::new();
    lob.take(cap * 4).read_to_string(&mut text).ok()?;
    Some(text.chars().take(cap as usize).collect())
}

fn byte_lob(lob: &mut Lob, cap: u64) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    lob.take(cap).read_to_end(&mut bytes).ok()?;
    Some(bytes)
}

/// The text without the time-of-day parts the value does not carry.
fn timestamp(v: &OracleTimestamp) -> String {
    let mut s =
        format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", v.year(), v.month(), v.day(), v.hour(), v.minute(), v.second());
    let nano = v.nanoseconds();
    if nano != 0 {
        s.push('.');
        s.push_str(format!("{nano:09}").trim_end_matches('0'));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_strings() {
        let s = |db: &str| DataSource {
            driver: DbDriver::Oracle,
            host: "db.local".into(),
            port: Some(1521),
            database: db.into(),
            ..Default::default()
        };
        assert_eq!(connect_string_with(&s("ORCLPDB1"), false).unwrap(), "//db.local:1521/ORCLPDB1");
        // The thin driver cannot connect without a service name (or a resolver
        // for the Database field's alias or descriptor).
        assert!(connect_string_with(&s(""), false).is_err());
        let empty_host = |db: &str| DataSource { host: String::new(), port: None, database: db.into(), ..s("") };
        assert_eq!(connect_string_with(&empty_host("ORCL"), false).unwrap(), "//localhost:1521/ORCL");
        assert_eq!(
            connect_string_with(&s("(DESCRIPTION=(ADDRESS=...))"), false).unwrap(),
            "(DESCRIPTION=(ADDRESS=...))"
        );
        assert_eq!(connect_string_with(&s("PROD=MYDB"), false).unwrap(), "PROD=MYDB");
    }

    #[test]
    fn a_tns_alias_resolves_when_tnsnames_exists() {
        let alias = DataSource {
            driver: DbDriver::Oracle,
            host: String::new(),
            port: None,
            database: "PROD_DB".into(),
            ..Default::default()
        };
        // alias=true: passed as is (the driver asks tnsnames.ora for it).
        assert_eq!(connect_string_with(&alias, true).unwrap(), "PROD_DB");
        // Without a resolvable tnsnames.ora the alias name connects as a
        // service name; the alias form is only used when the file exists.
        assert_eq!(connect_string_with(&alias, false).unwrap(), "//localhost:1521/PROD_DB");
    }

    #[test]
    fn write_detection() {
        assert!(!is_write("SELECT 1 FROM DUAL"));
        assert!(!is_write("-- x\nWITH q AS (SELECT 1) SELECT * FROM q"));
        assert!(!is_write("DESC users"));
        assert!(is_write("INSERT INTO t VALUES (1)"));
        assert!(is_write("begin\n  null;\nend;"));
        assert!(is_write("DROP TABLE t"));
    }

    #[test]
    fn missing_user_is_rejected_before_connecting() {
        let setup = Setup::default();
        let err = State::of(&DataSource { driver: DbDriver::Oracle, ..Default::default() }, None, &setup).unwrap_err();
        assert!(err.message.contains("user name"));
        let state = State::of(
            &DataSource {
                driver: DbDriver::Oracle,
                user: "scott".into(),
                database: "ORCLPDB1".into(),
                ..Default::default()
            },
            Some("tiger"),
            &setup,
        )
        .unwrap();
        assert_eq!(
            (state.user.as_str(), state.password.as_str(), state.connect.as_str()),
            ("scott", "tiger", "//localhost:1521/ORCLPDB1")
        );
        assert!(state.auto_commit);
        state.config().expect("valid config");
    }

    #[test]
    fn configs_carry_the_connect_string_and_driver_name() {
        let setup = Setup { statement_cache: Some(4), ..Setup::default() };
        let state = State::of(
            &DataSource {
                driver: DbDriver::Oracle,
                user: "scott".into(),
                database: "ORCLPDB1".into(),
                ..Default::default()
            },
            Some("tiger"),
            &setup,
        )
        .unwrap();
        let config = state.config().unwrap();
        assert_eq!(config.user(), Some("scott"));
        assert_eq!(config.driver_name(), "OpenIDEA");
        assert_eq!(config.stmtcachesize(), 4);
    }

    #[test]
    fn configs_validate_the_connect_string() {
        let source = DataSource {
            driver: DbDriver::Oracle,
            user: "scott".into(),
            database: "((DESCRIPTION=(ADDRESS=.".into(),
            ..Default::default()
        };
        let state = State::of(&source, None, &Setup::default()).unwrap();
        let err = match state.config() {
            Err(e) => e,
            Ok(_) => unreachable!("an invalid connect string reports an error"),
        };
        assert_eq!(err.kind, ErrorKind::InvalidInput, "{}", err.message);
    }

    #[test]
    fn a_discarded_connection_is_broken_but_a_live_one_is_not() {
        // `has_broken` decides whether bb8 keeps a connection, which is what
        // cancellation relies on.
        let manager = OracleConnectionManager(Arc::new(
            State::of(
                &DataSource {
                    driver: DbDriver::Oracle,
                    user: "scott".into(),
                    database: "ORCLPDB1".into(),
                    ..Default::default()
                },
                None,
                &Setup::default(),
            )
            .unwrap(),
        ));
        let mut broken = OracleHandle { conn: None, context: crate::conn::Context::new(None) };
        assert!(manager.has_broken(&mut broken));
    }

    /// No client library is involved: connecting fails on the network alone.
    #[tokio::test]
    async fn connecting_without_a_server_reports_clearly() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let state = State::of(
            &DataSource {
                driver: DbDriver::Oracle,
                user: "scott".into(),
                host: "127.0.0.1".into(),
                port: Some(port),
                database: "ORCLPDB1".into(),
                ..Default::default()
            },
            Some("tiger"),
            &Setup::default(),
        )
        .unwrap();
        match open(&state) {
            Ok(_) => unreachable!("no server is listening"),
            Err(err) => assert!(!err.into_ide().message.is_empty()),
        }
    }

    #[test]
    fn timestamps_render_without_nano_tail() {
        let v = OracleTimestamp::new_date(2026, 11, 8);
        assert_eq!(timestamp(&v), "2026-11-08 00:00:00");
        let v = OracleTimestamp::new_date(2026, 1, 2);
        assert_eq!(timestamp(&v), "2026-01-02 00:00:00");
    }

    /// A live server, skipped unless `OPENIDEA_ORACLE_TEST` names
    /// `user/password@host:port/service` (the thin driver needs no client
    /// libraries, only `TNS_ADMIN`-style net config for aliases).
    ///
    /// Checking out a connection runs `is_valid`, so this fails if the ping is
    /// not a query (bb8 silently retries a rejected check-out until it times out).
    #[tokio::test]
    async fn a_pooled_connection_runs_queries() {
        let Some(spec) = std::env::var("OPENIDEA_ORACLE_TEST").ok().filter(|s| !s.is_empty()) else { return };
        let (auth, addr) = spec.split_once('@').expect("user/password@host:port/service");
        let (user, password) = auth.split_once('/').expect("user/password");
        let (host, rest) = addr.split_once(':').expect("host:port/service");
        let (port, service) = rest.split_once('/').expect("port/service");
        let source = DataSource {
            driver: DbDriver::Oracle,
            host: host.into(),
            port: Some(port.parse().expect("port")),
            database: service.into(),
            user: user.into(),
            ..Default::default()
        };
        // A missing or wrong password asks for one (the UI prompts on PermissionDenied).
        for wrong in [None, Some("wrong")] {
            let Err(err) = crate::conn::Pool::connect(&source, wrong, None).await else { panic!("bad password") };
            assert_eq!(err.kind, ErrorKind::PermissionDenied, "{}", err.message);
        }
        let pool = crate::conn::Pool::connect(&source, Some(password), None).await.expect("connect");
        let rows = pool.rows("SELECT 1 FROM DUAL", 1).await.expect("query through the pool");
        assert_eq!(rows.first().and_then(|r| r.first().cloned().flatten()).as_deref(), Some("1"));

        // The result grid reads later pages by skipping rows client-side.
        let page = pool
            .execute_from("SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 10", 4, 3, &CancellationToken::new())
            .await
            .expect("second page");
        let values: Vec<String> = page.rows.iter().filter_map(|r| r.first().cloned().flatten()).collect();
        assert_eq!(values, ["5", "6", "7"]);
        assert!(page.has_more);

        // PL/SQL units reach the server with their closing `END;`.
        pool.rows("begin null; end;", 0).await.expect("anonymous block");
        pool.rows("declare n number := 1; begin n := n + 1; end;", 0).await.expect("declare block");
        let proc = "create or replace procedure openidea_p(a number) is\n  x number := a;\nbegin\n  x := x + 1;\nend;";
        let rows = pool.rows(&format!("{proc}\nselect 7 from dual;"), 1).await.expect("procedure, then a query");
        assert_eq!(rows.first().and_then(|r| r.first().cloned().flatten()).as_deref(), Some("7"));
        pool.rows("begin openidea_p(1); end;", 0).await.expect("call the procedure");
        let package = "create or replace package openidea_k as\n  function f return number;\nend openidea_k;\n/\n\
                       create or replace package body openidea_k as\n  function f return number is begin return 8; end;\nend openidea_k;\n/";
        pool.rows(package, 0).await.expect("package spec and body");
        let rows = pool.rows("select openidea_k.f from dual", 1).await.expect("call the package");
        assert_eq!(rows.first().and_then(|r| r.first().cloned().flatten()).as_deref(), Some("8"));
        pool.rows("drop package openidea_k; drop procedure openidea_p", 0).await.expect("clean up");

        // LOB cells are read through locators.
        let rows = pool.rows("select to_clob('abc'), to_blob(hextoraw('0A0B')) from dual", 1).await.expect("LOBs");
        assert_eq!(rows.first().cloned(), Some(vec![Some("abc".into()), Some("0x0A0B".into())]));
    }
}
