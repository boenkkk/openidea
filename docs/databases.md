# Databases

The Database tool window (View ▸ Tool Windows ▸ Database) connects to SQLite,
PostgreSQL, MySQL/MariaDB, Microsoft SQL Server, ClickHouse and Oracle, plus the
wire-compatible products listed in **New ▸ Data Source**. Connections are saved
per project in `.openidea/data-sources.toml`; passwords live outside the project
(and never in that file).

## Oracle

Oracle connections use the pure-Rust thin driver: **no Oracle client library and
no ORACLE_HOME are needed**. The driver **requires Oracle Database 19 or later**
(also tested against 21c and 26ai) and connects directly over the network.

1. General tab: host, port (1521), and the **service name** (for example
   `ORCLPDB1`) in the Database field. Connect with the schema's user and its
   password.
2. A full TNS descriptor, or a TNS alias resolved from `tnsnames.ora`
   (`TNS_ADMIN` or `$ORACLE_HOME/network/admin`), also works in the Database
   field.

`java.sql`-style URLs also work in the URL field:

```
oracle://scott:tiger@db.example.com:1521/ORCLPDB1
jdbc:oracle:thin:@//db.example.com:1521/ORCLPDB1
```

### What Oracle data sources support

- Schemas = Oracle users (`ALL_USERS`); system accounts (`SYS`, `SYSTEM`,
  `XDB`, `MDSYS`…) appear only with **Schemas ▸ Show system**.
- Tables, views, columns, indexes, foreign keys, triggers, routines and
  sequences are read from the `ALL_*` views.
- The query console runs SQL and PL/SQL blocks; `BEGIN … END;` blocks (nested
  `;` included) are sent as one statement, and a lone `/` on its own line ends
  one as in SQL*Plus.
- Explain Plan runs `EXPLAIN PLAN` and reads `PLAN_TABLE` (`DBMS_XPLAN` output
  is not required).
- Table editor (INSERT/UPDATE/DELETE in one transaction), Import/Export and
  generated SQL use Oracle syntax: `"UPPER_CASE"` identifiers, `FETCH FIRST n
  ROWS ONLY`, `MODIFY (…)` for column changes.

Table editing needs a primary key, and the connection must not be read-only.
