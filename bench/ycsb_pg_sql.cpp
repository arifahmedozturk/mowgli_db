#include "bench/ycsb_pg_sql.h"
#include "bench/ycsb_common.h"
#include <stdexcept>

// ---- PG helpers ----

PGconn* pg_connect(const std::string& connstr) {
    PGconn* conn = PQconnectdb(connstr.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::string err = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error("PQconnectdb: " + err);
    }
    return conn;
}

void pg_exec(PGconn* conn, const std::string& sql) {
    PGresult* r = PQexec(conn, sql.c_str());
    ExecStatusType s = PQresultStatus(r);
    if (s != PGRES_COMMAND_OK && s != PGRES_TUPLES_OK) {
        std::string err = PQresultErrorMessage(r);
        PQclear(r);
        throw std::runtime_error("pg_exec: " + err + "\nSQL: " + sql);
    }
    PQclear(r);
}

// ---- schema helpers ----

// Build the CREATE TABLE statement: ycsb (ycsb_key CHAR(14) PRIMARY KEY, field0..field9 CHAR(100))
std::string create_table_sql(const std::string& tname) {
    std::string s = "CREATE TABLE " + tname + " (ycsb_key CHAR(14) PRIMARY KEY";
    for (int i = 0; i < FIELD_COUNT; i++)
        s += ", field" + std::to_string(i) + " CHAR(" + std::to_string(FIELD_SIZE) + ")";
    s += ")";
    return s;
}

// Build parameterised INSERT for COPY-like bulk load (used by single-row inserts in op phase).
// We use a prepared statement: INSERT INTO tname VALUES ($1,$2,...,$11)
void prepare_stmts(PGconn* conn, const std::string& tname) {
    // INSERT
    {
        std::string sql = "INSERT INTO " + tname + " VALUES ($1";
        for (int i = 2; i <= FIELD_COUNT + 1; i++)
            sql += ",$" + std::to_string(i);
        sql += ") ON CONFLICT DO NOTHING";
        PGRes r(PQprepare(conn, "ht_insert", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare insert: ") + PQresultErrorMessage(r.r));
    }
    // SELECT (point lookup)
    {
        std::string sql = "SELECT * FROM " + tname + " WHERE ycsb_key=$1";
        PGRes r(PQprepare(conn, "ht_read", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare read: ") + PQresultErrorMessage(r.r));
    }
    // UPDATE (one random field — we'll always update field0 for simplicity; same cost)
    {
        std::string sql = "UPDATE " + tname + " SET field0=$2 WHERE ycsb_key=$1";
        PGRes r(PQprepare(conn, "ht_update", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare update: ") + PQresultErrorMessage(r.r));
    }
    // SCAN (range with limit)
    {
        std::string sql = "SELECT * FROM " + tname
                        + " WHERE ycsb_key>=$1 ORDER BY ycsb_key LIMIT $2";
        PGRes r(PQprepare(conn, "ht_scan", sql.c_str(), 0, nullptr));
        if (PQresultStatus(r.r) != PGRES_COMMAND_OK)
            throw std::runtime_error(std::string("prepare scan: ") + PQresultErrorMessage(r.r));
    }
}
