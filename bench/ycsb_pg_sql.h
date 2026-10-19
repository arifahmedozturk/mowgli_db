#pragma once
// libpq helpers and the YCSB table's SQL for ycsb_pg (#105).
#include <libpq-fe.h>
#include <string>

PGconn* pg_connect(const std::string& connstr);   // throws if it cannot connect
void    pg_exec(PGconn* conn, const std::string& sql);   // throws unless the command succeeds

// Thin RAII wrapper around PGresult.
struct PGRes {
    PGresult* r;
    explicit PGRes(PGresult* r) : r(r) {}
    ~PGRes() { if (r) PQclear(r); }
    PGRes(const PGRes&) = delete;
    PGRes& operator=(const PGRes&) = delete;
};

// CREATE TABLE tname (ycsb_key CHAR(14) PRIMARY KEY, field0..field9 CHAR(100))
std::string create_table_sql(const std::string& tname);
// The prepared statements the workloads run: ht_insert, ht_read, ht_update, ht_scan.
void        prepare_stmts(PGconn* conn, const std::string& tname);
