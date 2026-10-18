#pragma once
// Parsed statements of the SQL-like query language (#103). The parser
// (parser.cpp) turns a command's text into one Statement; the engine runs it
// and the server reads it to route the command (#92).
#include <cstddef>
#include <optional>
#include <string>
#include <variant>
#include <vector>

struct Literal {
    enum class Kind { NUMBER, STRING } kind;
    std::string text;   // digits, or the string's bytes (quotes removed, '' unescaped)
};

// One comparison in a WHERE clause; a clause is the AND of all of them.
struct Condition {
    enum class Op { EQ, LT, LE, GT, GE, BETWEEN, IN } op;
    std::string          column;
    Literal              value;
    Literal              upper;   // BETWEEN only: value AND upper, both inclusive
    std::vector<Literal> list;    // IN only
};
using Where = std::vector<Condition>;

struct ColumnSpec {
    std::string name;
    std::string type;     // NUMBER, INT, BIGINT, STRING, TEXT, VARCHAR (as written, upper-cased)
    size_t      size = 0; // VARCHAR(n); 0 = the default
    bool        primary_key = false;
};

struct CreateTable { std::string table; std::vector<ColumnSpec> columns; };
struct DropTable   { std::string table; };
struct ShowTables  {};
struct Describe    { std::string table; };
struct Insert      { std::string table; std::vector<std::vector<Literal>> rows; };
struct Select {
    std::string              table;
    std::vector<std::string> columns;   // empty = *
    bool                     count = false;   // SELECT COUNT(*)
    Where                    where;
    std::optional<size_t>    limit;
};
struct Update {
    std::string                                   table;
    std::vector<std::pair<std::string, Literal>>  set;
    Where                                         where;
};
struct Delete  { std::string table; Where where; };
struct Compact { std::string table; };
struct Chains  { std::string table; };   // SHOW CHAINS t
struct Stats   {};
struct Help    {};

using Statement = std::variant<CreateTable, DropTable, ShowTables, Describe, Insert, Select,
                               Update, Delete, Compact, Chains, Stats, Help>;

// Statements that change data or schema: logged to the WAL and replicated.
inline bool is_write(const Statement& s) {
    return std::holds_alternative<CreateTable>(s) || std::holds_alternative<DropTable>(s) ||
           std::holds_alternative<Insert>(s)      || std::holds_alternative<Update>(s) ||
           std::holds_alternative<Delete>(s);
}

// Parse one statement (an optional trailing ';' is allowed); throws
// std::runtime_error with a message on a syntax error.
Statement parse(const std::string& text);
