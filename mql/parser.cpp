#include "mql/ast.h"
#include "mql/lexer.h"
#include <stdexcept>

// A recursive-descent parser over the lexer's tokens, one function per
// statement.

namespace {

class Parser {
public:
    explicit Parser(const std::string& text) : t_(lex(text)) {}

    Statement statement() {
        const Token& first = t_[i_];
        if (first.type != TokenType::KEYWORD) fail("expected a statement");
        Statement s = dispatch(first.value);
        if (peek(TokenType::SEMI)) i_++;
        if (!peek(TokenType::END)) fail("unexpected '" + t_[i_].value + "' after the statement");
        return s;
    }

private:
    std::vector<Token> t_;
    size_t             i_ = 0;

    [[noreturn]] void fail(const std::string& msg) const { throw std::runtime_error(msg); }

    bool peek(TokenType type) const { return t_[i_].type == type; }
    bool peek_kw(const char* kw) const { return t_[i_].type == TokenType::KEYWORD && t_[i_].value == kw; }
    bool accept_kw(const char* kw) { if (!peek_kw(kw)) return false; i_++; return true; }
    void expect(TokenType type, const char* what) {
        if (!peek(type)) fail(std::string("expected ") + what + ", got '" + t_[i_].value + "'");
        i_++;
    }
    void expect_kw(const char* kw) {
        if (!accept_kw(kw)) fail(std::string("expected ") + kw + ", got '" + t_[i_].value + "'");
    }
    // A table or column name. Keywords are allowed (a column named "key"):
    // every place a name appears is unambiguous.
    std::string name(const char* what) {
        const Token& tok = t_[i_];
        if (tok.type == TokenType::IDENT)   { i_++; return tok.value; }
        if (tok.type == TokenType::KEYWORD) { i_++; return tok.text; }
        fail(std::string("expected ") + what + ", got '" + tok.value + "'");
    }
    Literal literal() {
        const Token& tok = t_[i_];
        if (tok.type == TokenType::NUMBER) { i_++; return {Literal::Kind::NUMBER, tok.value}; }
        if (tok.type == TokenType::STRING) { i_++; return {Literal::Kind::STRING, tok.value}; }
        fail("expected a value, got '" + tok.value + "'");
    }
    size_t number(const char* what) {
        if (!peek(TokenType::NUMBER)) fail(std::string("expected ") + what);
        return std::stoul(t_[i_++].value);
    }

    Statement dispatch(const std::string& kw) {
        i_++;
        if (kw == "CREATE") {
            if (accept_kw("INDEX")) return create_index();
            expect_kw("TABLE");
            return create_table();
        }
        if (kw == "DROP") {
            if (accept_kw("INDEX")) {
                DropIndex d{name("an index name"), {}};
                expect_kw("ON");
                d.table = name("a table name");
                return d;
            }
            expect_kw("TABLE");
            return DropTable{name("a table name")};
        }
        if (kw == "SHOW") {
            if (accept_kw("TABLES")) return ShowTables{};
            expect_kw("CHAINS");
            return Chains{name("a table name")};
        }
        if (kw == "DESCRIBE") return Describe{name("a table name")};
        if (kw == "INSERT")   return insert();
        if (kw == "SELECT")   return select();
        if (kw == "UPDATE")   return update();
        if (kw == "DELETE")   { expect_kw("FROM"); Delete d{name("a table name"), {}}; d.where = where(); return d; }
        if (kw == "COMPACT")  return Compact{name("a table name")};
        if (kw == "STATS")    return Stats{};
        if (kw == "HELP")     return Help{};
        fail("unknown statement: " + kw);
    }

    CreateTable create_table() {
        CreateTable c{name("a table name"), {}};
        expect(TokenType::LPAREN, "'('");
        do {
            ColumnSpec col;
            col.name = name("a column name");
            if (!peek(TokenType::IDENT)) fail("expected a type for column '" + col.name + "'");
            col.type = t_[i_++].value;
            for (char& ch : col.type) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
            if (peek(TokenType::LPAREN)) {   // VARCHAR(n)
                i_++;
                col.size = number("a size");
                expect(TokenType::RPAREN, "')'");
            }
            if (accept_kw("PRIMARY")) { expect_kw("KEY"); col.primary_key = true; }
            c.columns.push_back(std::move(col));
        } while (peek(TokenType::COMMA) && (i_++, true));
        expect(TokenType::RPAREN, "')'");
        return c;
    }

    // CREATE INDEX name ON t (col, ...)
    CreateIndex create_index() {
        CreateIndex c{name("an index name"), {}, {}};
        expect_kw("ON");
        c.table = name("a table name");
        expect(TokenType::LPAREN, "'('");
        do c.columns.push_back(name("a column name")); while (peek(TokenType::COMMA) && (i_++, true));
        expect(TokenType::RPAREN, "')'");
        return c;
    }

    Insert insert() {
        expect_kw("INTO");
        Insert ins{name("a table name"), {}};
        expect_kw("VALUES");
        do {
            expect(TokenType::LPAREN, "'('");
            std::vector<Literal> row;
            do row.push_back(literal()); while (peek(TokenType::COMMA) && (i_++, true));
            expect(TokenType::RPAREN, "')'");
            ins.rows.push_back(std::move(row));
        } while (peek(TokenType::COMMA) && (i_++, true));
        return ins;
    }

    Select select() {
        Select s;
        if (peek(TokenType::STAR)) {
            i_++;
        } else if (peek_kw("COUNT") && t_[i_ + 1].type == TokenType::LPAREN) {
            i_ += 2;
            expect(TokenType::STAR, "'*'");
            expect(TokenType::RPAREN, "')'");
            s.count = true;
        } else {
            do s.columns.push_back(name("a column name")); while (peek(TokenType::COMMA) && (i_++, true));
        }
        expect_kw("FROM");
        s.table = name("a table name");
        s.where = where();
        if (accept_kw("LIMIT")) s.limit = number("a row limit after LIMIT");
        return s;
    }

    Update update() {
        Update u{name("a table name"), {}, {}};
        expect_kw("SET");
        do {
            std::string col = name("a column name");
            if (!peek(TokenType::OP) || t_[i_].value != "=") fail("expected '=' after " + col);
            i_++;
            u.set.push_back({col, literal()});
        } while (peek(TokenType::COMMA) && (i_++, true));
        u.where = where();
        return u;
    }

    // [WHERE cond AND cond ...]
    Where where() {
        Where w;
        if (!accept_kw("WHERE")) return w;
        do w.push_back(condition()); while (accept_kw("AND"));
        return w;
    }

    Condition condition() {
        Condition c{Condition::Op::EQ, name("a column name"), {}, {}, {}};
        if (accept_kw("IN")) {
            c.op = Condition::Op::IN;
            expect(TokenType::LPAREN, "'('");
            do c.list.push_back(literal()); while (peek(TokenType::COMMA) && (i_++, true));
            expect(TokenType::RPAREN, "')'");
            return c;
        }
        if (accept_kw("BETWEEN")) {
            c.op    = Condition::Op::BETWEEN;
            c.value = literal();
            expect_kw("AND");
            c.upper = literal();
            return c;
        }
        if (!peek(TokenType::OP)) fail("expected a comparison after " + c.column);
        const std::string op = t_[i_++].value;
        c.op = op == "=" ? Condition::Op::EQ : op == "<" ? Condition::Op::LT : op == "<=" ? Condition::Op::LE
             : op == ">" ? Condition::Op::GT : Condition::Op::GE;
        c.value = literal();
        return c;
    }
};

}  // namespace

Statement parse(const std::string& text) {
    return Parser(text).statement();
}
