#include "mql/lexer.h"
#include <stdexcept>
#include <string_view>

static constexpr std::string_view KEYWORDS[] = {
    "CREATE", "DROP", "TABLE", "TABLES", "SHOW", "DESCRIBE", "PRIMARY", "KEY",
    "INSERT", "INTO", "VALUES", "SELECT", "FROM", "WHERE", "AND", "BETWEEN", "IN", "LIMIT",
    "UPDATE", "SET", "DELETE", "COUNT", "COMPACT", "CHAINS", "STATS", "HELP", "INDEX", "ON", "STARTS_WITH",
};

// ASCII only, and inline: the <cctype> functions consult the locale per call.
static bool is_space(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'; }
static bool is_digit(char c) { return c >= '0' && c <= '9'; }
static bool is_alpha(char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); }
static int  hex_digit(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
static char upper(char c) { return c >= 'a' && c <= 'z' ? static_cast<char>(c - 'a' + 'A') : c; }

// The keyword w spells in any case, or empty. Every word of a statement is
// looked up, so this compares in place rather than building an upper-cased copy.
static std::string_view keyword(std::string_view w) {
    for (std::string_view k : KEYWORDS) {
        if (k.size() != w.size()) continue;
        size_t i = 0;
        while (i < k.size() && upper(w[i]) == k[i]) i++;
        if (i == k.size()) return k;
    }
    return {};
}

std::vector<Token> lex(const std::string& input) {
    std::vector<Token> tokens;
    tokens.reserve(16);
    size_t i = 0;
    const size_t n = input.size();

    while (i < n) {
        if (is_space(input[i])) { i++; continue; }

        char c = input[i];

        if (c == '(')       { tokens.push_back({TokenType::LPAREN, "("}); i++; }
        else if (c == ')')  { tokens.push_back({TokenType::RPAREN, ")"}); i++; }
        else if (c == ',')  { tokens.push_back({TokenType::COMMA,  ","}); i++; }
        else if (c == ';')  { tokens.push_back({TokenType::SEMI,   ";"}); i++; }
        else if (c == '*')  { tokens.push_back({TokenType::STAR,   "*"}); i++; }
        else if (c == '=')  { tokens.push_back({TokenType::OP,     "="}); i++; }
        else if (c == '<' || c == '>') {
            std::string op(1, c);
            if (i + 1 < n && input[i + 1] == '=') op += '=';
            tokens.push_back({TokenType::OP, op});
            i += op.size();
        }
        else if ((c == 'x' || c == 'X') && i + 1 < n && input[i + 1] == '\'') {
            // Hex string x'00ff': any bytes, two hex digits each (#82).
            i += 2;
            std::string val;
            int high = -1;
            while (true) {
                if (i >= n) throw std::runtime_error("unterminated hex literal");
                char h = input[i++];
                if (h == '\'') break;
                int d = hex_digit(h);
                if (d < 0) throw std::runtime_error(std::string("not a hex digit in x'...': ") + h);
                if (high < 0) { high = d; continue; }
                val += static_cast<char>(high << 4 | d);
                high = -1;
            }
            if (high >= 0) throw std::runtime_error("odd number of hex digits in x'...'");
            tokens.push_back({TokenType::STRING, val});
        }
        else if (c == '\'') {
            // Quoted string; '' stands for one quote.
            i++;
            std::string val;
            val.reserve(16);
            while (true) {
                if (i >= n) throw std::runtime_error("unterminated string literal");
                if (input[i] == '\'') {
                    if (i + 1 < n && input[i + 1] == '\'') { val += '\''; i += 2; continue; }
                    i++;
                    break;
                }
                val += input[i++];
            }
            tokens.push_back({TokenType::STRING, val});
        }
        else if (is_digit(c)) {
            size_t start = i;
            while (i < n && is_digit(input[i])) i++;
            tokens.push_back({TokenType::NUMBER, input.substr(start, i - start)});
        }
        else if (is_alpha(c) || c == '_') {
            size_t start = i;
            while (i < n && (is_alpha(input[i]) || is_digit(input[i]) || input[i] == '_')) i++;
            std::string_view word(input.data() + start, i - start);
            std::string_view kw = keyword(word);
            if (!kw.empty())
                tokens.push_back({TokenType::KEYWORD, std::string(kw), std::string(word)});
            else
                tokens.push_back({TokenType::IDENT, std::string(word)});
        }
        else {
            throw std::runtime_error(std::string("unexpected character: ") + c);
        }
    }

    tokens.push_back({TokenType::END, ""});
    return tokens;
}
