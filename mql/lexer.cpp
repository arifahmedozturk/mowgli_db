#include "mql/lexer.h"
#include <cctype>
#include <stdexcept>

static const std::string KEYWORDS[] = {
    "TABLE", "NEW", "UPDATE", "QUERY", "DELETE", "PRIMARY", "KEY",
    "CHAINS", "COUNT", "RANGE", "IN", "HELP", "BULK"
};

static bool is_keyword(const std::string& s) {
    for (const auto& k : KEYWORDS)
        if (s == k) return true;
    return false;
}

std::vector<Token> lex(const std::string& input) {
    std::vector<Token> tokens;
    size_t i = 0;
    const size_t n = input.size();

    while (i < n) {
        if (std::isspace(static_cast<unsigned char>(input[i]))) { i++; continue; }

        char c = input[i];

        if (c == '(')       { tokens.push_back({TokenType::LPAREN, "("}); i++; }
        else if (c == ')')  { tokens.push_back({TokenType::RPAREN, ")"}); i++; }
        else if (c == ',')  { tokens.push_back({TokenType::COMMA,  ","}); i++; }
        else if (c == '\'') {
            // quoted string
            i++;
            std::string val;
            while (i < n && input[i] != '\'') val += input[i++];
            if (i >= n) throw std::runtime_error("unterminated string literal");
            i++;
            tokens.push_back({TokenType::STRING, val});
        }
        else if (std::isdigit(static_cast<unsigned char>(c))) {
            std::string val;
            while (i < n && std::isdigit(static_cast<unsigned char>(input[i])))
                val += input[i++];
            tokens.push_back({TokenType::NUMBER, val});
        }
        else if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
            std::string val;
            while (i < n && (std::isalnum(static_cast<unsigned char>(input[i]))
                              || input[i] == '_'))
                val += input[i++];
            std::string upper = val;
            for (char& ch : upper) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
            if (is_keyword(upper))
                tokens.push_back({TokenType::KEYWORD, upper});
            else
                tokens.push_back({TokenType::IDENT, val});
        }
        else {
            throw std::runtime_error(std::string("unexpected character: ") + c);
        }
    }

    tokens.push_back({TokenType::END, ""});
    return tokens;
}
