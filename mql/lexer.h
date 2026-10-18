#pragma once
#include <string>
#include <vector>

enum class TokenType {
    KEYWORD,  // SELECT, FROM, WHERE, ... (upper-cased)
    IDENT,    // unquoted name
    STRING,   // 'value'  ('' inside is one ')
    NUMBER,   // 123
    LPAREN,   // (
    RPAREN,   // )
    COMMA,    // ,
    SEMI,     // ;
    STAR,     // *
    OP,       // = < <= > >=
    END
};

struct Token {
    TokenType   type;
    std::string value;
    std::string text = {};   // KEYWORD: as written, for a keyword used as a name
};

std::vector<Token> lex(const std::string& input);
