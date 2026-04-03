#pragma once
#include <string>
#include <vector>

enum class TokenType {
    KEYWORD,  // TABLE, UPDATE, QUERY, DELETE, PRIMARY, KEY
    IDENT,    // unquoted name
    STRING,   // 'value'
    NUMBER,   // 123
    LPAREN,   // (
    RPAREN,   // )
    COMMA,    // ,
    END
};

struct Token {
    TokenType   type;
    std::string value;
};

std::vector<Token> lex(const std::string& input);
