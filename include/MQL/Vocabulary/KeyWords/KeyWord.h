#ifndef KEYWORD_H
#define KEYWORD_H

#include <string>

using namespace std;

class KeyWord {
    public:
        KeyWord(string name) {
            this -> name = name;
        }

        const string getName() {
            return name;
        }
    
    private:
        string name;
};

#endif // KEYWORD_H
