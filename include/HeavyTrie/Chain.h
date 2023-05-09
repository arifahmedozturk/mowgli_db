#ifndef CHAIN_H
#define CHAIN_H

#include <string>
#include <vector>

using namespace std;

class Chain {
    public:
        Chain(vector<char> values) {
            this -> values = values;
        }

        string get_match_from_word(string word) {
            int max_match_length = min(word.length(), values.size());
            
            for(int i = 0; i < max_match_length; i++) {
                if(word[i] != values[i]) 
                    return word.substr(0, i);
            }
            return word.substr(0, max_match_length);
        }

        vector<char> get_values() {
            return values;
        }

private:
    vector<char> values;
};

#endif // CHAIN_H
