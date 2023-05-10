#ifndef CHAIN_H
#define CHAIN_H

#include "DiskManager/DiskWriter.h"

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

        void write_chain_to_file(string file_path) {
            disk_writer.write_values_to_file(values, file_path);
        }

        vector<char> get_values() {
            return values;
        }

private:
    vector<char> values;
    DiskWriter disk_writer;
};

#endif // CHAIN_H
