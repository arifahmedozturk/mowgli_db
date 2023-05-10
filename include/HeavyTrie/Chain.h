#ifndef CHAIN_H
#define CHAIN_H

#include "DiskManager/DiskReader.h"
#include "DiskManager/DiskWriter.h"

#include <string>
#include <vector>

using namespace std;

class Chain {
    public:
        Chain(vector<char> values) {
            this -> values = values;
        }

        Chain(string file_path) {
            this -> values = disk_reader.get_values_from_file(file_path);
        }

        string get_match_from_index(string word) {
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
        DiskReader disk_reader;
        DiskWriter disk_writer;
        vector<char> values;
};

#endif // CHAIN_H
