#ifndef DISKWRITER_H
#define DISKWRITER_H

#include <fstream>
#include <string>
#include <vector>
#include "Config/Config.h"

using namespace std;

class DiskWriter {
    public:
        DiskWriter() {

        }

        void write_string_to_id(string text, int id, string path="data/data0.txt") {
            string padded_text = with_padding(text);
            ofstream file(path, ios::binary);
            int starting_position = Config::BLOCK_SIZE * (id - 1);
            file.seekp(starting_position, ios::beg);
            file.write(padded_text.c_str(), Config::BLOCK_SIZE);
        }
    
        string with_padding(string text) {
            if(text.length() > Config::BLOCK_SIZE) return text;
            string padding(Config::BLOCK_SIZE - text.length(), 'X');
            return text + padding;
        }
};

#endif // DISKWRITER_H
