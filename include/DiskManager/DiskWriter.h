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

        void write_string_to_id(string text, int id) {
            ofstream file("data/data0.txt");
            int starting_position = Config::BLOCK_SIZE * (id - 1);
            file.seekp(Config::BLOCK_SIZE, ios::beg);
            file.write(text.c_str(), Config::BLOCK_SIZE);
        }
};

#endif // DISKWRITER_H
