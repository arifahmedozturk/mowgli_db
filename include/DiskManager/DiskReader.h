#ifndef DISKREADER_H
#define DISKREADER_H

#include <fstream>
#include <string>
#include <vector>
#include "Config/Config.h"

using namespace std;

class DiskReader {
    public:
        DiskReader() {

        }

        string read_data_block(int block_id) {
            ifstream file("data/data0.txt");
            int starting_position = Config::BLOCK_SIZE * (block_id - 1);
            file.seekg(Config::BLOCK_SIZE, ios::beg);
            char buffer[Config::BLOCK_SIZE];
            file.read(buffer, Config::BLOCK_SIZE);
            return string(buffer);
        }
};

#endif // DISKREADER_H
