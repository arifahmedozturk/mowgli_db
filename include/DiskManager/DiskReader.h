#ifndef DISKREADER_H
#define DISKREADER_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

class DiskReader {
    public:
        DiskReader() {

        }

        vector<char> get_values_from_file(string file_path) {
            vector<char> values;
            values.clear();
            int values_size = 0;
            char ch;

            ifstream fin(file_path.c_str());
            fin >> values_size;
            for(int i = 0; i < values_size; i++) {
                fin >> ch;
                values.push_back(ch);
            }
            fin.close();
            
            return values;
        }
};

#endif // DISKREADER_H
