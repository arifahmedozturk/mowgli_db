#ifndef DISKWRITER_H
#define DISKWRITER_H

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

class DiskWriter {
    public:
        DiskWriter() {

        }

        void write_values_to_file(vector<char> values, string file_path) {
            ofstream fout(file_path.c_str());
            fout << values.size() << " ";
            for(int i = 0; i < values.size(); i++) {
                fout << values[i];
            }
            fout.close();
        }
};

#endif // DISKWRITER_H
