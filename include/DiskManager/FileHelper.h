#ifndef FILEHELPER_H
#define FILEHELPER_H

#include <fstream>

using namespace std;

class FileHelper {
    public:
        FileHelper() {

        }

        bool file_exists(string file_path) {
            ifstream fin(file_path.c_str());
            return fin.good();
        }
};

#endif // FILEHELPER_H