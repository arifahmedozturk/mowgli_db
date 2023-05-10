#ifndef FILEHELPER_H
#define FILEHELPER_H

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>

using namespace std;

class FileHelper {
    public:
        FileHelper() {

        }

        bool delete_folder(string folder_path) {
            string full_folder_path = get_current_path() + "\\" + folder_path;
            string remove_folder_command = "rmdir /s /q " + full_folder_path;

            return system(remove_folder_command.c_str()) == 0;
        }

        bool file_exists(string file_path) {
            ifstream fin(file_path.c_str());
            return fin.good();
        }
    private:
        string get_current_path() {
            char buffer[1000];
            FILE* pipe = _popen("echo %cd%", "r");
            fgets(buffer, sizeof(buffer), pipe);
            _pclose(pipe);
            return string(buffer, strlen(buffer) - 1);
        }
};

#endif // FILEHELPER_H