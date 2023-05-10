#ifndef FILEHELPER_H
#define FILEHELPER_H

#include <cstring>
#include <fstream>
#include <string>
#include <windows.h>

using namespace std;

class FileHelper {
    public:
        FileHelper() {

        }

        bool delete_folder(string folder_path) {
            char current_path_buffer[1000];
            strcpy(current_path_buffer, "");
            GetCurrentDirectory(1000, current_path_buffer);
            string current_path(current_path_buffer), full_folder_path = current_path + "\\" + folder_path;
            string remove_folder_command = "rmdir /s /q " + full_folder_path;
            return system(remove_folder_command.c_str()) == 0;
        }

        bool file_exists(string file_path) {
            ifstream fin(file_path.c_str());
            return fin.good();
        }
};

#endif // FILEHELPER_H