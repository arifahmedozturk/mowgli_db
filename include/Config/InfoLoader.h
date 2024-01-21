#ifndef CONFIG_H
#define CONFIG_H

#include <fstream>

using namespace std;

class InfoLoader {
    private:
        int free_data_block_id;
    public:
        InfoLoader(string path="info.mwg") {
            ifstream file(path);
            file >> this->free_data_block_id;
        }
        
        int get_free_data_block_id() {
            return this->free_data_block_id;
        }
};

#endif // CONFIG_H
