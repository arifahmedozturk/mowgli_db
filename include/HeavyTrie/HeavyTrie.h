#ifndef HEAVYTRIE_H
#define HEAVYTRIE_H

#include "HeavyTrie/Chain.h"
#include "DiskManager/FileHelper.h"

#include <string>
#include <vector>

using namespace std;

class HeavyTrie {
    public:
        HeavyTrie() {

        }
        
        void insert(string index) {
            string chain_root_path = "./indexes/root/" + index[0] + ".txt";
            if(!file_helper.file_exists(chain_root_path)) {
                vector<char> values(index.begin(), index.end());
                Chain new_chain(values);
                new_chain.write_chain_to_file(chain_root_path);
                return;
            }
        }
    private:
        FileHelper file_helper;
}

#endif // HEAVYTRIE_H
