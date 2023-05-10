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
                save_chain_from_string(chain_root_path, index, true);
                return;
            }

            Chain current_chain(chain_root_path);
            string current_prefix = current_chain.get_match_from_index(index);

            while(true) {
                
            }
        }
    private:
        FileHelper file_helper;

        void save_chain_from_string(string prefix, string index, bool root=false) {
            if(root) {
                vector<char> values(index.begin(), index.end());
                Chain new_chain(values);
                new_chain.write_chain_to_file(prefix);
            }
        }
}

#endif // HEAVYTRIE_H
