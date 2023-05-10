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
            create_data_folders();
        }
        
        void insert(string index) {
            string chain_root_path = "data/indexes/root/" + string(1, index[0]) + ".txt";
            if(!file_helper.file_exists(chain_root_path)) {
                save_chain_from_string(chain_root_path, index);
                return;
            }

            Chain current_chain(chain_root_path);
            string current_prefix = current_chain.get_match_from_index(index);

            while(true) {
                string chain_path = "data/indexes/" + current_prefix + string(1, index[current_prefix.length()]) + ".txt";
                string index_suffix = get_index_suffix(current_prefix, index);

                if(!file_helper.file_exists(chain_path)) {
                    save_chain_from_string(chain_path, index_suffix);
                    break;
                }
                
                current_chain = Chain(chain_root_path);
                current_prefix = current_prefix + current_chain.get_match_from_index(index_suffix);
            }
        }
    private:
        FileHelper file_helper;

        string get_index_suffix(string current_prefix, string index) {
            string index_suffix = "";
            if(current_prefix.length() + 1 < index.length()) {
                index_suffix = index.substr(current_prefix.length() + 1, index.length() - (current_prefix.length() + 1));
            }

            return index_suffix;
        }

        void save_chain_from_string(string path, string index) {
            vector<char> values(index.begin(), index.end());
            Chain new_chain(values);
            new_chain.write_chain_to_file(path);
        }

        void create_data_folders() {
            _mkdir("data");
            _mkdir("data/indexes");
            _mkdir("data/indexes/root");
        }
};

#endif // HEAVYTRIE_H
