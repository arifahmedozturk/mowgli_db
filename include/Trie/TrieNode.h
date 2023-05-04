#include <vector>

using namespace std;

#ifndef TRIENODE_H
#define TRIENODE_H

class TrieNode {
    public:
        TrieNode(int storage_key) {
            this -> key_ends = true;
            this -> storage_key = storage_key;
            this -> children.resize(256, false);
        }

        TrieNode(vector<bool> children) {
            this -> key_ends = false;
            this -> storage_key = -1;
            this -> children = children;
        }

        bool is_key_end() {
            return key_ends;
        }

        int get_storage_key() {
            return storage_key;
        }

        vector<int> get_children() {
            vector<int> all_children;
            all_children.resize(0);

            for(int i = 0; i < 256; i++) {
                if(children[i]) {
                    all_children.push_back(i);
                }
            }

            return all_children;
        }
    private:
        bool key_ends;
        int storage_key;
        vector<bool> children;
};

#endif //TRIENODE_H