#include <unordered_map>
#include <string>

#ifndef HUFFMANNODE_H
#define HUFFMANNODE_H

using namespace std;

class HuffmanNode {
    public:
        int symbol;
        int frequency;
        HuffmanNode* left_son;
        HuffmanNode* right_son;

        HuffmanNode(int symbol, int frequency) {
            this->symbol = symbol;
            this->frequency = frequency;
            this->left_son = NULL;
            this->right_son = NULL;
        }

        HuffmanNode(HuffmanNode* left_son, HuffmanNode* right_son) {
            this->symbol = -1;
            this->frequency = left_son->frequency + right_son->frequency;
            this->left_son = left_son;
            this->right_son = right_son;
        }

        void expand_node_for_translation(unordered_map<int, string> &translation_dictionary, string current_value="") {
            if((this->left_son == NULL) || (this->right_son == NULL)) {
                translation_dictionary[this->symbol] = current_value;
                return;
            }

            this->left_son->expand_node_for_translation(translation_dictionary, current_value + "0");
            this->right_son->expand_node_for_translation(translation_dictionary, current_value + "1");
        }

        unordered_map<int, string> get_translation_dictionary() {
            unordered_map<int, string> translation_dictionary = {};
            expand_node_for_translation(translation_dictionary);
            return translation_dictionary;
        }
};

#endif // HUFFMANNODE_H
