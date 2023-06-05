#include <vector>
#include <string>
#include <algorithm>
#include <queue>
#include <functional>
#include <iostream>
#include <unordered_map>
#include "HuffmanNode.h"
#include "../BaseComponents/BaseEncoder.h"

#ifndef HUFFMANENCODER_H
#define HUFFMANENCODER_H

using namespace std;

bool compare_huffman_nodes(HuffmanNode* &node, HuffmanNode* &other_node) {
    return node->frequency > other_node->frequency;
}

string encode_symbol_for_huffman_dictionary(int symbol, string symbol_encoding, int bucket_size=8) {
    string binary_symbol = "";
    for(int current_bit = bucket_size - 1; current_bit >= 0; current_bit--) {
        if((1 << current_bit)&symbol)
            binary_symbol += "1";
        else
            binary_symbol += "0";
    }
    for(int current_bit = bucket_size - 1; current_bit >= 0; current_bit--) {
        if((1 << current_bit)&symbol_encoding.length())
            binary_symbol += "1";
        else
            binary_symbol += "0";
    }

    return binary_symbol+symbol_encoding;
}

class HuffmanEncoder : BaseEncoder {
    public:
        HuffmanEncoder() {

        }

        HuffmanNode* get_huffman_tree_root_from_numbers(vector<int> numbers) {
            vector<HuffmanNode*> nodes = get_nodes_from_values(numbers);
            HuffmanNode* tree_head = create_huffman_tree_from_leaves(nodes);
            return tree_head;
        }

        unordered_map<int, string> get_encoding_dictionary_from_numbers(vector<int> numbers) {
            vector<HuffmanNode*> nodes = get_nodes_from_values(numbers);
            HuffmanNode* tree_head = create_huffman_tree_from_leaves(nodes);
            return tree_head->get_translation_dictionary();
        }

        string get_encoding_string_from_numbers(vector<int> numbers) {
            HuffmanNode* tree_head = get_huffman_tree_root_from_numbers(numbers);
            unordered_map<int, string>translation_dictionary = tree_head->get_translation_dictionary();
            string binary_string = create_binary_string_from_translation_dictionary_and_huffman_tree(numbers, tree_head, translation_dictionary);
            return binary_string;
        }

        string get_dictionary_string_from_numbers(vector<int> numbers) {
            HuffmanNode* tree_head = get_huffman_tree_root_from_numbers(numbers);
            return create_translation_string_from_huffman_node(tree_head);
        }

    private:
        vector<HuffmanNode*> get_nodes_from_values(vector<int> numbers) {
            vector<HuffmanNode*> huffman_nodes;
            huffman_nodes.resize(0);

            int numbers_size = (int) numbers.size();
            if(numbers_size == 0)
                return huffman_nodes;

            sort(numbers.begin(), numbers.end());

            int current_number = numbers[0], current_number_frequency = 1;
            for(int i = 1; i < numbers_size; i++) {
                if(current_number == numbers[i])
                        ++current_number_frequency;
                else {
                    huffman_nodes.push_back(new HuffmanNode(current_number, current_number_frequency));
                    current_number = numbers[i];
                    current_number_frequency = 1;
                }
            }
            huffman_nodes.push_back(new HuffmanNode(current_number, current_number_frequency));

            return huffman_nodes;
        }

        HuffmanNode* create_huffman_tree_from_leaves(vector<HuffmanNode*> &leaves) {
            priority_queue<HuffmanNode*, vector<HuffmanNode*>, decltype(&compare_huffman_nodes) > pq(compare_huffman_nodes);
            for(vector<HuffmanNode*>::iterator it = leaves.begin(); it != leaves.end(); ++it)
                    pq.push(*it);

            while(pq.size() > 1) {
                HuffmanNode* first_node = pq.top();
                pq.pop();
                HuffmanNode* second_node = pq.top();
                pq.pop();

                HuffmanNode* new_node = new HuffmanNode(first_node, second_node);
                pq.push(new_node);
            }

            return pq.top();
        }

        string create_translation_string_from_huffman_node(HuffmanNode* tree_head) {
            string translation_string = "";
            queue<HuffmanNode*> nodes_queue;
            nodes_queue = queue<HuffmanNode*>();
            nodes_queue.push(tree_head);
            while(!nodes_queue.empty()) {
                HuffmanNode* current_node = nodes_queue.front();
                nodes_queue.pop();
                if(current_node->symbol == -1) {
                    nodes_queue.push(current_node->left_son);
                    nodes_queue.push(current_node->right_son);
                    translation_string += "0";
                }
                else {
                    translation_string += "1";
                    translation_string += get_bitstring_from_number(current_node->symbol);
                }
            }

            return translation_string;
        }

        string create_binary_string_from_translation_dictionary_and_huffman_tree(vector<int> numbers, HuffmanNode* tree_head, unordered_map<int, string> translation_dictionary, int bucket_size = 8) {
            string dictionary_string = create_translation_string_from_huffman_node(tree_head);
            /*
            string dictionary_string = "";
            for (auto& [key, value]: translation_dictionary) {
                dictionary_string += encode_symbol_for_huffman_dictionary(key, value);
            }
            */

            string numbers_string = "";
            for(auto& number: numbers)
                numbers_string += translation_dictionary[number];

            return dictionary_string + numbers_string;
        }

        string turn_binary_string_into_compressed_string(string binary_string, int bucket_size = 8) {
            string compressed_string = "";
            int binary_string_length = binary_string.length();

            int current_bit = bucket_size - 1;
            char current_symbol = 0;
            for(int i = 0; i <binary_string_length; i++) {
                if(binary_string[i] == '1')
                    current_symbol += (1<<current_bit);

                if(--current_bit == -1) {
                    current_bit = bucket_size - 1;
                    compressed_string += current_symbol;
                    current_symbol = 0;
                }
            }
            if(current_bit != bucket_size - 1)
                compressed_string += current_symbol;

            return compressed_string;
        }
};
#endif // HUFFMANENCODER_H
