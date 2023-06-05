#include <vector>
#include <unordered_map>
#include "../BaseComponents/BaseEncoder.h"
#include "../Huffman/HuffmanEncoder.h"
#include "../LZW/LZWEncoder.h"

#ifndef DEFLATELZWENCODER_H
#define DEFLATELZWENCODER_H

using namespace std;

class DeflateLZWEncoder : BaseEncoder {
    public:
        DeflateLZWEncoder() {

        }

        string get_encoding_string_from_numbers(vector<int> numbers) {
            vector<pair<int, bool> > lzw_values = lzw_encoder.get_translated_values_from_numbers(numbers);

            vector<int> normal_values, dictionary_values;
            tie(normal_values, dictionary_values) = split_values(lzw_values);

            unordered_map<int, string> normal_huffman_dictionary = huffman_encoder.get_encoding_dictionary_from_numbers(normal_values);
            unordered_map<int, string> dictionary_huffman_dictionary = huffman_encoder.get_encoding_dictionary_from_numbers(dictionary_values);

            string huffman_dictionary_strings = huffman_encoder.get_dictionary_string_from_numbers(normal_values) + huffman_encoder.get_dictionary_string_from_numbers(dictionary_values);

            return huffman_dictionary_strings + encode_lzw_values_with_huffman_dictionaries(lzw_values, normal_huffman_dictionary, dictionary_huffman_dictionary);
        }
    private:
        HuffmanEncoder huffman_encoder;
        LZWEncoder lzw_encoder;

        pair<vector<int>, vector<int> > split_values(vector<pair<int, bool> > lzw_values) {
            vector<int> normal_values;
            normal_values.clear();
            vector<int> dictionary_values;
            dictionary_values.clear();

            for(vector<pair<int, bool> >::iterator it = lzw_values.begin(); it != lzw_values.end(); ++it) {
                if((*it).second)
                    dictionary_values.push_back((*it).first);
                else
                    normal_values.push_back((*it).second);
            }

            return make_pair(normal_values, dictionary_values);
        }

        string encode_lzw_values_with_huffman_dictionaries(vector<pair<int, bool> > lzw_values, unordered_map<int, string> normal_huffman_dictionary, unordered_map<int, string> dictionary_huffman_dictionary) {
            string encoded_values = "";

            for(vector<pair<int, bool> >::iterator it = lzw_values.begin(); it != lzw_values.end(); ++it) {
                if((*it).second) {
                    encoded_values += "1";
                    encoded_values += dictionary_huffman_dictionary[(*it).first];
                }
                else {
                    encoded_values += "0";
                    encoded_values += normal_huffman_dictionary[(*it).first];
                }
            }

            return encoded_values;
        }
};
#endif // DEFLATELZWENCODER_H
