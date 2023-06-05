#include <vector>
#include <unordered_map>
#include "../BaseComponents/BaseEncoder.h"
#include "LZWDictionary.h"

#ifndef LZWENCODER_H
#define LZWENCODER_H

using namespace std;

class LZWEncoder : BaseEncoder {
    public:
        LZWEncoder(int no_of_number_bits=8, int no_of_dictionary_bits=10) : lzw_dictionary((1<<no_of_dictionary_bits)){
            number_bits = no_of_number_bits;
            dictionary_bits = no_of_dictionary_bits;
        }

        vector<pair<int, bool> > get_translated_values_from_numbers(vector<int> numbers) {
            vector<pair<int, bool> > translated_values;
            translated_values.clear();

            int current_index = 0;
            int current_number = get_next_number(numbers, current_index);
            int dictionary_used = false;

            while(true) {
                int next_number = -1;
                bool dictionary_used = false;
                
                while(true) {
                    next_number = get_next_number(numbers, current_index);
                    if(next_number == -1)
                        break;
                    
                    if(!lzw_dictionary.exists(current_number, next_number)) 
                        break;
                    int dictionary_number = lzw_dictionary.get(current_number, next_number);

                    if(dictionary_used == false)
                        dictionary_used = true;
                    current_number = dictionary_number;
                }
                translated_values.push_back(make_pair(current_number, dictionary_used));

                if(next_number != -1) {
                    lzw_dictionary.add(current_number, next_number);
                    current_number = next_number;
                }
                else
                    break;
            }

            return translated_values;
        }

        string get_encoding_string_from_numbers(vector<int> numbers) {
            vector<pair<int, bool> > translated_values = get_translated_values_from_numbers(numbers);
            return encode_translated_values(translated_values);
        }
    private:
        LZWDictionary lzw_dictionary;
        int dictionary_bits;
        int number_bits;

        int get_next_number(vector<int> &numbers, int &current_index) {
            if(current_index == numbers.size())
                return -1;
            
            ++current_index;
            return numbers[current_index-1];
        }
        
        string encode_translated_values(vector<pair<int, bool> > &numbers) {
            string translation_string = "";

            for(int i = 0; i < numbers.size(); i++) {
                if(!numbers[i].second)
                    translation_string += "0" + get_bitstring_from_number(numbers[i].first, number_bits);
                else
                    translation_string += "1" + get_bitstring_from_number(numbers[i].first, dictionary_bits);
            }

            return translation_string;
        }
};
#endif // LZWENCODER_H
