
#include <unordered_map>

#ifndef LZWDICTIONARY_H
#define LZWDICTIONARY_H

using namespace std;

class LZWDictionary {
    public:
        LZWDictionary(int capacity_of_dictionary) : dictionary_map{} {
            dictionary_capacity = capacity_of_dictionary;
            dictionary_size = 0;
        }

        void reset() {
            dictionary_size = 0;
            dictionary_map.clear();
        }

        void add(int number1, int number2) {
            string numbers = hash_numbers(number1, number2);

            if(dictionary_size == dictionary_capacity)
                reset();
            
            dictionary_map[numbers] = dictionary_size;
            dictionary_size++;
        }

        int get(int number1, int number2) {
            if(!exists(number1, number2))
                return -1;

            string numbers = hash_numbers(number1, number2);
            
            return dictionary_map[numbers];
        }

        bool exists(int number1, int number2) {
            string numbers = hash_numbers(number1, number2);

            if(dictionary_map.find(numbers) == dictionary_map.end())
                return false;
            
            return true;
        }
    private:
        unordered_map<string, int> dictionary_map;
        int dictionary_capacity;
        int dictionary_size;

        string hash_numbers(int number1, int number2) {
            return to_string(number1) + "+" + to_string(number2);
        }
};
#endif // LZWDICTIONARY_H
