#include <string>
#include <vector>

#ifndef BASEENCODER_H
#define BASEENCODER_H

using namespace std;

class BaseEncoder {
    public:
        virtual string get_encoding_string_from_numbers(vector<int> numbers) = 0;

        string get_bitstring_from_number(int number, int BUCKET_SIZE=8) {
            string bitstring = "";
            for(int bit = BUCKET_SIZE - 1; bit >= 0; bit--) {
                if(number & (1<<bit))
                        bitstring += "1";
                else
                    bitstring += "0";
            }
            
            return bitstring;
        }
};
#endif // BASEENCODER_H