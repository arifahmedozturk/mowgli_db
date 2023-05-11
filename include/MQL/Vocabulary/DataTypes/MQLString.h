#ifndef MQLSTRING_H
#define MQLSTRING_H

#include "DataType.h"

class MQLString : DataType {
    public:
        MQLString() : DataType("string") {
            this -> name = name;
        }
        
        string convert_from_string(string buffer) {
            return buffer;
        }

    private:
        string name;
};

#endif MQLSTRING_H