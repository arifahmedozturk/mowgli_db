#ifndef MQLLIST_H
#define MQLLIST_H

#include "DataType.h"

class MQLList : DataType {
    public:
        MQLList() : DataType("list") {
            this -> name = name;
        }
        
        string convert_from_string(string buffer) {
            return buffer;
        }

    private:
        string name;
};

#endif MQLLIST_H