#ifndef DATATYPE_H
#define DATATYPE_H

#include <string>

using namespace std;

class DataType {
    public:
        DataType(string name) {
            this -> name = name;
        }

        string get_name() {
            return name;
        }

        virtual string convert_from_string(string buffer) = 0;

    private:
        string name;
};

#endif DATATYPE_H