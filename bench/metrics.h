#pragma once
// Machine-readable benchmark output: one "name,value,unit" row per metric.

#include <fstream>
#include <string>
#include <vector>

class Metrics {
public:
    void add(const std::string& name, double value, const std::string& unit) {
        rows_.push_back({name, value, unit});
    }

    void write_csv(const std::string& path) const {
        if (path.empty()) return;
        std::ofstream f(path);
        f << "name,value,unit\n";
        f.precision(17);
        for (const auto& r : rows_) f << r.name << "," << r.value << "," << r.unit << "\n";
    }

private:
    struct Row { std::string name; double value; std::string unit; };
    std::vector<Row> rows_;
};
