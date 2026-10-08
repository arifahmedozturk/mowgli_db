#pragma once
// Machine-readable benchmark output: one "name,value,unit" row per metric.

#include <algorithm>
#include <cmath>
#include <fstream>
#include <string>
#include <vector>

// Nearest-rank percentile (q in [0,1]) of samples; reorders samples.
inline double percentile(std::vector<double>& samples, double q) {
    if (samples.empty()) return 0;
    size_t k = static_cast<size_t>(std::ceil(q * samples.size()));
    k = std::clamp<size_t>(k, 1, samples.size()) - 1;
    std::nth_element(samples.begin(), samples.begin() + k, samples.end());
    return samples[k];
}

// Per-operation latency samples in seconds.
struct Latencies {
    std::vector<double> samples;
    void   record(double secs) { samples.push_back(secs); }
    double p_us(double q)      { return percentile(samples, q) * 1e6; }
};

class Metrics {
public:
    void add(const std::string& name, double value, const std::string& unit) {
        rows_.push_back({name, value, unit});
    }

    // Adds <name>_p50/_p99/_p999 rows in microseconds.
    void add_latency(const std::string& name, Latencies& l) {
        if (l.samples.empty()) return;
        add(name + "_p50",  l.p_us(0.50),  "us");
        add(name + "_p99",  l.p_us(0.99),  "us");
        add(name + "_p999", l.p_us(0.999), "us");
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
