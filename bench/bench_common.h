#pragma once
// Timing helpers shared by the benchmark programs.
#include <chrono>
#include <cstddef>
#include <cstdint>

using Clock = std::chrono::steady_clock;
using Dur   = std::chrono::duration<double>;

inline double ops_per_sec(size_t n, double secs) { return secs > 0 ? n / secs : 0; }
inline double per_key(uint64_t bytes, size_t n)   { return n ? double(bytes) / n : 0; }
