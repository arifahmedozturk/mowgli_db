#pragma once
// A row read the way bench_btree reads LMDB and SQLite: each column copied
// into a reused buffer, rather than built into a Row (#64, #116).
#include "catalog/row_view.h"
#include <string>
#include <vector>

inline void copy_out(const RowView& row, std::vector<std::string>& cells) {
    cells.resize(row.columns());
    for (size_t i = 0; i < row.columns(); i++) {
        auto [p, n] = row.cell(i);
        cells[i].assign(reinterpret_cast<const char*>(p), n);
    }
}
