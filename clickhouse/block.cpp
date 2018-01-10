#include "block.h"

#include <stdexcept>

namespace clickhouse {

Block::Iterator::Iterator(const Block& block)
    : block_(block)
    , idx_(0)
{
}

const std::string& Block::Iterator::Name() const {
    return block_.columns_[idx_].name;
}

TypeRef Block::Iterator::Type() const {
    return block_.columns_[idx_].column->Type();
}

ColumnRef Block::Iterator::Column() const {
    return block_.columns_[idx_].column;
}

void Block::Iterator::Next() {
    ++idx_;
}

bool Block::Iterator::IsValid() const {
    return idx_ < block_.columns_.size();
}


Block::Block(size_t cols, size_t rows)
{
    // TODO reserve rows for each column.
    (void)rows;
    columns_.reserve(cols);
}

Block::~Block() = default;

void Block::AppendColumn(const std::string& name, const ColumnRef& col) {
    if ((columns_.size() > 0) &&
            (col->Size() != columns_[0].column->Size())) {
        throw std::runtime_error(
            "all columns in block must have same count of rows, 1st column size "
            + std::to_string(columns_[0].column->Size()) + " != "
            + std::to_string(col->Size()));
    }

    columns_.push_back(ColumnItem{name, col});
}

/// Count of columns in the block.
size_t Block::GetColumnCount() const {
    return columns_.size();
}

const BlockInfo& Block::Info() const {
    return info_;
}

/// Count of rows in the block.
size_t Block::GetRowCount() const {
    if (columns_.empty()) {
        return 0;
    }
    return columns_[0].column->Size();
}

ColumnRef Block::operator [] (size_t idx) const {
    if (idx < columns_.size()) {
        return columns_[idx].column;
    }

    throw std::out_of_range("column index is out of range");
}

void Block::SetColumnName(size_t idx, const std::string& name) {
    columns_[idx].name = name;
}

void Block::Clear() {
    info_.bucket_num = -1;
    info_.is_overflows = 0;

    for (auto& col : columns_) {
        col.name.resize(0);
        col.column->Clear();
    }
}

}
