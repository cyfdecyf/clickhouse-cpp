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


Block::Block()
    : rows_(0)
{
}

Block::Block(size_t cols, size_t rows)
    : rows_(rows)
{
    columns_.reserve(cols);
}

Block::~Block() = default;

void Block::AppendColumn(const std::string& name, const ColumnRef& col) {
    if (columns_.empty()) {
        rows_ = col->Size();
    } else if (col->Size() != rows_) {
        throw std::runtime_error(
            "all clumns in block must have same count of rows"
        );
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
    return rows_;
}

ColumnRef Block::operator [] (size_t idx) const {
    if (idx < columns_.size()) {
        return columns_[idx].column;
    }

    throw std::out_of_range("column index is out of range");
}

void Block::Clear() {
    info_.bucket_num = -1;
    info_.is_overflows = 0;

    rows_ = 0;

    for (auto& col : columns_) {
        col.name = "";
        col.column->Clear();
    }
}

}
