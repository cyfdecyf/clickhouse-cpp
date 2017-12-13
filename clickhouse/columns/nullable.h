#pragma once

#include "column.h"
#include "numeric.h"

namespace clickhouse {

/**
 * Represents column of Nullable(T).
 */
class ColumnNullable : public Column {
public:
    ColumnNullable(ColumnRef nested, ColumnRef nulls);

    /// Returns null flag at given row number.
    bool IsNull(size_t n) const;

    /// Returns nested column.
    ColumnRef Nested() const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Appends one element to the end of column.
    void AppendData(const void* v, size_t n = 1) override {
        // TODO how to support insert null?
        nulls_->AppendData(0, n);
        nested_->AppendData(v, n);
    }

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    const void* Data(size_t n = 0) const override {
        return nested_->Data(n);
    }

    /// Removes all data, ready for Load/Append.
    void Clear() override;

    /// Reserve memory to hold data.
    void ReserveRows(size_t rows) override;

private:
    ColumnRef nested_;
    std::shared_ptr<ColumnUInt8> nulls_;
};

}
