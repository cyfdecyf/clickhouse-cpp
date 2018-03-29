#pragma once

#include "numeric.h"

#include <ctime>

namespace clickhouse {

/** */
class ColumnDate : public Column {
public:
    ColumnDate();

    /// Appends one element to the end of column.
    void Append(const std::time_t& value);

    /// Returns element at given row number.
    std::time_t At(size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Appends n elements to the end of column.
    void AppendData(const void* v, size_t n = 1) override;

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    /// The underlying data type is uint16 thus can't cast and access like an
    /// std::time_t array.
    const void* Data(size_t n) const override {
        return data_->Data(n);
    }

    /// Removes all data, ready for Load/Append.
    void Clear() override;

    /// Reserve memory to hold data.
    void ReserveRows(size_t rows) override;

private:
    std::shared_ptr<ColumnUInt16> data_;
};

/** */
class ColumnDateTime : public Column {
public:
    ColumnDateTime();

    /// Appends one element to the end of column.
    void Append(const std::time_t& value);

    /// Returns element at given row number.
    std::time_t At(size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Appends n elements to the end of column.
    void AppendData(const void* v, size_t n = 1) override;

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    /// The underlying data type is uint32 thus can't cast and access like an
    /// std::time_t array.
    const void* Data(size_t n) const override {
        return data_->Data(n);
    }

    /// Removes all data, ready for Load/Append.
    void Clear() override;

    /// Reserve memory to hold data.
    void ReserveRows(size_t rows) override;

private:
    std::shared_ptr<ColumnUInt32> data_;
};

}
