#pragma once

#include "column.h"

namespace clickhouse {


template <typename T>
class ColumnEnum : public Column {
public:
    ColumnEnum(TypeRef type);
    ColumnEnum(TypeRef type, const std::vector<T>& data);

    /// Appends one element to the end of column.
    void Append(const T& value, bool checkValue = false);
    void Append(const std::string& name);

    /// Returns element at given row number.
    const T& At(size_t n) const;
    const std::string NameAt(size_t n) const;

    /// Returns element at given row number.
    const T& operator[] (size_t n) const;

    /// Set element at given row number.
    void SetAt(size_t n, const T& value, bool checkValue = false);
    void SetNameAt(size_t n, const std::string& name);

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Appends n elements to the end of column.
    /// v must point to numeric value array, not string array.
    void AppendData(const void* v, size_t n = 1) override {
        auto vv = static_cast<const T*>(v);
        data_.insert(data_.end(), vv, vv + n);
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
        return &data_[n];
    }

    /// Removes all data, ready for Load/Append.
    void Clear() override;

    /// Reserve memory to hold data.
    void ReserveRows(size_t rows) override;

private:
    std::vector<T> data_;
};

using ColumnEnum8 = ColumnEnum<int8_t>;
using ColumnEnum16 = ColumnEnum<int16_t>;

}
