#pragma once

#include "column.h"

namespace clickhouse {

/**
 * Represents column of fixed-length strings.
 */
class ColumnFixedString : public Column {
public:
    explicit ColumnFixedString(size_t n);

    /// Appends one element to the column.
    void Append(const std::string& str);

    /// Returns element at given row number.
    const std::string& At(size_t n) const;

    /// Returns element at given row number.
    const std::string& operator [] (size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Appends one element to the end of column.
    void AppendAddr(const void* v) override {
        Append(*static_cast<const std::string*>(v));
    }

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    const void* Addr(size_t n) const override {
        return &data_[n];
    }

    /// Removes all data, ready for Load/Append.
    void Clear() override;

private:
    const size_t string_size_;
    std::vector<std::string> data_;
    size_t size_ = 0;
};


/**
 * Represents column of variable-length strings.
 */
class ColumnString : public Column {
public:
    ColumnString();
    explicit ColumnString(const std::vector<std::string>& data);

    /// Appends one element to the column.
    void Append(const std::string& str);

    /// Returns element at given row number.
    const std::string& At(size_t n) const;

    /// Returns element at given row number.
    const std::string& operator [] (size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Appends one element to the end of column.
    void AppendAddr(const void* v) override {
        Append(*static_cast<const std::string*>(v));
    }

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    const void* Addr(size_t n) const override {
        return &data_[n];
    }

    /// Removes all data, ready for Load/Append.
    void Clear() override;

private:
    std::vector<std::string> data_;
    size_t size_ = 0;
};

}
