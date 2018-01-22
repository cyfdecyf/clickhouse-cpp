#include "string.h"

#include <stdexcept>

#include "utils.h"

#include "../base/wire_format.h"

namespace clickhouse {

ColumnFixedString::ColumnFixedString(size_t n)
    : Column(Type::CreateString(n))
    , string_size_(n)
{
}

void ColumnFixedString::Append(const std::string& str) {
    if (size_ >= data_.size()) {
        if (data_.size() == 0) {
            data_.resize(8);
        } else {
            data_.resize(2 * data_.size());
        }
    }
    data_[size_] = str;
    data_[size_].resize(string_size_);
    ++size_;
}

const std::string& ColumnFixedString::At(size_t n) const {
    if (n >= size_) {
        throw std::out_of_range(
                "ColumnFixedString idx:" + std::to_string(n) +
                " size:" + std::to_string(size_));
    }
    return data_[n];
}

const std::string& ColumnFixedString::operator [] (size_t n) const {
    return data_[n];
}

void ColumnFixedString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnFixedString>()) {
        if (string_size_ == col->string_size_) {
            data_.insert(data_.end(), col->data_.begin(), col->data_.end());
            size_ += col->Size();
        }
    }
}

bool ColumnFixedString::Load(CodedInputStream* input, size_t rows) {
    data_.resize(size_ + rows);

    for (size_t i = 0; i < rows; ++i) {
        std::string& s = data_[size_ + i];
        s.resize(string_size_);

        if (!WireFormat::ReadBytes(input, &s[0], s.size())) {
            size_ += i;
            return false;
        }
    }

    size_ += rows;
    return true;
}

void ColumnFixedString::Save(CodedOutputStream* output) {
    for (size_t i = 0; i < size_; ++i) {
        WireFormat::WriteBytes(output, data_[i].data(), string_size_);
    }
}

size_t ColumnFixedString::Size() const {
    return size_;
}

ColumnRef ColumnFixedString::Slice(size_t begin, size_t len) {
    auto result = std::make_shared<ColumnFixedString>(string_size_);

    if (begin < size_) {
        result->data_ = SliceVector(data_, begin, len);
    }

    return result;
}

void ColumnFixedString::Clear() {
    size_ = 0;
    for (auto& s : data_) {
        s.resize(0);
    }
}

void ColumnFixedString::ReserveRows(size_t rows) {
    data_.reserve(rows);
}

ColumnString::ColumnString()
    : Column(Type::CreateString())
{
}

ColumnString::ColumnString(const std::vector<std::string>& data)
    : Column(Type::CreateString())
    , data_(data), size_(data.size())
{
}

void ColumnString::Append(const std::string& str) {
    if (size_ >= data_.size()) {
        if (data_.size() == 0) {
            data_.resize(8);
        } else {
            data_.resize(2 * data_.size());
        }
    }
    data_[size_] = str;
    ++size_;
}

const std::string& ColumnString::At(size_t n) const {
    if (n >= size_) {
        throw std::out_of_range(
                "ColumnString idx:" + std::to_string(n) +
                " size:" + std::to_string(size_));
    }
    return data_[n];
}

const std::string& ColumnString::operator [] (size_t n) const {
    return data_[n];
}

void ColumnString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnString>()) {
        data_.insert(data_.end(), col->data_.begin(), col->data_.end());
        size_ += col->Size();
    }
}

bool ColumnString::Load(CodedInputStream* input, size_t rows) {
    data_.resize(size_ + rows);

    for (size_t i = 0; i < rows; ++i) {
        std::string& s = data_[size_ + i];

        if (!WireFormat::ReadString(input, &s)) {
            size_ += i;
            return false;
        }
    }

    size_ += rows;
    return true;
}

void ColumnString::Save(CodedOutputStream* output) {
    size_t i = 0;
    for (auto si = data_.begin(); i < size_; ++si, ++i) {
        WireFormat::WriteString(output, *si);
    }
}

size_t ColumnString::Size() const {
    return size_;
}

ColumnRef ColumnString::Slice(size_t begin, size_t len) {
    return std::make_shared<ColumnString>(SliceVector(data_, begin, len));
}

void ColumnString::Clear() {
    for (auto& s : data_) {
        s.resize(0);
    }
    // We want to avoid release all allocated memory here while being able to
    // load data into correct place, so we maintain size by ourself instead of
    // relying on data_.size().
    size_ = 0;
}

void ColumnString::ReserveRows(size_t rows) {
    data_.reserve(rows);
}

}
