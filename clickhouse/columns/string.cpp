#include "string.h"

#include <stdexcept>

#include "utils.h"

#include "../base/wire_format.h"

namespace clickhouse {

static const size_t INITIAL_CAPACITY = 8;

ColumnFixedString::ColumnFixedString(size_t n)
    : Column(Type::CreateString(n))
    , string_size_(n)
    , data_(INITIAL_CAPACITY * n)
{
}

void ColumnFixedString::EnlargeBuffer(size_t new_size) {
    size_t need_bytes = new_size * string_size_;
    while (need_bytes > data_.size()) {
        data_.resize(data_.size() * 2, '\0');
    }
}

void ColumnFixedString::Append(const std::string& str) {
    size_t new_size = size_ + 1;
    EnlargeBuffer(new_size);

    size_t copy_size =
        (str.size() >= string_size_) ? string_size_ : str.size();
    memcpy(&data_[size_ * string_size_], str.c_str(), copy_size);
    size_ = new_size;
}

void ColumnFixedString::AppendData(const void* v, size_t n) {
    size_t new_size = size_ + n;
    EnlargeBuffer(new_size);

    memcpy(&data_[size_ * string_size_], v, n * string_size_);
    size_ = new_size;
}

std::string ColumnFixedString::At(size_t n) const {
    if (n >= size_) {
        throw std::out_of_range(
                "ColumnFixedString idx:" + std::to_string(n) +
                " size:" + std::to_string(size_));
    }
    return std::string(&data_[n * string_size_], string_size_);
}

std::string ColumnFixedString::operator [] (size_t n) const {
    return std::string(&data_[n * string_size_], string_size_);
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
    data_.resize((size_  + rows) * string_size_, '\0');

    if (!WireFormat::ReadBytes(input,
                &data_[size_ * string_size_], rows * string_size_)) {
        return false;
    }

    size_ += rows;
    return true;
}

void ColumnFixedString::Save(CodedOutputStream* output) {
    WireFormat::WriteBytes(output, data_.data(), size_ * string_size_);
}

size_t ColumnFixedString::Size() const {
    return size_;
}

ColumnRef ColumnFixedString::Slice(size_t begin, size_t len) {
    auto result = std::make_shared<ColumnFixedString>(string_size_);

    if (begin < size_) {
        result->data_ = SliceVector(data_,
                begin * string_size_, len * string_size_);
    }

    return result;
}

void ColumnFixedString::Clear() {
    size_ = 0;
    memset(data_.data(), 0, data_.size());
}

void ColumnFixedString::ReserveRows(size_t rows) {
    data_.reserve(rows * string_size_);
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

void ColumnString::AppendData(const void* v, size_t n) {
    auto vv = static_cast<const std::string*>(v);
    for (size_t i = 0; i < n; i++) {
        Append(*(vv + i));
    }
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
