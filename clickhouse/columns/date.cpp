#include "date.h"

namespace clickhouse {

ColumnDate::ColumnDate()
    : Column(Type::CreateDate())
    , data_(std::make_shared<ColumnUInt16>())
{
}

void ColumnDate::Append(const std::time_t& value) {
    data_->Append(static_cast<uint16_t>(value / 86400));
}

void ColumnDate::AppendData(const void* v, size_t n) {
    auto vv = static_cast<const std::time_t*>(v);
    for (size_t i = 0; i < n; i++) {
        Append(*(vv + i));
    }
}

std::time_t ColumnDate::At(size_t n) const {
    return data_->At(n) * 86400;
}

void ColumnDate::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDate>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDate::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnDate::Save(CodedOutputStream* output) {
    data_->Save(output);
}

size_t ColumnDate::Size() const {
    return data_->Size();
}

ColumnRef ColumnDate::Slice(size_t begin, size_t len) {
    auto col = data_->Slice(begin, len)->As<ColumnUInt16>();
    auto result = std::make_shared<ColumnDate>();

    result->data_->Append(col);

    return result;
}

void ColumnDate::Clear() {
    data_->Clear();
}

void ColumnDate::ReserveRows(size_t rows) {
    data_->ReserveRows(rows);
}

ColumnDateTime::ColumnDateTime()
    : Column(Type::CreateDateTime())
    , data_(std::make_shared<ColumnUInt32>())
{
}

void ColumnDateTime::Append(const std::time_t& value) {
    data_->Append(static_cast<uint32_t>(value));
}

void ColumnDateTime::AppendData(const void* v, size_t n) {
    auto vv = static_cast<const std::time_t*>(v);
    for (size_t i = 0; i < n; i++) {
        Append(*(vv + i));
    }
}

std::time_t ColumnDateTime::At(size_t n) const {
    return data_->At(n);
}

void ColumnDateTime::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDateTime>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDateTime::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnDateTime::Save(CodedOutputStream* output) {
    data_->Save(output);
}

size_t ColumnDateTime::Size() const {
    return data_->Size();
}

ColumnRef ColumnDateTime::Slice(size_t begin, size_t len) {
    auto col = data_->Slice(begin, len)->As<ColumnUInt32>();
    auto result = std::make_shared<ColumnDateTime>();

    result->data_->Append(col);

    return result;
}

void ColumnDateTime::Clear() {
    data_->Clear();
}

void ColumnDateTime::ReserveRows(size_t rows) {
    data_->ReserveRows(rows);
}

}
