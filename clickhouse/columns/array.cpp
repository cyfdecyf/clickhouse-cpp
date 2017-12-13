#include "array.h"

#include <stdexcept>

namespace clickhouse {

ColumnArray::ColumnArray(ColumnRef data)
    : Column(Type::CreateArray(data->Type()))
    , data_(data)
    , offsets_(std::make_shared<ColumnUInt64>())
{
}

void ColumnArray::AppendAsColumn(ColumnRef array) {
    if (!data_->Type()->IsEqual(array->Type())) {
        throw std::runtime_error(
            "can't append column of type " + array->Type()->GetName() + " "
            "to column type " + data_->Type()->GetName());
    }

    if (offsets_->Size() == 0) {
        offsets_->Append(array->Size());
    } else {
        offsets_->Append((*offsets_)[offsets_->Size() - 1] + array->Size());
    }

    data_->Append(array);
}

ColumnRef ColumnArray::GetAsColumn(size_t n) const {
    return data_->Slice(GetOffset(n), GetSize(n));
}

void ColumnArray::Append(ColumnRef column) {
    if (auto col = column->As<ColumnArray>()) {
        if (!col->data_->Type()->IsEqual(data_->Type())) {
            return;
        }

        for (size_t i = 0; i < col->Size(); ++i) {
            AppendAsColumn(col->GetAsColumn(i));
        }
    }
}

bool ColumnArray::Load(CodedInputStream* input, size_t rows) {
    size_t old_size = offsets_->Size();
    if (!offsets_->Load(input, rows)) {
        return false;
    }

    size_t new_size = offsets_->Size();
    size_t load_size = (*offsets_)[new_size - 1]; // Number of elements loaded.
    if (!data_->Load(input, load_size)) {
        return false;
    }

    // If there are previously loaded offsets, adjust newly added offsets.
    if (old_size > 0) {
        size_t adjust_offset = (*offsets_)[old_size - 1];
        uint64_t* off = const_cast<uint64_t*>(&(*offsets_)[old_size]);
        for (auto i = old_size; i < new_size; ++i, ++off) {
            *off += adjust_offset;
        }
    }
    return true;
}

void ColumnArray::Save(CodedOutputStream* output) {
    offsets_->Save(output);
    data_->Save(output);
}

size_t ColumnArray::Size() const {
    return offsets_->Size();
}

size_t ColumnArray::GetOffset(size_t n) const {
    return (n == 0) ? 0 : (*offsets_)[n - 1];
}

size_t ColumnArray::GetSize(size_t n) const {
    return (n == 0) ? (*offsets_)[n] : ((*offsets_)[n] - (*offsets_)[n - 1]);
}

void ColumnArray::Clear() {
    offsets_->Clear();
    data_->Clear();
}

void ColumnArray::ReserveRows(size_t rows) {
    // Assume each array have 2 elements.
    const size_t N_ELEMENT = 2;
    offsets_->ReserveRows(rows);
    data_->ReserveRows(rows * N_ELEMENT);
}

}
