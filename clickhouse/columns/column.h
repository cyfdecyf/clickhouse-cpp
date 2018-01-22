#pragma once

#include <string>

#include "../base/input.h"
#include "../base/coded.h"
#include "../types/types.h"

namespace clickhouse {

using ColumnRef = std::shared_ptr<class Column>;

/**
 * An abstract base of all columns classes.
 */
class Column : public std::enable_shared_from_this<Column>
{
public:
    explicit inline Column(TypeRef type)
        : type_(type)
    {
    }

    virtual ~Column()
    { }

    /// Downcast pointer to the specific culumn's subtype.
    template <typename T>
    inline std::shared_ptr<T> As() {
        return std::dynamic_pointer_cast<T>(shared_from_this());
    }

    /// Downcast pointer to the specific culumn's subtype.
    template <typename T>
    inline std::shared_ptr<const T> As() const {
        return std::dynamic_pointer_cast<const T>(shared_from_this());
    }

    /// Get type object of the column.
    inline TypeRef Type() const { return type_; }

    /// Appends content of given column to the end of current one.
    virtual void Append(ColumnRef column) = 0;

    /// Appends one element to the end of column.
    virtual void AppendAddr(const void* v) {
        (void)v;
        throw "AppendAddr not supported";
    }

    template <typename T>
    void AppendValue(const T& v) {
        AppendAddr(&v);
    }

    /// Loads column data from input stream.
    virtual bool Load(CodedInputStream* input, size_t rows) = 0;

    /// Saves column data to output stream.
    virtual void Save(CodedOutputStream* output) = 0;

    /// Returns count of rows in the column.
    virtual size_t Size() const = 0;

    /// Makes slice of the current column.
    virtual ColumnRef Slice(size_t begin, size_t len) = 0;

    /// Get address of nth row.
    /// For ColumnArray, return address of the first element of nth row.
    /// (Avoids datapy copy in ColumnArray::GetAsColumn)
    /// Row data is contiguous thus can be iterated over by pointer
    /// arithemetic.
    virtual const void* Addr(size_t n) const {
        (void)n;
        throw "Addr not supported";
    }

    /// Get value of nth row.
    template <typename T>
    const T& Value(size_t n) const {
        return *static_cast<const T*>(Addr(n));
    }

    /// Removes all data, ready for Load/Append again.
    virtual void Clear() = 0;

    /// Reserve memory to hold data.
    virtual void ReserveRows(size_t rows) = 0;

    /// Return type name used by ClickHouse protocol.
    std::string GetTypeName() {
        return type_->GetName();
    }

protected:
    TypeRef type_;
};

}
