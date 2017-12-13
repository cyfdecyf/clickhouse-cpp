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

    /// Appends n elements to the end of column.
    virtual void AppendData(const void* v, size_t n = 1) {
        (void)v;
        (void)n;
        throw "AppendData not supported";
    }

    template <typename T>
    void AppendValue(const T& v) {
        AppendData(&v);
    }
    /// For ColumnFixedString, must append char* not std::string.
    void AppendValue(const char* v) {
        AppendData(v);
    }
    void AppendValue(char* v) {
        AppendData(v);
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
    virtual const void* Data(size_t n = 0) const {
        (void)n;
        throw "Column::Data() not supported";
    }

    /// Get value of nth row.
    /// XXX ColumnFixedString::Addr returns char*, so do not use this function
    /// with ColumnFixedString.
    template <typename T>
    const T& Value(size_t n) const {
        return *static_cast<const T*>(Data(n));
    }

    /// Return number of elements at nth row.
    /// ColumnArray overrides this.
    virtual size_t GetSize(size_t n) const {
        (void)n;
        return 1;
    }

    /// Removes all data, ready for Load/Append again.
    virtual void Clear() = 0;

    /// Reserve memory to hold data.
    virtual void ReserveRows(size_t rows) = 0;

protected:
    TypeRef type_;
};

}
