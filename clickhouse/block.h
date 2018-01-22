#pragma once

#include "columns/column.h"

namespace clickhouse {

class Client;

struct BlockInfo {
    uint8_t is_overflows = 0;
    int32_t bucket_num = -1;
};

class Block {
public:
    /// Allow to iterate over block's columns.
    class Iterator {
    public:
        Iterator(const Block& block);

        /// Name of column.
        const std::string& Name() const;

        /// Type of column.
        TypeRef Type() const;

        /// Reference to column object.
        ColumnRef Column() const;

        /// Move to next column.
        void Next();

        /// Is the iterator still valid.
        bool IsValid() const;

    private:
        Iterator() = delete;

        const Block& block_;
        size_t idx_;
    };

public:
     Block() = default;
     Block(size_t cols);
    ~Block();

    /// Append named column to the block.
    void AppendColumn(const std::string& name, const ColumnRef& col);

    /// Count of columns in the block.
    size_t GetColumnCount() const;

    const BlockInfo& Info() const;

    /// Count of rows in the block.
    size_t GetRowCount() const;

    const std::string& GetColumnName(size_t idx) const {
        return columns_.at(idx).name;
    }

    /// Reference to column by index in the block.
    ColumnRef operator [] (size_t idx) const;

    /// Removes data in all columns, ready for select query or append data.
    void Clear();

    /// Reserve memory to hold rows of data. Only works for already added
    /// columns, thus mainly useful for using Block for insert.
    void ReserveRows(size_t rows);

private:
    friend class Client;

    void SetColumnName(size_t idx, const std::string& name);

    struct ColumnItem {
        std::string name;
        ColumnRef   column;
    };

    BlockInfo info_;
    std::vector<ColumnItem> columns_;
};

}
