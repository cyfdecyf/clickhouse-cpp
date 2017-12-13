#include <clickhouse/client.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

// Use value-parameterized tests to run same tests with different client
// options.
class ClientCase : public testing::TestWithParam<ClientOptions> {
protected:
    void SetUp() override {
        client_ = new Client(GetParam());
        client_->Execute("DROP DATABASE IF EXISTS test");
        client_->Execute("CREATE DATABASE test");
    }

    void TearDown() override {
        delete client_;
    }

    Client* client_ = nullptr;

    static const int REUSE_BLOCK_CNT = 3;
};

TEST_P(ClientCase, Array) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.array (arr Array(UInt64)) "
            "ENGINE = Memory");

    /// Insert some values.
    {
        auto arr = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        arr->AppendAsColumn(id);

        id->Append(3);
        arr->AppendAsColumn(id);

        id->Append(7);
        arr->AppendAsColumn(id);

        id->Append(9);

        arr->AppendAsColumn(id);

        Block b;
        b.AppendColumn("arr", arr);
        client_->Insert("test.array", b);
    }

    const uint64_t ARR_SIZE[] = { 1, 2, 3, 4 };
    const uint64_t VALUE[] = { 1, 3, 7, 9 };
    const size_t NUM_ROW = 4;

    /// Test with callback select.
    size_t row = 0;
    client_->Select("SELECT arr FROM test.array",
            [=, &row](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_LE(row + block.GetRowCount(), NUM_ROW);
            ASSERT_EQ(1U, block.GetColumnCount());
            EXPECT_EQ("arr", block.GetColumnName(0));

            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                auto arr = block[0]->As<ColumnArray>();
                auto col = arr->GetAsColumn(c)->As<ColumnUInt64>();

                EXPECT_EQ(ARR_SIZE[row], col->Size());
                for (size_t i = 0; i < col->Size(); ++i) {
                    EXPECT_EQ(VALUE[i], (*col)[i]);
                }
            }
        }
    );
    EXPECT_EQ(NUM_ROW, row);

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT arr FROM test.array", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(1U, block.GetColumnCount());
        EXPECT_EQ("arr", block.GetColumnName(0));

        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            EXPECT_EQ(ARR_SIZE[c], block[0]->GetSize(c));

            auto p = static_cast<const uint64_t*>(block[0]->Data(c));
            for (size_t i = 0; i < ARR_SIZE[c]; ++i) {
                EXPECT_EQ(VALUE[i], *p++);
            }
        }
    }
}

TEST_P(ClientCase, LargeArray) {
    /// Test Array offset adjusting when selecting large array without callback.

    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.largearray (arr Array(UInt64)) "
            "ENGINE = Memory");

    const size_t ARR_SIZE[] = { 10000, 50000, 100, 10000, 10 };
    constexpr size_t NUM_ROW = sizeof(ARR_SIZE) / sizeof(ARR_SIZE[0]);

    uint64_t val = 0;
    /// Insert some values.
    for (size_t r = 0; r < NUM_ROW; ++r) {
        auto id = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < ARR_SIZE[r]; ++i, ++val)  {
            id->Append(val);
        }

        auto arr = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
        arr->AppendAsColumn(id);

        Block b;
        b.AppendColumn("arr", arr);
        client_->Insert("test.largearray", b);
    }

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT arr FROM test.largearray", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(1U, block.GetColumnCount());
        EXPECT_EQ("arr", block.GetColumnName(0));

        val = 0;
        for (size_t r = 0; r < NUM_ROW; ++r) {
            auto arr = block[0]->As<ColumnArray>();
            ASSERT_EQ(ARR_SIZE[r], arr->GetSize(r));

            auto p = static_cast<const uint64_t*>(arr->Data(r));
            for (size_t i = 0; i < ARR_SIZE[r]; ++i, ++val) {
                EXPECT_EQ(val, *p++);
            }
        }
    }
}

TEST_P(ClientCase, DateTime) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.datetime (d DateTime) "
            "ENGINE = Memory");

    const std::time_t now = std::time(nullptr);
    const struct {
        std::time_t d;
    } TEST_DATA[] = {
        { now - 2 * 86400 },
        { now - 1 * 86400 },
        { now + 1 * 86400 },
        { now + 2 * 86400 },
    };
    constexpr size_t NUM_ROW = sizeof(TEST_DATA) / sizeof(TEST_DATA[0]);
    {
        Block b;
        b.AppendColumn("d", std::make_shared<ColumnDateTime>());
        for (auto const& td : TEST_DATA) {
            b[0]->AppendValue(td.d);
        }
        client_->Insert("test.datetime", b);
    }

    size_t row = 0;
    client_->Select("SELECT d FROM test.datetime", [=, &row](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_EQ(NUM_ROW, block.GetRowCount());
            ASSERT_EQ(1U, block.GetColumnCount());
            EXPECT_EQ("d", block.GetColumnName(0));

            auto col = block[0]->As<ColumnDateTime>();
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                EXPECT_EQ(TEST_DATA[row].d, col->At(c));
            }
        }
    );

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT d FROM test.datetime", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(1U, block.GetColumnCount());

        auto col = block[0]->As<ColumnDateTime>();
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            EXPECT_EQ(TEST_DATA[c].d, col->At(c));
        }
    }
}

TEST_P(ClientCase, String) {
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.string (id UInt64, name String) "
            "ENGINE = Memory");

    const struct {
        uint64_t id;
        std::string name;
    } TEST_DATA[] = {
        { 1, "id" },
        { 3, "foo" },
        { 5, "bar" },
        { 7, "name" },
    };
    constexpr size_t NUM_ROW = sizeof(TEST_DATA) / sizeof(TEST_DATA[0]);

    /// Insert some values.
    {
        Block block;
        block.AppendColumn("id", std::make_shared<ColumnUInt64>());
        block.AppendColumn("name", std::make_shared<ColumnString>());

        for (auto const& td : TEST_DATA) {
            block[0]->AppendValue(td.id);
            block[1]->AppendValue(td.name);
        }

        client_->Insert("test.string", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, name FROM test.string", [=, &row](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_LE(row + block.GetRowCount(), NUM_ROW);
            ASSERT_EQ(2U, block.GetColumnCount());
            EXPECT_EQ("id", block.GetColumnName(0));
            EXPECT_EQ("name", block.GetColumnName(1));

            auto id = block[0]->As<ColumnUInt64>();
            auto name = block[1]->As<ColumnString>();
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                EXPECT_EQ(TEST_DATA[row].id, (*id)[c]);
                EXPECT_EQ(TEST_DATA[row].name, (*name)[c]);
            }
        }
    );
    EXPECT_EQ(NUM_ROW, row);

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT id, name FROM test.string", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(2U, block.GetColumnCount());
        EXPECT_EQ("id", block.GetColumnName(0));
        EXPECT_EQ("name", block.GetColumnName(1));

        auto id = static_cast<const uint64_t*>(block[0]->Data());
        auto name = static_cast<const std::string*>(block[1]->Data());
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            EXPECT_EQ(TEST_DATA[c].id, *id++);
            EXPECT_EQ(TEST_DATA[c].name, *name++);

            /// Test Column::Value.
            EXPECT_EQ(TEST_DATA[c].id, block[0]->Value<uint64_t>(c));
            EXPECT_EQ(TEST_DATA[c].name, block[1]->Value<std::string>(c));
        }
    }
}

TEST_P(ClientCase, FixedString) {
    const size_t FIXED_STR_LEN = 4;

    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.fixedstring (id UInt64, name FixedString(4)) "
            "ENGINE = Memory");

    const struct {
        uint64_t id;
        std::string name;
    } TEST_DATA[] = {
        { 1, "id" },
        { 3, "foo" },
        { 5, "bar" },
        { 7, "name" },
        { 9, "name___" },  // Character after fixed length should be dropped.
    };
    constexpr size_t NUM_ROW = sizeof(TEST_DATA) / sizeof(TEST_DATA[0]);

    /// Insert some values.
    {
        auto id = std::make_shared<ColumnUInt64>();
        auto name = std::make_shared<ColumnFixedString>(FIXED_STR_LEN);
        for (auto const& td : TEST_DATA) {
            id->Append(td.id);
            name->Append(td.name);
        }

        Block block;
        block.AppendColumn("id"  , id);
        block.AppendColumn("name", name);

        client_->Insert("test.fixedstring", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, name FROM test.fixedstring", [=, &row](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_LE(row + block.GetRowCount(), NUM_ROW);
            ASSERT_EQ(2U, block.GetColumnCount());
            EXPECT_EQ("id", block.GetColumnName(0));
            EXPECT_EQ("name", block.GetColumnName(1));

            auto id = block[0]->As<ColumnUInt64>();
            auto name = block[1]->As<ColumnFixedString>();
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                EXPECT_EQ(TEST_DATA[row].id, (*id)[c]);
                EXPECT_EQ(FIXED_STR_LEN, (*name)[c].size());
                /// Construct new string to drop ending null byte for test.
                EXPECT_EQ(TEST_DATA[row].name.substr(0, FIXED_STR_LEN),
                        std::string((*name)[c].c_str()));
            }
        }
    );
    EXPECT_EQ(NUM_ROW, row);

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT id, name FROM test.fixedstring", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(2U, block.GetColumnCount());
        EXPECT_EQ("id", block.GetColumnName(0));
        EXPECT_EQ("name", block.GetColumnName(1));

        auto id = static_cast<const uint64_t*>(block[0]->Data());
        auto name = static_cast<const char*>(block[1]->Data());
        char sbuf[FIXED_STR_LEN];
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            EXPECT_EQ(TEST_DATA[c].id, *id++);
            memset(sbuf, 0, sizeof(sbuf));
            strncpy(sbuf, TEST_DATA[c].name.c_str(), sizeof(sbuf));
            ASSERT_EQ(
                    std::string(sbuf, FIXED_STR_LEN),
                    std::string(name + c * FIXED_STR_LEN, FIXED_STR_LEN));
        }
    }
}

TEST_P(ClientCase, Nullable) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.nullable (id Nullable(UInt64), date Nullable(Date)) "
            "ENGINE = Memory");

    // Round std::time_t to start of date.
    const std::time_t cur_date = std::time(nullptr) / 86400 * 86400;
    const struct {
        uint64_t id;
        uint8_t id_null;
        std::time_t date;
        uint8_t date_null;
    } TEST_DATA[] = {
        { 1, 0, cur_date - 2 * 86400, 0 },
        { 2, 0, cur_date - 1 * 86400, 1 },
        { 3, 1, cur_date + 1 * 86400, 0 },
        { 4, 1, cur_date + 2 * 86400, 1 },
    };
    constexpr size_t NUM_ROW = sizeof(TEST_DATA) / sizeof(TEST_DATA[0]);

    /// Insert some values.
    {
        Block block;

        {
            auto id = std::make_shared<ColumnUInt64>();
            auto nulls = std::make_shared<ColumnUInt8>();
            for (auto const& td : TEST_DATA) {
                id->AppendValue(td.id);
                nulls->AppendValue(td.id_null);
            }
            block.AppendColumn("id", std::make_shared<ColumnNullable>(id, nulls));
        }
        {
            auto date = std::make_shared<ColumnDate>();
            auto nulls = std::make_shared<ColumnUInt8>();
            for (auto const& td : TEST_DATA) {
                date->AppendValue(td.date);
                nulls->Append(td.date_null);
            }
            block.AppendColumn("date", std::make_shared<ColumnNullable>(date, nulls));
        }

        client_->Insert("test.nullable", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, date FROM test.nullable",
            [=, &row](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }

            ASSERT_EQ(2U, block.GetColumnCount());

            auto col_id   = block[0]->As<ColumnNullable>();
            auto col_date = block[1]->As<ColumnNullable>();
            auto id = col_id->Nested()->As<ColumnUInt64>();
            auto date = col_date->Nested()->As<ColumnDate>();

            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                EXPECT_EQ(TEST_DATA[row].id_null, col_id->IsNull(c));
                EXPECT_EQ(TEST_DATA[row].date_null, col_date->IsNull(c));

                if (!col_id->IsNull(c)) {
                    EXPECT_EQ(TEST_DATA[row].id, id->At(c));
                }
                if (!col_date->IsNull(c)) {
                    EXPECT_EQ(TEST_DATA[row].date, date->At(c));
                }
            }
        }
    );
    EXPECT_EQ(NUM_ROW, row);

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT id, date FROM test.nullable", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(2U, block.GetColumnCount());

        auto col_id   = block[0]->As<ColumnNullable>();
        auto col_date = block[1]->As<ColumnNullable>();
        auto date = col_date->Nested()->As<ColumnDate>();

        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            EXPECT_EQ(static_cast<bool>(TEST_DATA[c].id_null), col_id->IsNull(c));
            EXPECT_EQ(static_cast<bool>(TEST_DATA[c].date_null),
                    col_date->IsNull(c));

            if (!col_id->IsNull(c)) {
                EXPECT_EQ(TEST_DATA[c].id, col_id->Value<uint64_t>(c));
            }
            if (!col_date->IsNull(c)) {
                // Because date column type is Date instead of
                // DateTime, round to start second of date for test.
                EXPECT_EQ(TEST_DATA[c].date, date->At(c));
            }
        }
    }
}

TEST_P(ClientCase, Numbers) {
    const size_t NUM_ROW = 100000;
    size_t num = 0;
    client_->Select("SELECT number, number FROM system.numbers LIMIT 100000", [&num](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }
            auto col = block[0]->As<ColumnUInt64>();

            for (size_t i = 0; i < col->Size(); ++i, ++num) {
                EXPECT_EQ(num, col->At(i));
            }
        }
    );
    EXPECT_EQ(NUM_ROW, num);

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT number, number FROM system.numbers LIMIT 100000", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(2U, block.GetColumnCount());
        auto n = static_cast<const uint64_t*>(block[0]->Data());
        auto n2 = static_cast<const uint64_t*>(block[1]->Data());
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            EXPECT_EQ(c, *n++);
            EXPECT_EQ(c, *n2++);
        }
    }
}

TEST_P(ClientCase, Cancelable) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.cancel (x UInt64) "
            "ENGINE = Memory");

    /// Insert a few blocks.
    /// The following setup only makes cancel effective for compression enabled
    /// client.
    const int NUM_BLOCK = 10;
    const int NUM_ROW_EACH_BLOCK = 500000;
    for (unsigned j = 0; j < NUM_BLOCK; j++) {
        Block b;

        auto x = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < NUM_ROW_EACH_BLOCK; i++) {
            x->Append(i);
        }

        b.AppendColumn("x", x);
        client_->Insert("test.cancel", b);
    }

    /// Send a query which is canceled after receiving the first blockr.
    int row_cnt = 0;
    EXPECT_NO_THROW(
        client_->SelectCancelable("SELECT * FROM test.cancel",
            [&row_cnt](const Block& block)
            {
                row_cnt += block.GetRowCount();
                return false;
            }
        );
    );
    EXPECT_LE(row_cnt, NUM_BLOCK * NUM_ROW_EACH_BLOCK);
}

TEST_P(ClientCase, Exception) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.exceptions (id UInt64, name String) "
            "ENGINE = Memory");

    /// Expect failing on table creation.
    EXPECT_THROW(
        client_->Execute(
            "CREATE TABLE test.exceptions (id UInt64, name String) "
            "ENGINE = Memory"),
        ServerException);
}

TEST_P(ClientCase, Enum) {
    /// Create a table.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.enums (id UInt64, e Enum8('One' = 1, 'Two' = 2)) "
            "ENGINE = Memory");

    const struct {
        uint64_t id;
        int8_t eval;
        std::string ename;
    } TEST_DATA[] = {
        { 1, 1, "One" },
        { 2, 2, "Two" },
        { 3, 2, "Two" },
        { 4, 1, "One", },
    };
    constexpr size_t NUM_ROW = sizeof(TEST_DATA) / sizeof(TEST_DATA[0]);

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        auto e = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"One", 1}, {"Two", 2}}));

        int i = 0;
        for (auto const& td : TEST_DATA) {
            id->Append(td.id);
            if (++i % 2) {
                e->Append(td.eval);
            } else {
                e->Append(td.ename);
            }
        }

        block.AppendColumn("id", id);
        block.AppendColumn("e", e);

        client_->Insert("test.enums", block);
    }

    /// Select values inserted in the previous step.
    size_t row = 0;
    client_->Select("SELECT id, e FROM test.enums", [&row, TEST_DATA](const Block& block)
        {
            if (block.GetRowCount() == 0) {
                return;
            }

            EXPECT_EQ("id", block.GetColumnName(0));
            EXPECT_EQ("e", block.GetColumnName(1));

            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                EXPECT_EQ(TEST_DATA[row].id, (*block[0]->As<ColumnUInt64>())[c]);
                EXPECT_EQ(TEST_DATA[row].eval, (*block[1]->As<ColumnEnum8>()).At(c));
                EXPECT_EQ(TEST_DATA[row].ename, (*block[1]->As<ColumnEnum8>()).NameAt(c));
            }
        }
    );
    EXPECT_EQ(NUM_ROW, row);

    /// Test with non-callback select with block reuse.
    Block block;
    for (int t = 0; t < REUSE_BLOCK_CNT; ++t) {
        client_->Select("SELECT id, e FROM test.enums", &block);

        ASSERT_EQ(NUM_ROW, block.GetRowCount());
        ASSERT_EQ(2U, block.GetColumnCount());
        EXPECT_EQ("id", block.GetColumnName(0));
        EXPECT_EQ("e", block.GetColumnName(1));

        auto id = static_cast<const uint64_t*>(block[0]->Data());
        auto eval = static_cast<const int8_t*>(block[1]->Data());
        auto ename = block[1]->As<ColumnEnum8>();
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            EXPECT_EQ(TEST_DATA[c].id, *id++);
            EXPECT_EQ(TEST_DATA[c].eval, *eval++);
            EXPECT_EQ(TEST_DATA[c].ename, ename->NameAt(c));
        }
    }
}

TEST_P(ClientCase, Insert) {
    /// Test reuse Block with Bolock::Clear and Column::AppendValue.
    client_->Execute(
            "CREATE TABLE IF NOT EXISTS test.insert (d DateTime, i32 Int32, u64 UInt64, fs FixedString(4), s String) "
            "ENGINE = Memory");

    const struct {
        std::time_t t;
        int32_t i32;
        uint64_t u64;
        std::string fs;
        std::string s;
    } TEST_DATA[] = {
        { std::time(nullptr) + 0, -1, 1, "One", "Hello" },
        { std::time(nullptr) + 1,  0, 2, "Two", "Hello" },
        { std::time(nullptr) + 2,  1, 3, "Three", "Hello" },
        { std::time(nullptr) + 3,  2, 4, "Four", "Hello" },
        { std::time(nullptr) + 4,  3, 5, "One", "Hello" },
    };
    constexpr size_t NUM_ROW = sizeof(TEST_DATA) / sizeof(TEST_DATA[0]);
    const size_t FIXED_STR_LEN = 4;

    /// Reuse same block to insert some value.
    const int REPEAT_CNT = 5;
    Block block;
    block.AppendColumn("d", std::make_shared<ColumnDateTime>());
    block.AppendColumn("i32", std::make_shared<ColumnInt32>());
    block.AppendColumn("u64", std::make_shared<ColumnUInt64>());
    block.AppendColumn("fs", std::make_shared<ColumnFixedString>(FIXED_STR_LEN));
    block.AppendColumn("s", std::make_shared<ColumnString>());
    for (int i = 0; i < REPEAT_CNT; ++i) {
        block.Clear();

        for (auto const& td : TEST_DATA) {
            block[0]->AppendValue(td.t);
            block[1]->AppendValue(td.i32);
            block[2]->AppendValue(td.u64);
            block[3]->AppendValue(
                    std::string(td.fs.c_str(), FIXED_STR_LEN).c_str());
            block[4]->AppendValue(td.s);
        }

        client_->Insert("test.insert", block);
    }

    block.ReserveRows(NUM_ROW * REPEAT_CNT);

    client_->Select("SELECT * FROM test.insert", &block);
    ASSERT_EQ(NUM_ROW * REPEAT_CNT, block.GetRowCount());
    ASSERT_EQ(5U, block.GetColumnCount());
    for (int i = 0; i < REPEAT_CNT; ++i) {
        for (size_t c = 0; c < NUM_ROW; ++c) {
            size_t idx = i * NUM_ROW + c;
            auto const& td = TEST_DATA[c];
            EXPECT_EQ(td.t, block[0]->As<ColumnDateTime>()->At(idx));
            EXPECT_EQ(td.i32, block[1]->Value<int32_t>(idx));
            EXPECT_EQ(td.u64, block[2]->Value<uint64_t>(idx));
            EXPECT_EQ(
                    std::string(td.fs.c_str(), FIXED_STR_LEN),
                    std::string(static_cast<const char*>(block[3]->Data(idx)),
                        FIXED_STR_LEN));
            ASSERT_EQ(td.s, std::string(block[4]->Value<std::string>(idx)));
        }
    }
}

INSTANTIATE_TEST_CASE_P(
    Client, ClientCase,
    ::testing::Values(
        ClientOptions()
            .SetHost("localhost")
            .SetPingBeforeQuery(true),
        ClientOptions()
            .SetHost("localhost")
            .SetPingBeforeQuery(false)
            .SetCompressionMethod(CompressionMethod::LZ4)
    ));

