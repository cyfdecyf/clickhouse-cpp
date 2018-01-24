#include <sstream>

#include <pybind11/pybind11.h>
#include <pybind11/chrono.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

#include "clickhouse/client.h"

static_assert(sizeof(std::time_t) == 8,
        "only std::time_t size 8 is supported");

namespace py = pybind11;

namespace clickhouse {

// Helper class to expose ColumnArray element to Python without copy.
struct ArrayElement {
    void* data;
    size_t size;
    TypeRef type;
};

template <typename T>
static inline py::buffer_info _GenBufferInfo(void* data, size_t size) {
    return
        py::buffer_info(
            data,
            sizeof(T),    // Size of one scalar
            py::format_descriptor<T>::format(),
            1,            // Number of dimensions
            { size },     // Buffer dimensions
            { sizeof(T) } // Strides (in bytes) for each index
        );
}

static py::buffer_info GenBufferInfo(
        void* data, size_t size, TypeRef type) {
    switch (type->GetCode()) {
    case Type::Int8:
    case Type::Enum8:
        return _GenBufferInfo<int8_t>(data, size);
    case Type::UInt8:
        return _GenBufferInfo<uint8_t>(data, size);
    case Type::Int16:
    case Type::Enum16:
        return _GenBufferInfo<int16_t>(data, size);
    case Type::UInt16:
    case Type::Date:
        return _GenBufferInfo<uint16_t>(data, size);
    case Type::Int32:
        return _GenBufferInfo<int32_t>(data, size);
    case Type::UInt32:
    case Type::DateTime:
        return _GenBufferInfo<uint32_t>(data, size);
    case Type::Int64:
        return _GenBufferInfo<int64_t>(data, size);
    case Type::UInt64:
        return _GenBufferInfo<uint64_t>(data, size);
    case Type::Float32:
        return _GenBufferInfo<float>(data, size);
    case Type::Float64:
        return _GenBufferInfo<double>(data, size);
    case Type::FixedString:
        return py::buffer_info(
            data,
            type->GetStringSize(),    // Size of one scalar
            std::to_string(type->GetStringSize()) + "s", // format string
            1,            // Number of dimensions
            { size },     // Buffer dimensions
            { type->GetStringSize() } // Strides (in bytes) for each index
        );
    default:
        throw std::runtime_error(
                "buffer protocol not supported for type: " +
                type->GetName());
    }
}

template <typename T>
static inline py::list EnumToListString(ColumnRef col) {
    auto size = col->Size();

    py::list v(size);
    auto cenum = col->As<T>();
    for (size_t i = 0; i < col->Size(); ++i) {
        v[i] = py::str(cenum->NameAt(i));
    }
    return v;
}

static Column* NewEnumColumn(
        const py::dtype& dtype, py::handle handle) {
    std::vector<Type::EnumItem> enum_item;

    auto pyobj = handle.ptr();
    PyObject *iterator = PyObject_GetIter(pyobj);
    PyObject *item = nullptr;
    if (iterator == nullptr) {
        py::print(pyobj);
        throw std::invalid_argument("can't get iterator for enum item");
    }

    auto idx0 = PyInt_FromLong(0);
    auto idx1 = PyInt_FromLong(1);
    while ((item = PyIter_Next(iterator)) != nullptr) {
        auto name = PyObject_GetItem(item, idx0);
        auto val = PyObject_GetItem(item, idx1);
        enum_item.push_back(Type::EnumItem{
                std::string(PyString_AsString(name)),
                static_cast<int16_t>(PyInt_AsLong(val))});
        Py_DECREF(name);
        Py_DECREF(val);
        Py_DECREF(item);
    }
    Py_DECREF(idx0);
    Py_DECREF(idx1);
    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        PyErr_Print();
        throw std::runtime_error("error occured while appending string array");
    }
    ssize_t itemsize = dtype.itemsize();

    TypeRef typeref = nullptr;
    if (itemsize == 1) {
        return new ColumnEnum8(Type::CreateEnum8(enum_item));
    } else if (itemsize == 2) {
        return new ColumnEnum16(Type::CreateEnum16(enum_item));
    } else {
        throw std::invalid_argument(
                "enum column with itemsize " + std::to_string(itemsize));
    }
}

static Column* NewColumnFromDtype(
        const py::dtype& dtype, py::handle enum_item) {
    if (enum_item.ptr() != Py_None) {
        return NewEnumColumn(dtype, enum_item);
    }

    Column* col = nullptr;
    ssize_t itemsize = dtype.itemsize();

    char kind = dtype.kind();
    switch (kind) {
    case 'i':
        // signed integer
        switch (itemsize) {
        case 1:
            col = new ColumnInt8();
            break;
        case 2:
            col = new ColumnInt16();
            break;
        case 4:
            col = new ColumnInt32();
            break;
        case 8:
            col = new ColumnInt64();
            break;
        default:
            throw std::runtime_error(
                    "invalid item size for dtype kind i: " + std::to_string(itemsize));
            break;
        }
        break;

    case 'u':
        // unsigned integer
        switch (itemsize) {
        case 1:
            col = new ColumnUInt8();
            break;
        case 2:
            col = new ColumnUInt16();
            break;
        case 4:
            col = new ColumnUInt32();
            break;
        case 8:
            col = new ColumnUInt64();
            break;
        default:
            throw std::runtime_error(
                    "invalid item size for dtype kind u: " + std::to_string(itemsize));
            break;
        }
        break;

    case 'f':
        // float
        switch (itemsize) {
        case 4:
            col = new ColumnFloat32();
            break;
        case 8:
            col = new ColumnFloat64();
            break;
        default:
            throw std::runtime_error(
                    std::string("invalid item size for dtype kind f: ") + kind);
            break;
        }
        break;

    case 'S':
        if (itemsize > 0) {
            col = new ColumnFixedString(itemsize);
        } else {
            col = new ColumnString();
        }
        break;

    case 'O':
        col = new ColumnString();
        break;

    case 'M': {
        std::string format = py::str(dtype.ptr());
        if (format == "datetime64[D]") {
            col = new ColumnDate();
        } else if (format == "datetime64[s]") {
            col = new ColumnDateTime();
        } else {
            throw std::invalid_argument(
                "unsupported datetime64 type: "  + format);
        }
        break;
    }


    default:
        throw std::runtime_error(
                std::string("unsupported dtype kind: ") + kind);
    }

    return col;
}

static void AppendPyObject(ColumnRef col, py::handle handle) {
    auto pyobj = handle.ptr();
    using Code = Type::Code;
    switch (col->Type()->GetCode()) {
    case Code::Int8:
        col->AppendValue(static_cast<int8_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::UInt8:
        col->AppendValue(static_cast<uint8_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::Int16:
        col->AppendValue(static_cast<int16_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::UInt16:
        col->AppendValue(static_cast<uint16_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::Int32:
        col->AppendValue(static_cast<int32_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::UInt32:
        col->AppendValue(static_cast<uint32_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::Int64:
        col->AppendValue(static_cast<int64_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::UInt64:
    case Code::Date:
    case Code::DateTime:
        col->AppendValue(static_cast<uint64_t>(PyInt_AsLong(pyobj)));
        break;
    case Code::Float32:
        col->AppendValue(static_cast<float>(PyFloat_AsDouble(pyobj)));
        break;
    case Code::Float64:
        col->AppendValue(static_cast<double>(PyFloat_AsDouble(pyobj)));
        break;
    case Code::FixedString: {
        ssize_t size = 0;
        char* s = nullptr;
        if (PyString_AsStringAndSize(pyobj, &s, &size) == -1) {
            PyErr_Print();
            throw std::runtime_error("fail to get string from python object");
        }

        if (size >= col->Type()->GetStringSize()) {
            col->AppendValue(s);
        } else {
            // ColumnFixedString::Append(std::string) handles short string.
            auto strcol = col->As<ColumnFixedString>();
            strcol->Append(std::string(s, size));
        }
        break;
    }
    case Code::String:
        col->AppendValue(std::string(PyString_AsString(pyobj)));
        break;
    case Code::Enum8:
        if (PyString_CheckExact(pyobj)) {
            auto enum_col = col->As<ColumnEnum8>();
            enum_col->Append(PyString_AsString(pyobj));
        } else {
            col->AppendValue(static_cast<int8_t>(PyInt_AsLong(pyobj)));
        }
        break;
    case Code::Enum16:
        if (PyString_CheckExact(pyobj)) {
            auto enum_col = col->As<ColumnEnum16>();
            enum_col->Append(PyString_AsString(pyobj));
        } else {
            col->AppendValue(static_cast<int16_t>(PyInt_AsLong(pyobj)));
        }
        break;
    default:
        throw std::runtime_error(
                std::string("invalid Type::Code ") +
                std::to_string(col->Type()->GetCode()));
    }

    if (PyErr_Occurred()) {
        PyErr_Print();
        throw std::runtime_error("error occured while appending data item");
    }
}


PYBIND11_MODULE(_clickhouse, m) {
    m.doc() = "clickhouse-cpp python binding";

    py::enum_<Type::Code>(m, "TypeCode")
        .value("Int8", Type::Code::Int8)
        .value("Int16", Type::Code::Int16)
        .value("Int32", Type::Code::Int32)
        .value("Int64", Type::Code::Int64)
        .value("UInt8", Type::Code::UInt8)
        .value("UInt16", Type::Code::UInt16)
        .value("UInt32", Type::Code::UInt32)
        .value("UInt64", Type::Code::UInt64)
        .value("Float32", Type::Code::Float32)
        .value("Float64", Type::Code::Float64)
        .value("Array", Type::Code::Array)
        .value("Date", Type::Code::Date)
        .value("DateTime", Type::Code::DateTime)
        .value("Enum8", Type::Code::Enum8)
        .value("Enum16", Type::Code::Enum16)
        .value("String", Type::Code::String);

    // TypeRef is std::shared_ptr<Type>
    py::class_<Type, TypeRef>(m, "Type")
        .def("code", &Type::GetCode)
        .def("item_type", &Type::GetItemType)
        .def("name", &Type::GetName)
        .def("__eq__", &Type::IsEqual);

    py::class_<ArrayElement>(m, "ArrayElement", py::buffer_protocol())
        .def_buffer([](const ArrayElement& arr) -> py::buffer_info {
            return GenBufferInfo(arr.data, arr.size, arr.type);
        }
    );

    // ColumnRef is std::shared_ptr<Column>
    py::class_<Column, ColumnRef>(m, "Column", py::buffer_protocol())
        .def(py::init(&NewColumnFromDtype))
        .def("type", &Column::Type)
        .def("clear", &Column::Clear)
        .def("as_str", [](ColumnRef self) {
                if (self->Type()->GetCode() != Type::String) {
                    throw std::invalid_argument(
                        "as_str only supports ColumnString, given Column" +
                        self->Type()->GetName());
                }
                size_t size = self->Size();
                py::list lst(size);
                auto arr = static_cast<const std::string*>(self->Data());
                for (size_t i = 0; i < size; ++i) {
                    lst[i] = arr[i];
                }
                return lst;
            }
        )
        .def("as_enum_str", [](ColumnRef self) {
                if (self->Type()->GetCode() == Type::Enum8) {
                    return EnumToListString<ColumnEnum8>(self);
                } else if (self->Type()->GetCode() == Type::Enum16) {
                    return EnumToListString<ColumnEnum16>(self);
                } else {
                    throw std::invalid_argument(
                        "as_enum_str only supports ColumnEnum8/16, given Column" +
                        self->Type()->GetName());
                }
            }
        )
        .def("get_arr", [](ColumnRef self, size_t idx) {
                if (self->Type()->GetCode() != Type::Array) {
                    throw std::invalid_argument(
                        "get_arr only supports ColumnArray, given Column" +
                        self->Type()->GetName());
                }
                // TODO add another method to return string array for given
                // index.
                if (self->Type()->GetItemType()->GetCode() == Type::String) {
                    throw std::invalid_argument(
                        "get_arr does not support ColumnArray with String type");
                }
                return ArrayElement{
                    const_cast<void*>(self->Data(idx)),
                    self->GetSize(idx),
                    self->Type()->GetItemType()
                };
            }
        )
        .def("append_ndarray", [](ColumnRef self, py::array data) {
                if (data.ndim() != 1) {
                    throw std::invalid_argument(
                            "append ndarray dim " +
                            std::to_string(data.ndim()) + " > 1");
                }
                self->AppendData(data.data(), data.shape(0));
            }
        )
        .def("append_strarray", [](ColumnRef self, py::handle pyobj) {
                // Use Python C API here because I don't know how to access
                // PyObject from py::array.
                PyObject *iterator = PyObject_GetIter(pyobj.ptr());
                PyObject *item;
                if (iterator == nullptr) {
                    py::print(pyobj);
                    throw std::invalid_argument("can't get iterator");
                }

                while ((item = PyIter_Next(iterator)) != nullptr) {
                    self->AppendValue(
                            std::string(PyString_AsString(item)));
                    /* release reference when done */
                    Py_DECREF(item);
                }
                Py_DECREF(iterator);

                if (PyErr_Occurred()) {
                    PyErr_Print();
                    throw std::runtime_error("error occured while appending string array");
                }
            }
        )
        // Append single python object.
        .def("append", AppendPyObject)
        .def("__len__", &Column::Size)
        .def("__repr__", [](ColumnRef self) {
                std::ostringstream os;
                os << "Column("
                   << "type=" << self->Type()->GetName()
                   << " len=" << self->Size()
                   << ")";
                return os.str();
            }
        )
        .def_buffer([](const Column& self) -> py::buffer_info {
                return GenBufferInfo(
                        const_cast<void*>(self.Data()),
                        self.Size(),
                        self.Type());
            }
        );

    py::class_<Block>(m, "Block")
        .def(py::init<>())
        .def("cols", &Block::GetColumnCount)
        .def("rows", &Block::GetRowCount)
        .def("name", &Block::GetColumnName)
        .def("append_column", [](
                    Block* self, const std::string& name, ColumnRef col) {
                self->AppendColumn(name, col, false);
            }
        )
        .def("__repr__", [](const Block& self) {
                std::ostringstream os;
                os << "Block("
                   << "cols=" << self.GetColumnCount()
                   << " rows=" << self.GetRowCount()
                   << ")";
                return os.str();
            }
        )
        .def("__getitem__", [](const Block& self, size_t idx) {
                auto col = self[idx];
                return col;
            }
        );

    py::enum_<CompressionMethod>(m, "CompressionMethod")
        .value("None", CompressionMethod::None)
        .value("LZ4", CompressionMethod::LZ4);

    py::class_<ClientOptions>(m, "ClientOptions")
        .def(py::init(
            [](const std::string& host,
                    int port,
                    const std::string& user,
                    const std::string& password) {
                auto opt = new ClientOptions;
                opt->host = host;
                opt->port = port;
                opt->user = user;
                opt->password = password;
                return opt;
            }),
                py::arg("host") = "localhost",
                py::arg("port") = 9000,
                py::arg("user") = "default",
                py::arg("password") = ""
        )
        .def_readwrite("host", &ClientOptions::host)
        .def_readwrite("port", &ClientOptions::port)
        .def_readwrite("user", &ClientOptions::user)
        .def_readwrite("password", &ClientOptions::password)
        .def_readwrite("rethrow_exceptions", &ClientOptions::rethrow_exceptions)
        .def_readwrite("ping_before_query", &ClientOptions::ping_before_query)
        .def_readwrite("send_retries", &ClientOptions::send_retries)
        .def_readwrite("retry_timeout", &ClientOptions::retry_timeout)
        .def_readwrite("compression_method", &ClientOptions::retry_timeout)
        .def("__repr__", [](const ClientOptions &self) {
                std::ostringstream os;
                os << self;
                return os.str();
            }
        );

    py::class_<Client>(m, "Client")
        .def(py::init<const ClientOptions&>())
        .def("execute", [](Client* self, const std::string& query) {
                self->Execute(query);
            }
        )
        // Use type convertion to select overloaded function.
        .def("select", (void (Client::*)(const std::string&, Block*))
                &Client::Select
        )
        .def("insert", &Client::Insert);
}

}
