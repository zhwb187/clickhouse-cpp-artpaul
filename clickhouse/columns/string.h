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

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    const void* Data() const override { return data_.data(); }

    size_t StringSize() { return string_size_; }

private:
    const size_t string_size_;
    std::vector<std::string> data_;
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

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override;

    const void* Data() const override { return data_.data(); }

private:
    std::vector<std::string> data_;
};


class ColumnFixedChars : public Column {
public:
    explicit ColumnFixedChars(size_t n)
        : Column(Type::CreateString(n))
        , string_size_(n)
    {}

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override { column->Size(); }

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override { input->Skip(rows); return true; }

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override { output->Flush(); }

    /// Clear column data .
    void Clear() override {}

    /// Returns count of rows in the column.
    size_t Size() const override { return data_.size(); }

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override { begin += len; return std::make_shared<ColumnFixedChars>(0); }

    const void* Data() const override { return data_.data(); }

    size_t StringSize() { return string_size_; }

private:
    const size_t string_size_;
    std::vector<char> data_;
};



class ColumnChars : public Column {
public:
    ColumnChars()
        :Column(Type::CreateString())
    {}

    ~ColumnChars()
    {
        for (auto p : data_)
            delete[] p;
    }

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override { column->Size(); }

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override
    {
        return input->ReadCharsRows(data_, rows);
    }

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override { output->Flush(); }

    /// Clear column data .
    void Clear() override {}

    /// Returns count of rows in the column.
    size_t Size() const override { return data_.size(); }

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) override { begin += len; return std::make_shared<ColumnChars>(); }

    const void* Data() const override { return data_.data(); }

private:
    std::vector<char*> data_;
};

}
