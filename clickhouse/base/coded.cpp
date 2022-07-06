#include "coded.h"

#include <memory.h>
#include <cstring>

namespace clickhouse {

static const int MAX_VARINT_BYTES = 10;

CodedInputStream::CodedInputStream(ZeroCopyInput* input)
    : input_(input)
{
}

//bool CodedInputStream::ReadRaw(void* buffer, size_t size) {
//    uint8_t* p = static_cast<uint8_t*>(buffer);
//
//    while (size > 0) {
//        const void* ptr;
//        size_t len = input_->Next(&ptr, size);
//
//        memcpy(p, ptr, len);
//
//        p += len;
//        size -= len;
//    }
//
//    return true;
//}
bool CodedInputStream::ReadRaw(void* buffer, size_t size)
{
    auto p = static_cast<uint8_t*>(buffer);
    const void* ptr;
    size_t len;
    do
    {
        len = input_->Next(&ptr, size);
        std::memcpy(p, ptr, len);
        p += len;
        size -= len;
    } while (size);

    return true;
}

bool CodedInputStream::Skip(size_t count) {
    while (count > 0) {
        const void* ptr;
        size_t len = input_->Next(&ptr, count);

        if (len == 0) {
            return false;
        }

        count -= len;
    }

    return true;
}

//bool CodedInputStream::ReadVarint64(uint64_t* value) {
//    *value = 0;
//
//    for (size_t i = 0; i < MAX_VARINT_BYTES; ++i) {
//        uint8_t byte;
//
//        if (!input_->ReadByte(&byte)) {
//            return false;
//        } else {
//            *value |= uint64_t(byte & 0x7F) << (7 * i);
//
//            if (!(byte & 0x80)) {
//                return true;
//            }
//        }
//    }
//
//    // TODO skip invalid
//    return false;
//}
bool CodedInputStream::ReadVarint64(uint64_t* value)
{
    *value = 0;
    uint8_t byte;
    if (input_->ReadByte(&byte))
        *value = byte & 0x7FULL;
    else
        return false;
    if (byte & 0x80)
    {
        if (input_->ReadByte(&byte))
            *value |= (byte & 0x7FULL) << 7;
        else
            return false;
        if (byte & 0x80)
        {
            int lest_shift = 14;
            int i = MAX_VARINT_BYTES;
            --i;
            while (--i)
            {
                if (input_->ReadByte(&byte))
                {
                    *value |= (byte & 0x7FULL) << lest_shift;
                    if (!(byte & 0x80))
                        return true;
                    lest_shift += 7;
                }
                else
                    return false;
            }
            return false;
        }
    }
    return true;
}

bool CodedInputStream::SkipVarint64()
{
    uint8_t byte;
    for (size_t i = 0; i < MAX_VARINT_BYTES; ++i)
    {
        if (input_->ReadByte(&byte))
        {
            if (!(byte & 0x80)) 
                return true;
        }
        else
            return false;
    }
    return false;
}

bool CodedInputStream::ReadStringRows(std::vector<std::string>& data, size_t rows)
{
    data.resize(rows);
    auto ps = data.data();
    uint8_t byte;
    uint64_t len;
    size_t size;
    int i;
    int lest_shift;
    char* p;
    const void* ptr;
    ++rows;
    while(--rows)
    {
        // load string length
        if (input_->ReadByte(&byte))
            len = byte & 0x7FULL;
        else
            return false;
        if (byte & 0x80)
        {
            if (input_->ReadByte(&byte))
                len |= (byte & 0x7FULL) << 7;
            else
                return false;
            if (byte & 0x80)
            {
                lest_shift = 14;
                i = MAX_VARINT_BYTES;
                --i;
                while (--i)
                {
                    if (input_->ReadByte(&byte))
                    {
                        len |= (byte & 0x7FULL) << lest_shift;
                        if (!(byte & 0x80)) {
                            break;
                        }
                        lest_shift += 7;
                    }
                    else
                        return false;
                }
                if (!i)
                    return false;
            }
        }
        if (len > 0x00FFFFFFULL)
            return false;
        // load string data
        ps->resize(len);
        p = const_cast<char*>(ps->data());
        do
        {
            size = input_->Next(&ptr, len);
            std::memcpy(p, ptr, len);
            p += size;
            len -= size;
        } while (len);
        ++ps;
    }
    return true;
}

bool CodedInputStream::ReadCharsRows(std::vector<char*>& data, size_t rows)
{
    data.resize(rows);
    auto ps = data.data();
    uint8_t byte;
    uint64_t len;
    size_t size;
    int i;
    int lest_shift;
    char* p;
    const void* ptr;
    ++rows;
    while (--rows)
    {
        // load string length
        if (input_->ReadByte(&byte))
            len = byte & 0x7FULL;
        else
            return false;
        if (byte & 0x80)
        {
            if (input_->ReadByte(&byte))
                len |= (byte & 0x7FULL) << 7;
            else
                return false;
            if (byte & 0x80)
            {
                lest_shift = 14;
                i = MAX_VARINT_BYTES;
                --i;
                while (--i)
                {
                    if (input_->ReadByte(&byte))
                    {
                        len |= (byte & 0x7FULL) << lest_shift;
                        if (!(byte & 0x80)) {
                            break;
                        }
                        lest_shift += 7;
                    }
                    else
                        return false;
                }
                if (!i)
                    return false;
            }
        }
        if (len > 0x00FFFFFFULL)
            return false;
        // load string data
        p = new char[sizeof(size_t) + len];
        *ps = p;
        *(reinterpret_cast<size_t*>(p)) = len;
        p += sizeof(size_t);
        do
        {
            size = input_->Next(&ptr, len);
            std::memcpy(p, ptr, len);
            p += size;
            len -= size;
        } while (len);
        ++ps;
    }
    return true;
}

bool CodedInputStream::ReadFixedStringRows(std::vector<std::string>& data, size_t rows, size_t string_size)
{
    data.resize(rows);
    auto ps = data.data();
    char* p;
    size_t len;
    size_t size;
    const void* ptr;
    ++rows;
    while (--rows)
    {
        ps->resize(string_size);
        p = const_cast<char*>(ps->data());
        len = string_size;
        do
        {
            size = input_->Next(&ptr, len);
            std::memcpy(p, ptr, len);
            p += size;
            len -= size;
        } while (len);
        ++ps;
    }
    return true;
}

bool CodedInputStream::ReadFixedCharsRows(std::vector<char>& data, size_t rows, size_t string_size)
{
    size_t len = rows * string_size;
    data.resize(len);
    char* p = const_cast<char*>(data.data());
    size_t size;
    const void* ptr;
    do
    {
        size = input_->Next(&ptr, len);
        std::memcpy(p, ptr, len);
        p += size;
        len -= size;
    } while (len);
    return true;
}

CodedOutputStream::CodedOutputStream(ZeroCopyOutput* output)
    : output_(output)
{
}

void CodedOutputStream::Flush() {
    output_->Flush();
}

void CodedOutputStream::WriteRaw(const void* buffer, int size) {
    output_->Write(buffer, size);
}

void CodedOutputStream::WriteVarint64(uint64_t value) {
    uint8_t bytes[MAX_VARINT_BYTES];
    int size = 0;

    for (size_t i = 0; i < MAX_VARINT_BYTES; ++i) {
        uint8_t byte = value & 0x7F;
        if (value > 0x7F)
            byte |= 0x80;

        bytes[size++] = byte;

        value >>= 7;
        if (!value) {
            break;
        }
    }

    WriteRaw(bytes, size);
}

}
