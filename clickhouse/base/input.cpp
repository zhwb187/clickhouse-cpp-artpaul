#include "input.h"

#include <algorithm>
#include <thread>
#include <memory.h>
#include <cstring>

namespace clickhouse {

size_t ZeroCopyInput::DoRead(void* buf, size_t len) {
    const void* ptr;
    size_t result = DoNext(&ptr, len);

    if (result) {
        memcpy(buf, ptr, result);
    }

    return result;
}

ArrayInput::ArrayInput() noexcept
    : data_(nullptr)
    , len_(0)
{
}

ArrayInput::ArrayInput(const void* buf, size_t len) noexcept
    : data_(static_cast<const uint8_t*>(buf))
    , len_(len)
{
}

ArrayInput::~ArrayInput() = default;

size_t ArrayInput::DoNext(const void** ptr, size_t len) {
    len = std::min(len_, len);

    *ptr   = data_;
    len_  -= len;
    data_ += len;

    return len;
}


BufferedInput::BufferedInput(InputStream* slave, size_t buflen, size_t quelen)
    : slave_(slave)
    , array_input_(nullptr, 0)
    , buflen_(buflen)
    , data_(nullptr)
    , data_queue_(quelen)
{
}

BufferedInput::~BufferedInput() = default;

void BufferedInput::Reset() {
    array_input_.Reset(nullptr, 0);
}

void BufferedInput::RecvData()
{
    std::vector<uint8_t> tmp(buflen_);
    auto buf = tmp.data();
    size_t read_len = 0;
    uint8_t* data = nullptr;
    while (true)
    {
        read_len = slave_->Read(buf, buflen_);
        data = new uint8_t[read_len + sizeof(size_t)];
        *(reinterpret_cast<size_t*>(data)) = read_len;
        std::memcpy(data + sizeof(size_t), buf, read_len);

        if (!data_queue_.enqueue(data))
        {
            throw std::runtime_error("enqueue memory allocation fails");
        }
    }
}

void BufferedInput::SwitchBuffer()
{
    static std::thread recv_thr = std::thread([this]() { RecvData(); });

    delete[] data_;
    data_queue_.wait_dequeue(data_);
    array_input_.Reset(data_ + sizeof(size_t), *(reinterpret_cast<size_t*>(data_)));
}

size_t BufferedInput::DoNext(const void** ptr, size_t len) {
    if (array_input_.Exhausted()) {
        //array_input_.Reset(
        //    buffer_.data(), slave_->Read(buffer_.data(), buffer_.size())
        //);
        SwitchBuffer();
    }

    return array_input_.Next(ptr, len);
}

size_t BufferedInput::DoRead(void* buf, size_t len) {
    if (array_input_.Exhausted()) {
        //if (len > buffer_.size() / 2) {
        //    return slave_->Read(buf, len);
        //}

        //array_input_.Reset(
        //    buffer_.data(), slave_->Read(buffer_.data(), buffer_.size())
        //);
        SwitchBuffer();
    }

    return array_input_.Read(buf, len);
}

}
