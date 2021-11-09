#include "input.h"

#include <algorithm>
#include <memory.h>

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


BufferedInput::BufferedInput(InputStream* slave, size_t buflen)
    : slave_(slave)
    , array_input_(nullptr, 0)
    //, buffer_(buflen)
    , buffers_(2)
    , read_index_(0)
    , recv_index_(0)
    , recv_size_(0)
{
    for (auto& buf : buffers_)
        buf.resize(buflen);
}

BufferedInput::~BufferedInput() = default;

void BufferedInput::Reset() {
    array_input_.Reset(nullptr, 0);
}

void BufferedInput::RecvData()
{
    auto& buffer_ = buffers_[recv_index_];
    recv_size_ = slave_->Read(buffer_.data(), buffer_.size());
}

void BufferedInput::SwitchBuffer()
{
    if (recv_thr_.joinable())
        recv_thr_.join();
    if (recv_size_ == 0)
    {
        auto& buffer_ = buffers_[read_index_];
        array_input_.Reset(
            buffer_.data(), slave_->Read(buffer_.data(), buffer_.size())
        );
    }
    else
    {
        read_index_ = recv_index_;
        auto& buffer_ = buffers_[read_index_];
        array_input_.Reset(buffer_.data(), recv_size_);
    }
    recv_index_ = (read_index_ + 1) % buffers_.size();
    recv_size_ = 0;
    recv_thr_ = std::thread([this]() { RecvData(); });
}

size_t BufferedInput::DoNext(const void** ptr, size_t len)  {
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
