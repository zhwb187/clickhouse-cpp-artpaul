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


BufferedInput::BufferedInput(InputStream* slave, size_t buflen, size_t bufnum)
    : slave_(slave)
    , array_input_(nullptr, 0)
    //, buffer_(buflen)
    , read_index_(0)
    , recv_index_(0)
    , ready_bufnum_(0)
    , bufnum_(bufnum >= 2 ? bufnum : 2)
    , buffers_(bufnum_)
    , recv_sizes_(bufnum_)
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
    std::unique_lock<std::mutex> lck(recv_mtx_);
    lck.unlock();
    while (true)
    {
        lck.lock();
        ++ready_bufnum_;
        recv_cv_.notify_one();
        recv_cv_.wait(lck, [this]() { return ready_bufnum_ < bufnum_; });
        lck.unlock();

        ++recv_index_;
        recv_index_ %= bufnum_;
        auto& buffer_ = buffers_[recv_index_];
        recv_sizes_[recv_index_] = slave_->Read(buffer_.data(), buffer_.size());
    }
}

void BufferedInput::SwitchBuffer()
{
    static std::thread recv_thr = std::thread([this]() { RecvData(); });

    std::unique_lock<std::mutex> lck(recv_mtx_);
    --ready_bufnum_;
    recv_sizes_[read_index_] = 0;
    recv_cv_.notify_one();
    recv_cv_.wait(lck, [this]() { return ready_bufnum_ > 0; });
    lck.unlock();

    ++read_index_;
    read_index_ %= bufnum_;
    auto& buffer_ = buffers_[read_index_];
    array_input_.Reset(buffer_.data(), recv_sizes_[read_index_]);
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
