#include "client.h"
#include "protocol.h"
#include "varint.h"
#include "columns.h"

#include "io/coded_input.h"
#include "net/socket.h"

#include <system_error>
#include <vector>
#include <iostream>

#define DBMS_NAME                                       "ClickHouse"
#define DBMS_VERSION_MAJOR                              1
#define DBMS_VERSION_MINOR                              1
#define REVISION                                        54126

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         50264
#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS   51554
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO               51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO              54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060

namespace clickhouse {

using namespace io;
using namespace net;

struct ClientInfo {
    uint8_t interface = 1; // TCP
    uint8_t query_kind;
    std::string initial_user;
    std::string initial_query_id;
    std::string quota_key;
    std::string os_user;
    std::string client_hostname;
    std::string client_name;
    std::string initial_address = "[::ffff:127.0.0.1]:0";
    uint64_t client_version_major = 0;
    uint64_t client_version_minor = 0;
    uint32_t client_revision = 0;
};

struct ServerInfo {
    std::string name;
    std::string timezone;
    uint64_t    version_major;
    uint64_t    version_minor;
    uint64_t    revision;
};

class Client::Impl {
public:
    Impl(const std::string& host, const std::string& port)
        : socket_(SocketConnect(NetworkAddress(host, port)))
        , socket_input_(socket_)
        , buffered_(&socket_input_)
        , input_(&buffered_)
        , events_(nullptr)
    {
        if (socket_.Closed()) {
            throw std::system_error(errno, std::system_category());
        }
    }

    ~Impl() {
        Disconnect();
    }

    void Handshake() {
        sendHello();
        receiveHello();
    }

    void ExecuteQuery(const std::string& query, QueryEvents* events) {
        events_ = events;

        // Выбрать рабочее соединение
        try {
            SendQuery(query);

            while (ReceivePacket())
            { }

            events_ = nullptr;
        } catch (...) {
            events_ = nullptr;
            throw;
        }
    }

    void SendQuery(const std::string& query) {
        std::vector<char> data(64 << 10);
        char* p = data.data();

        p = writeVarUInt(ClientCodes::Query, p);
        p = writeStringBinary("1"/*query_id*/, p);

        /// Client info.
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        {
            ClientInfo info;

            info.query_kind = 1;
            info.client_name = "ClickHouse client";
            info.client_version_major = DBMS_VERSION_MAJOR;
            info.client_version_minor = DBMS_VERSION_MINOR;
            info.client_revision = REVISION;

            p = writeBinary(info.query_kind, p);
            p = writeStringBinary(info.initial_user, p);
            p = writeStringBinary(info.initial_query_id, p);
            p = writeStringBinary(info.initial_address, p);
            p = writeBinary(info.interface, p);

            p = writeStringBinary(info.os_user, p);
            p = writeStringBinary(info.client_hostname, p);
            p = writeStringBinary(info.client_name, p);
            p = writeVarUInt(info.client_version_major, p);
            p = writeVarUInt(info.client_version_minor, p);
            p = writeVarUInt(info.client_revision, p);

            if (server_info_.revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
                p = writeStringBinary(info.quota_key, p);
        }

        /// Per query settings.
        //if (settings)
        //    settings->serialize(*out);
        //else
        p = writeStringBinary("", p);

        uint64_t stage = 2; // Complete
        p = writeVarUInt(stage, p);
        p = writeVarUInt(CompressionState::Disable, p);
        p = writeStringBinary(query, p);

        // Empty block
        {
            p = writeVarUInt(ClientCodes::Data, p);

            if (server_info_.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
                p = writeStringBinary("", p);
            }
            /// Дополнительная информация о блоке.
            //if (REVISION >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
            //    block.info.write(ostr);
            //}

            /// Размеры
            p = writeVarUInt(0, p);
            p = writeVarUInt(0, p);
            p = writeVarUInt(0, p);
        }

        // TODO result
        if (::send(socket_, data.data(), p - data.data(), 0) != p - data.data()) {
            throw std::runtime_error("fail to send hello");
        }
    }

private:
    bool ReceivePacket() {
        uint64_t packet_type = 0;

        if (!input_.ReadVarint64(&packet_type)) {
            return false;
        }

        std::cerr << "receive packet " << packet_type << std::endl;
        switch (packet_type) {
            case ServerCodes::Data: {
                if (REVISION >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
                    std::string table_name;

                    if (!WireFormat::ReadString(&input_, &table_name)) {
                        return false;
                    }
                    std::cerr << "temporary_table_name : " << table_name << std::endl;
                }
                /// Дополнительная информация о блоке.
                if (REVISION >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
                    uint64_t num;
                    int32_t bucket_num = 0;
                    uint8_t is_overflows = 0;

                    // BlockInfo
                    if (!WireFormat::ReadUInt64(&input_, &num)) {
                        return false;
                    }
                    if (!WireFormat::ReadFixed(&input_, &is_overflows)) {
                        return false;
                    }
                    if (!WireFormat::ReadUInt64(&input_, &num)) {
                        return false;
                    }
                    if (!WireFormat::ReadFixed(&input_, &bucket_num)) {
                        return false;
                    }
                    if (!WireFormat::ReadUInt64(&input_, &num)) {
                        return false;
                    }

                    std::cerr << "bucket_num : " << bucket_num << std::endl;
                    std::cerr << "is_overflows : " << bool(is_overflows) << std::endl;
                }

                uint64_t num_columns = 0;
                uint64_t num_rows = 0;
                std::string name;

                if (!WireFormat::ReadUInt64(&input_, &num_columns)) {
                    return false;
                }
                if (!WireFormat::ReadUInt64(&input_, &num_rows)) {
                    return false;
                }

                std::cerr << "num_columns : " << num_columns << std::endl;
                std::cerr << "num_rows : " << num_rows << std::endl;

                for (size_t i = 0; i < num_columns; ++i) {
                    if (!WireFormat::ReadString(&input_, &name)) {
                        return false;
                    }
                    std::cerr << "name : " << name << std::endl;

                    if (!WireFormat::ReadString(&input_, &name)) {
                        return false;
                    }
                    std::cerr << "type : " << name << std::endl;

                    if (num_rows) {
                        if (name == "UInt64") {
                            ColumnUInt64 c;
                            if (c.Load(&input_, num_rows)) {
                                for (size_t i = 0; i < c.Size(); ++i) {
                                    std::cerr << c[i] << std::endl;
                                }
                            } else {
                                throw std::runtime_error("can't load");
                            }
                        } else if (name == "String") {
                            ColumnString c;
                            if (c.Load(&input_, num_rows)) {
                                for (size_t i = 0; i < c.Size(); ++i) {
                                    std::cerr << c[i] << std::endl;
                                }
                            } else {
                                throw std::runtime_error("can't load");
                            }
                        } else {
                            // type.deserializeBinary(column, istr, rows);
                            throw std::runtime_error("type deserialization is not implemented");
                        }
                    }
                }
                return true;
            }

        case ServerCodes::ProfileInfo: {
            uint64_t rows = 0;
            uint64_t blocks = 0;
            uint64_t bytes = 0;
            uint64_t rows_before_limit = 0;
            bool applied_limit = false;
            bool calculated_rows_before_limit = false;

            if (!WireFormat::ReadUInt64(&input_, &rows)) {
                return false;
            }
            if (!WireFormat::ReadUInt64(&input_, &blocks)) {
                return false;
            }
            if (!WireFormat::ReadUInt64(&input_, &bytes)) {
                return false;
            }
            if (!WireFormat::ReadFixed(&input_, &applied_limit)) {
                return false;
            }
            if (!WireFormat::ReadUInt64(&input_, &rows_before_limit)) {
                return false;
            }
            if (!WireFormat::ReadFixed(&input_, &calculated_rows_before_limit)) {
                return false;
            }

            std::cerr << "rows : " << rows << std::endl;
            std::cerr << "blocks : " << blocks << std::endl;
            std::cerr << "bytes : " << bytes << std::endl;
            std::cerr << "rows_before_limit : " << rows_before_limit << std::endl;
            std::cerr << "applied_limit : " << int(applied_limit) << std::endl;
            std::cerr << "calculated_rows_before_limit : " << int(calculated_rows_before_limit) << std::endl;
            return true;
        }

        case ServerCodes::Progress: {
            Progress info;

            if (!WireFormat::ReadUInt64(&input_, &info.rows)) {
                return false;
            }
            if (!WireFormat::ReadUInt64(&input_, &info.bytes)) {
                return false;
            }
            if (REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) {
                if (!WireFormat::ReadUInt64(&input_, &info.total_rows)) {
                    return false;
                }
            }

            if (events_) {
                events_->OnProgress(info);
            }

            break;
        }

        case ServerCodes::EndOfStream: {
            // graceful completion
            return false;
        }

        default:
            throw std::runtime_error("unimplemented");
            break;
        }

        return false;
    }

    void sendHello() {
        std::vector<char> data(64 << 10);
        char* p = data.data();

        p = writeVarUInt((int)ClientCodes::Hello, p);
        p = writeStringBinary(std::string(DBMS_NAME) + " client", p);
        p = writeVarUInt(DBMS_VERSION_MAJOR, p);
        p = writeVarUInt(DBMS_VERSION_MINOR, p);
        p = writeVarUInt(REVISION, p);
        p = writeStringBinary("system", p); // default_database
        p = writeStringBinary("default", p);            // user
        p = writeStringBinary("", p);  // password

        // TODO result
        ::send(socket_, data.data(), p - data.data(), 0);
    }

    void receiveHello() {
        uint64_t packet_type = 0;

        if (!input_.ReadVarint64(&packet_type)) {
            goto fail;
        }

        if (packet_type == ServerCodes::Hello) {
            if (!WireFormat::ReadString(&input_, &server_info_.name)) {
                goto fail;
            }
            if (!WireFormat::ReadUInt64(&input_, &server_info_.version_major)) {
                goto fail;
            }
            if (!WireFormat::ReadUInt64(&input_, &server_info_.version_minor)) {
                goto fail;
            }
            if (!WireFormat::ReadUInt64(&input_, &server_info_.revision)) {
                goto fail;
            }

            if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
                if (!WireFormat::ReadString(&input_, &server_info_.timezone)) {
                    goto fail;
                }
            }

            std::cerr << server_info_.name << std::endl;
            std::cerr << server_info_.revision << std::endl;
            std::cerr << server_info_.timezone << std::endl;
        } else if (packet_type == ServerCodes::Exception) {
            //receiveException()->rethrow();
            Disconnect();
        } else {
            /// Close connection, to not stay in unsynchronised state.
            Disconnect();
        }

        return;

    fail:
        Disconnect();
    }

private:
    void Disconnect() {
        socket_.Close();
    }

private:
    SocketHolder socket_;
    SocketInput socket_input_;
    BufferedInput buffered_;
    CodedInputStream input_;

    ServerInfo server_info_;

    QueryEvents* events_;
};

Client::Client()
    : host_()
    , port_(0)
{
}

Client::Client(const ClientOptions& opts)
    : host_(opts.host)
    , port_(opts.port)
{
}

Client::Client(const std::string& host, int port)
    : host_(host)
    , port_(port)
{
}

Client::~Client()
{ }

void Client::Connect() {
    // TODO check initialization
    impl_.reset(new Impl(host_, std::to_string(port_)));
    impl_->Handshake();
}

void Client::ExecuteQuery(const std::string& query, QueryEvents* events) {
    if (impl_) {
        impl_->ExecuteQuery(query, events);
    }
}

}
