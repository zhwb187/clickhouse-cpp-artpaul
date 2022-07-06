#pragma once

#include "column.h"

namespace clickhouse {

ColumnRef CreateColumnByType(const std::string& type_name);

extern bool g_use_chars_for_string;

}
