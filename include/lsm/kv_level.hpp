#pragma once

#include "type.hpp"

namespace lsm {

enum class KVLevelType { kTiering, kLeveling };
struct KVLevelConfig {
	size_type max_files;
	KVLevelType type;
};

} // namespace lsm