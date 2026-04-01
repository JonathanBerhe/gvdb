#pragma once

#include <string>

namespace gvdb::utils {

// Resolve a configuration value: returns env var if set, otherwise cli_value.
// cli_value should already contain the default from argument parsing.
std::string ResolveFlag(const char* env_var, const std::string& cli_value);

}  // namespace gvdb::utils
