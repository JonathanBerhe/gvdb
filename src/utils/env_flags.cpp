#include "utils/env_flags.h"

#include <cstdlib>

namespace gvdb::utils {

std::string ResolveFlag(const char* env_var, const std::string& cli_value) {
  if (env_var != nullptr) {
    const char* env_val = std::getenv(env_var);
    if (env_val != nullptr && env_val[0] != '\0') {
      return std::string(env_val);
    }
  }
  return cli_value;
}

}  // namespace gvdb::utils
