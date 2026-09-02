#include "common/version.h"

#include <gtest/gtest.h>

TEST(Version, IsSemver) { EXPECT_EQ(kv::Version(), "0.1.0"); }
