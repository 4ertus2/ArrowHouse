#include <gtest/gtest.h>
#include <Functions/FunctionsHashing.h>

TEST(IntHash, HashSmoke)
{
    EXPECT_NE(AH::IntHash32Impl::apply(0), 0);
    EXPECT_NE(AH::IntHash64Impl::apply(0), 0);
}

TEST(CityHash, HashSmoke)
{
    EXPECT_NE(AH::ImplCityHash64::apply("test", 4), 0);
}

TEST(xxHash, HashSmoke)
{
    EXPECT_NE(AH::ImplXxHash32::apply("test", 4), 0);
    EXPECT_NE(AH::ImplXxHash64::apply("test", 4), 0);
    EXPECT_NE(AH::ImplXXH3::apply("test", 4), 0);
}

TEST(wyHash, HashSmoke)
{
    EXPECT_NE(AH::ImplWyHash64::apply("test", 4), 0);
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
