#include <gtest/gtest.h>
#include <Functions/FunctionsHashing.h>

TEST(IntHash, HashSmoke)
{
    EXPECT_NE(CH::IntHash32Impl::apply(0), 0);
    EXPECT_NE(CH::IntHash64Impl::apply(0), 0);
}

TEST(CityHash, HashSmoke)
{
    EXPECT_NE(CH::ImplCityHash64::apply("test", 4), 0);
}

TEST(xxHash, HashSmoke)
{
    EXPECT_NE(CH::ImplXxHash32::apply("test", 4), 0);
    EXPECT_NE(CH::ImplXxHash64::apply("test", 4), 0);
    EXPECT_NE(CH::ImplXXH3::apply("test", 4), 0);
}

TEST(wyHash, HashSmoke)
{
    EXPECT_NE(CH::ImplWyHash64::apply("test", 4), 0);
}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
