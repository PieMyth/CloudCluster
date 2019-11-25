using System;
using Xunit;

namespace mongoCluster.Tests
{
    public class ProgramTest
    {
        [Fact]
        public void SolutionSanityTest_AssertSuccessful()
        {
            String test = "Solution correctly ties Program and ProgramTest";
            Assert.NotNull(test);
        }
    }
}
