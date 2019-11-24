using System;
using Xunit;

namespace mongoCluster.Tests
{
    public class DriverTest
    {
        [Fact]
        public void establishConnectionTest_connectionSuccessfullyEstablished()
        {

            Driver driver = new Driver();
            Assert.True(driver.establishConnection());
        }

        [Fact]
        public void getDatabaseTest_databaseSuccessfulyConnected()
        {

            Driver driver = new Driver();
            driver.establishConnection();
            Assert.True(driver.getDatabase());
        }

        [Theory]
        [InlineData("listings")]
        [InlineData("reviews")]
        public void getCollectionTest_collectionSuccessfullyConnected(String validCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Assert.True(driver.getCollection(validCollection));
        }
    }
}
