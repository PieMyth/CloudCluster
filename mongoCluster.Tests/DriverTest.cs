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
        public void collectionExistsTest_returnsTrue(String validCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Assert.True(driver.collectionExists(validCollection));
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

        [Theory]
        [InlineData("invalidName")]
        [InlineData("")]
        public void collectionExistsTest_invalidValueReturnsFalse(String invalidCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Assert.False(driver.collectionExists(invalidCollection));
        }

        [InlineData(null)]
        public void collectionExistsTest_nullValueThrowsException(String nullValue)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Assert.Throws<ArgumentNullException>(() => driver.collectionExists(nullValue));
        }

        [Theory]
        [InlineData("")]
        [InlineData("invalidName")]
        public void getCollectionTest_invalidNameConnectionFails(String invalidCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Assert.False(driver.getCollection(invalidCollection));
        }

        [Theory]
        [InlineData(null)]
        public void getCollectionTest_nullvalueConnectionFails(String nullValue)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Assert.Throws<ArgumentNullException>(() => driver.getCollection(nullValue));
        }
    }
}
