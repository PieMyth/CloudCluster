using System;
using System.Configuration;
using Xunit;

namespace mongoCluster.Tests
{
    public class DriverTest
    {
        private const string _listings = "listings";
        private const string _reviews = "reviews";

        [Fact]
        public void configurationFileExists()
        {
            // The following line returns the location of the .config file this test script reads from
            // string configFile = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).FilePath;
            
            string value = ConfigurationManager.AppSettings["TestValue"];
            Xunit.Assert.False(string.IsNullOrEmpty(value), "No App.Config found.");
        }

        [Fact]
        public void establishConnectionTest_connectionSuccessfullyEstablished()
        {

            Driver driver = new Driver();
            Xunit.Assert.True(driver.establishConnection());
        }

        [Fact]
        public void getDatabaseTest_databaseSuccessfulyConnected()
        {

            Driver driver = new Driver();
            driver.establishConnection();
            Xunit.Assert.True(driver.getDatabase());
        }

        [Theory]
        [InlineData(_listings)]
        [InlineData(_reviews)]
        public void collectionExistsTest_returnsTrue(String validCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Xunit.Assert.True(driver.collectionExists(validCollection));
        }

        [Theory]
        [InlineData(_listings)]
        [InlineData(_reviews)]
        public void getCollectionTest_collectionSuccessfullyConnected(String validCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Xunit.Assert.True(driver.getCollection(validCollection));
        }

        [Theory]
        [InlineData("invalidName")]
        [InlineData("")]
        public void collectionExistsTest_invalidValueReturnsFalse(String invalidCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Xunit.Assert.False(driver.collectionExists(invalidCollection));
        }

        [Theory]
        [InlineData(null)]
        public void collectionExistsTest_nullValueThrowsException(String nullValue)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Xunit.Assert.Throws<ArgumentNullException>(() => driver.collectionExists(nullValue));
        }

        [Theory]
        [InlineData("")]
        [InlineData("invalidName")]
        public void getCollectionTest_invalidNameConnectionFails(String invalidCollection)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Xunit.Assert.False(driver.getCollection(invalidCollection));
        }

        [Theory]
        [InlineData(null)]
        public void getCollectionTest_nullvalueConnectionFails(String nullValue)
        {
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            Xunit.Assert.Throws<ArgumentNullException>(() => driver.getCollection(nullValue));
        }

        [Fact]
        public void queryCountDocumentsTest_countGreaterThanFiveThousand()
        { 
            Driver driver = new Driver();
            driver.establishConnection();
            driver.getDatabase();
            driver.getCollection(_listings);
            Xunit.Assert.True(driver.queryCountDocuments(_listings) > 5000);
        }

    }
}
