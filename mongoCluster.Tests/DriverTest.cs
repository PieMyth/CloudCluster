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
        public void configurationFileExists_FileDoesNotExist()
        {
            // The following line returns the location of the .config file this test script reads from
            // string configFile = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).FilePath;
            
            string value = ConfigurationManager.AppSettings["TestValue"];
            Assert.False(string.IsNullOrEmpty(value), "No App.Config found.");
        }

        [Theory]
        [InlineData("invalidKey", "invalidConnection")]
        public void establishConnectionTest_connectionFailsAndThrows(string invalidKey, string invalidConnection)
        {
            Driver driver = new Driver(invalidKey, invalidConnection);
            Assert.Throws<UnauthorizedAccessException>(() => driver.establishConnection());
        }

        [Theory]
        [InlineData("AWS")]
        [InlineData("GCP")]
        [InlineData("Azure")]
        public void establishConnectionTest_connectionSuccessfullyEstablished(string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            Assert.True(driver.establishConnection());
        }

        [Theory]
        [InlineData("AWS")]
        [InlineData("GCP")]
        [InlineData("Azure")]
        public void getDatabaseTest_databaseSuccessfulyConnected(string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            Assert.True(driver.getDatabase());
        }

        [Theory]
        [InlineData(_listings, "AWS")]
        [InlineData(_reviews, "AWS")]
        [InlineData(_listings, "GCP")]
        [InlineData(_reviews, "GCP")]
        [InlineData(_listings, "Azure")]
        [InlineData(_reviews, "Azure")]
        public void collectionExistsTest_returnsTrue(String validCollection, string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            driver.getDatabase();
            Assert.True(driver.collectionExists(validCollection));
        }

        [Theory]
        [InlineData(_listings, "AWS")]
        [InlineData(_reviews, "AWS")]
        [InlineData(_listings, "GCP")]
        [InlineData(_reviews, "GCP")]
        [InlineData(_listings, "Azure")]
        [InlineData(_reviews, "Azure")]
        public void getCollectionTest_collectionSuccessfullyConnected(String validCollection, string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            driver.getDatabase();
            Assert.True(driver.getCollection(validCollection));
        }

        [Theory]
        [InlineData("invalidName", "AWS")]
        [InlineData("", "AWS")]
        [InlineData("invalidName", "GCP")]
        [InlineData("", "GCP")]
        [InlineData("invalidName", "Azure")]
        [InlineData("", "Azure")]
        public void collectionExistsTest_invalidValueReturnsFalse(String invalidCollection, string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            driver.getDatabase();
            Assert.False(driver.collectionExists(invalidCollection));
        }

        [Theory]
        [InlineData(null, "AWS")]
        [InlineData(null, "GCP")]
        [InlineData(null, "Azure")]
        public void collectionExistsTest_nullValueThrowsException(String nullValue, string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            driver.getDatabase();
            Assert.Throws<ArgumentNullException>(() => driver.collectionExists(nullValue));
        }

        [Theory]
        [InlineData("", "AWS")]
        [InlineData("invalidName", "AWS")]
        [InlineData("", "GCP")]
        [InlineData("invalidName", "GCP")]
        [InlineData("", "Azure")]
        [InlineData("invalidName", "Azure")]
        public void getCollectionTest_invalidNameConnectionFails(String invalidCollection, string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            driver.getDatabase();
            Assert.False(driver.getCollection(invalidCollection));
        }

        [Theory]
        [InlineData(null, "AWS")]
        [InlineData(null, "GCP")]
        [InlineData(null, "Azure")]
        public void getCollectionTest_nullvalueConnectionFails(String nullValue, string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            driver.getDatabase();
            Assert.Throws<ArgumentNullException>(() => driver.getCollection(nullValue));
        }

        [Theory]
        [InlineData("AWS")]
        [InlineData("GCP")]
        [InlineData("Azure")]
        public void queryCountDocumentsTest_countGreaterThanFiveThousand(string validKey)
        {
            string connectionString = ConfigurationManager.AppSettings.Get(validKey);
            Driver driver = new Driver(validKey, connectionString);
            driver.establishConnection();
            driver.getDatabase();
            driver.getCollection(_listings);
            Assert.True(driver.queryCountDocuments(_listings) > 5000);
        }
    }
}
