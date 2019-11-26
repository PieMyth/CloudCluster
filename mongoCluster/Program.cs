using System;
using NLog;


namespace mongoCluster
{
    // The Program class handles the C# driver and queries from beginning to end
    class Program
    {
        // For logging
        private static Logger logger = LogManager.GetCurrentClassLogger();

        // Valid collections
        private const string _listings = "listings";
        private const string _reviews = "reviews";

        // Import boolean: true if importing data with C# instead of python script
        private const bool importData = false;

        // Main: a Driver instance is created, connected, and queries are run 
        static void Main(string[] args)
        { 
            Driver driver = new Driver();

            if (!connect(ref driver)) {
                logger.Fatal("Driver failed to connect to database. Exiting Program.");
                Environment.Exit(1);
            }

            // Import data to db if using C# to import data
            // by default set to false as data is already loaded with a python script
            if (importData)
            {
                Importer import = new Importer();
                if (!import.begin(ref driver)) { 
                    logger.Fatal("Data import failed. Exiting Program.");
                    Environment.Exit(1);
                }
            }

            // Run queries specific to the Listings collection if a successful connection is established
            if (driver.getCollection(_listings))
            {
                // Run a simple query that counts the total number of listings
                long totalListings = driver.queryCountDocuments(_listings);
                if (totalListings.Equals(0))
                {
                    logger.Fatal("Queries are not working as expected. Ending Program.");
                    Environment.Exit(1);
                }
                else 
                { 
                    logger.Info($"There are {totalListings} totalListings");
                }

                // Run a test query
                if (!driver.queryTest(_listings)) {
                    logger.Error("Test query failed");
                }

                // Query 1: A count query
                if (!driver.queryCount(_listings)) {
                    logger.Error("Query1: Count query failed");
                }

                // Query 2: Sorted Subset
                if (driver.querySortedSubset(_listings)) {
                    logger.Error("Query2: Sorted subset driver.query failed");
                }


                // Query 3: Subset-search
                if (driver.querySubsetSearch(_listings)) {
                    logger.Error("Query3: Subset search driver.query failed");
                }

                // Query 4: Average
                if (driver.queryAverage(_listings)) {
                    logger.Error("Query4: Average driver.query failed");
                }

                // Query 5: Join
                if (driver.queryJoin(_listings, _reviews)) {
                    logger.Error("Query5: Join driver.query failed");
                }

                // Query 6: Update
                if (driver.queryUpdate(_listings)) {
                    logger.Error("Query6: Update query failed");
                }

                // Keep terminal open when program finishes
                Console.ReadLine();
            }
        }

        /// <summary>create a C# driver for mongoDB that establishes a connection with the mongoDB database</summary>
        /// <param name="driver">A Driver instance</param>
        /// <returns>
        /// True if driver is successfully created AND database is succesfully connected
        /// False if an error occurred or an exception was thrown
        /// </returns>
        static bool connect(ref Driver driver)
        {
            try
            {
                if (driver.establishConnection() && driver.getDatabase())
                {
                    return true;
                }
            }
            catch (UnauthorizedAccessException)
            {
                logger.Error("\nConnection was not established. Please check configuration, credentials and connection details");
            }
            catch (NullReferenceException)
            {
                logger.Error("\nConnection was not established. The database may not exist.");
            }
            catch (ArgumentException)
            {
                logger.Error("\nConnection was not established. An invalid name was used for access.");
            }
            return false;
        }
    }
}
