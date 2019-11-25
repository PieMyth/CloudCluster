using System;

namespace mongoCluster
{
    // The Program class handles the C# driver and queries from beginning to end
    class Program
    {
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
                Console.WriteLine("Driver failed to connect to database. Exiting Program.");
                Environment.Exit(1);
            }

            // Import data to db if using C# to import data
            // by default set to false as data is already loaded with a python script
            if (importData)
            {
                Importer import = new Importer();
                if (!import.begin(ref driver)) { 
                    Console.WriteLine("Data import failed. Exiting Program.");
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
                    Console.WriteLine("Queries are not working as expected. Ending Program.");
                    Environment.Exit(1);
                }
                else 
                { 
                    Console.WriteLine($"There are {totalListings} totalListings");
                }

                // Run a test query
                if (!driver.testQuery(_listings)) {
                    Console.WriteLine("Test query failed");
                }

                // Query 1: A count query
                if (!driver.countQuery(_listings)) {
                    Console.WriteLine("Count query failed");
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
                Console.WriteLine("\nConnection was not established. Please check configuration, credentials and connection details");
            }
            catch (NullReferenceException)
            {
                Console.WriteLine("\nConnection was not established. The database may not exist.");
            }
            catch (ArgumentException)
            {
                Console.WriteLine("\nConnection was not established. An invalid name was used for access.");
            }
            return false;
        }
    }
}
