using System;

namespace mongoCluster
{
    class Program
    {
        private const string _listings = "listings";
        private const string _reviews = "reviews";
        private const bool importData = false;   // true if importing data with C# instead of python script

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

            if (driver.getCollection(_listings))
            {
                if (!driver.testQuery()) {
                    Console.WriteLine("Test query failed");
                }

                if (!driver.countQuery()) {
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
