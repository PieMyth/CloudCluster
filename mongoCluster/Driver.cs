using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using MongoDB.Driver;
using MongoDB.Bson;

using NLog;

namespace mongoCluster
{
    class Driver
    {
        // Logging
        private static Logger logger = LogManager.GetCurrentClassLogger();

        // Atlas cluster connection string
        private const String _connection = "mongodb+srv://testuser:testpw@cluster0-lgn2s.gcp.mongodb.net/test?retryWrites=true&w=majority";

        // database name
        private const String _dbName = "airbnb";

        // database (established connection if containing a non-null value)
        private IMongoDatabase _db;
        public IMongoDatabase Db
        {
            get => this._db;
            set => this._db = value;
        }

        private MongoClient _client;
        public MongoClient Client
        {
            get { return this._client; }
            set { this._client = value; }
        }

        private Dictionary<string, IMongoCollection<BsonDocument>> _collections;

        public Dictionary<string, IMongoCollection<BsonDocument>> Collections
        {
            get { return this._collections; }
        }

        public Driver()
        {
            _client = null;
            _db = null;
            _collections = null;
        }

        /// <summary>Establishes connection to database</summary>
        /// <returns>True if connection was established, False, otherwise</returns>
        public bool establishConnection()
        {
            return this._establishConnection();
        }

        /// <summary>Establishes connection to database</summary>
        /// <returns>True if connection was established, False, otherwise</returns>
        private bool _establishConnection()
        {
            try
            {
                this._client = new MongoClient(_connection);
            }
            catch (MongoConfigurationException err)
            {
                Console.WriteLine($"Configuration error: {err}");
                throw new UnauthorizedAccessException();
            }
            return true;
        }

        /// <summary>Accesses and returns a specified database</summary>
        /// <returns>True if database was successfully accessed, false otherwise</returns>
        public bool getDatabase()
        {
            return this._getDatabase();
        }

        /// <summary>Accesses and retrieves a specified collection</summary>
        /// <returns>True if successfully accessed, False, otherwise</returns>
        public bool getCollection(String collectionName)
        {
            return this._getCollection(collectionName);
        }

        /// <summary>A Query that counts the total number of documents in a collection</summary>
        /// <param name="collectionName">The string collection to query</param>
        /// <returns>A total count of documents of long type</returns>
        public long queryCountDocuments(String collectionName)
        {
            return this._queryCountDocuments(collectionName);
        }

        /// <summary>
        /// Run a test query on the Listings collection
        /// Queries for all listings of up to price_limit with a min_nights_limit 
        /// </summary>
        /// <param name="collectionName">The string collection to query</param>
        /// <returns>Returns the amount of documents from query</returns>
        public bool testQuery(String collectionName)
        {
            // a list of tuples containing (price_limit, min_nights_limit)
            List<Tuple<int, int>> testInputs = new List<Tuple<int, int>>();

            // values to test
            int[] priceLimits = new int[] { 20, 100, 700 };
            int[] minimumNights = new int[] { 1, 7, 31 };

            for (int i = 0; i < priceLimits.Length; i++) 
            { 
                testInputs.Add(Tuple.Create(priceLimits[i], minimumNights[i]));
            }

            // Run the query on each tuple 
            foreach (Tuple<int, int> input in testInputs)
            {
                Console.WriteLine('\n' + new string('-', 100) + '\n');
                logger.Debug($"Querying with price_limit={input.Item1} and min_nights_limit={input.Item2}");
                Console.WriteLine($"Querying with price_limit={input.Item1} and min_nights_limit={input.Item2}");

                var count = _testQuery(this._collections[collectionName], input.Item1, input.Item2);
                Console.WriteLine($"count: {count}");

                logger.Info($"Returned {count.Result} records!");
                Console.WriteLine($"Returned {count.Result} records!");
            }
            return true;
        }

        public bool countQuery(String collectionName)
        { 
            // Run query 1 - Count query
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 1 - Count query");
            // Using zipcode range (I'm only doing downtown portland zip codes)
            Task<long> countQueryResult = this._countQuery(this._collections[collectionName], 2, 97201, 97210);
            logger.Info($"There are {countQueryResult.Result} listings with over 2 bedrooms in zipcode range from 97201 - 972010.");

            // Using city name
            Task<long> otherCountQueryResult = this._countQuery(this._collections[collectionName], 2, city_limit: "Portland");
            logger.Info($"There are {otherCountQueryResult.Result} listings with over 2 bedrooms in the city of Portland.");
            return true;
        }

        /// <summary>Accesses and returns a specified database</summary>
        /// <returns>True if database was successfully accessed, false otherwise</returns>
        private bool _getDatabase()
        {
            try
            {
                this._db = this._client.GetDatabase(_dbName);
            }
            catch (ArgumentException err)
            {
                Console.WriteLine($"\nThe database name must be composed of valid characters:\n{err}");
                throw new ArgumentException();
            }

            if (this._db != null) { 
                Console.WriteLine($"\nConnection successfully established with database '{_dbName}'.");
                return true;
            }
            return false;
        }

        /// <summary>Accesses and returns a specified collection</summary>
        /// <returns>True if successfully accessed, False, otherwise</returns>
        private bool _getCollection(String collectionName)
        {
            try
            {
                // Adds a collection if it doesn't exist
                this._collections.TryAdd(collectionName, this._db.GetCollection<BsonDocument>(collectionName));
            }
            catch (ArgumentException err)
            {
                Console.WriteLine($"\nThe collection name must be composed of valid characters:\n{err}");
                throw new ArgumentException();
            }
            return true;
        }

        /// <summary>A Query that counts the total number of documents in a collection</summary>
        /// <param name="collectionName">The string collection to query</param>
        /// <returns>A total count of documents of long type</returns>
        private long _queryCountDocuments(String collectionName)
        {
            BsonDocument filter = new BsonDocument();
            return this._collections[collectionName].CountDocuments(filter);
        }

        /// <summary>
        /// Run a test query on the Listings collection
        /// Queries for all listings of up to price_limit with a min_nights_limit 
        /// </summary>
        /// <param name="collection">The collection name</param>
        /// <param name="price_limit">A maximum limit on the total price for the minimum number of nights</param>
        /// <param name="min_nights_limit">The minimum number of nights required in order to make the booking</param>
        /// <returns>Returns the amount of documents from query</returns>
        private async Task<long> _testQuery(IMongoCollection<BsonDocument> collection, int price_limit, int min_nights_limit)
        {
            long count = 0;

            Console.WriteLine($"Here are all of the {min_nights_limit}-night stays that cost at most ${price_limit}:");
            Console.WriteLine(new string('-', 50));

            // Create the query filter (WHERE)
            var b = Builders<BsonDocument>.Filter;
            var filter = b.Lte("price", price_limit) & b.Eq("minimum_nights_avg_ntm", min_nights_limit);

            // Create the projection (SELECT)
            // We can also use a builder here, but string is more consise and managable
            var projection = "{_id:0, id:1, price:1, neighbourhood_cleansed:1, accomodates:1, smart_location:1, minimum_nights_avg_ntm:1}";

            // Specify sorting
            SortDefinition<BsonDocument> sort = "{ price: 1 }";

            // Run the query asynchronously
            await collection.Find(filter)
                            .Project(projection)
                            .Sort(sort)
                            .ForEachAsync(document =>
                            {
                                count++;
                                var result = document.ToDictionary();
                                Console.Write($"Book out at listing #{result["id"]} in {result["neighbourhood_cleansed"]} - {result["smart_location"]}");
                                if (result.ContainsKey("accomodates"))
                                {
                                    Console.Write($", which accomodates {result["accomodates"]} people ");
                                }
                                Console.WriteLine($" for the lovely {result["minimum_nights_avg_ntm"]} price of ${result["price"]}!");
                            });
            return count;
        }

        // Query 1: Count Query
        // Find the number of listings available with greater than 2 bedrooms in Portland.
        /*  Implementation Strategy:
         *      1. Perform a filter where the city is equal to “Portland” ($eq) 
         *      2. Perform a secondary filter where the number of bedrooms is greater than 2 ($gt)
         */
       private async Task<long> _countQuery(IMongoCollection<BsonDocument> collection, int bedroom_limit, 
                                           int zipcode_start_limit = -1, int zipcode_end_limit = -1, string city_limit = "")
       {
           FilterDefinition<BsonDocument> filter;
           // Try to query by zipcode range
           if (zipcode_start_limit >= 0 && zipcode_end_limit > 0)
           {
               // Here's one way of doing the filter and...
               var b = Builders<BsonDocument>.Filter;
               filter = ( b.Gte("zipcode", zipcode_start_limit) 
                        & b.Lte("zipcode", zipcode_end_limit) 
                        & b.Gt("bedrooms", bedroom_limit));
           } 
           // Try to query by city name
           else if (city_limit != "")
           {
               // ... here's another way of doing the filter!
               filter = $"{{ city: {{$eq: \"{city_limit}\"}}" +
                        $", bedrooms: {{$gt: {bedroom_limit} }} }}";
           }
           // Invalid zipcode range and invalid city name
           else
           {
               throw new ArgumentException();
           }

           // Return the async task of running this query
           return await collection.CountDocumentsAsync(filter);
       }
    }
}
