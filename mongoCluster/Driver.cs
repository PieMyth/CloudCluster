using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using MongoDB.Driver;
using MongoDB.Bson;

using NLog;

namespace mongoCluster
{
    // Driver class serves as a C# driver for running mongoDB queries
    public class Driver
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

        // the mongo client
        private MongoClient _client;
        public MongoClient Client
        {
            get { return this._client; }
            set { this._client = value; }
        }

        // dictionary of collections where the key="collectionName" and value is an IMongoCollection
        // created through a successful connection to the collection
        private Dictionary<string, IMongoCollection<BsonDocument>> _collections;

        public Dictionary<string, IMongoCollection<BsonDocument>> Collections
        {
            get { return this._collections; }
        }

        /// <summary>Default constructor</summary>
        public Driver()
        {
            _client = null;
            _db = null;
            _collections = new Dictionary<string, IMongoCollection<BsonDocument>>();
        }

        /// <summary>Establishes connection to database</summary>
        /// <returns>True if connection was established, False, otherwise</returns>
        public bool establishConnection()
        {
            return this._establishConnection();
        }

        /// <summary>Accesses and returns a specified database</summary>
        /// <returns>True if database was successfully accessed, false otherwise</returns>
        public bool getDatabase()
        {
            return this._getDatabase();
        }

        /// <summary>Check if a collection exists</summary>
        /// <returns>True if collection exists, False otherwise</returns>
        public bool collectionExists(String collectionName)
        {
            return _collectionExists(collectionName);
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
        public bool queryTest(String collectionName)
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
                var count = _queryTest(this._collections[collectionName], input.Item1, input.Item2);
                logger.Info($"Returned {count.Result} records!");
            }
            return true;
        }

        /// <summary>Query 1: Find the number of listings available with greater than 2 bedrooms in Portland.</summary>
        /// <param name="collectionName">String name of collection</param>
        /// <returns>True if successful, False otherwise</returns>
        public bool queryCount(String collectionName)
        { 
            // Run query 1 - Count query
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 1 - Count query");
            // Using zipcode range (I'm only doing downtown portland zip codes)
            Task<long> queryCountResult = this._queryCount(this._collections[collectionName], 2, 97201, 97210);
            logger.Info($"There are {queryCountResult.Result} listings with over 2 bedrooms in zipcode range from 97201 - 972010.");

            // Using city name
            Task<long> otherCountQueryResult = this._queryCount(this._collections[collectionName], 2, city_limit: "Portland");
            logger.Info($"There are {otherCountQueryResult.Result} listings with over 2 bedrooms in the city of Portland.");
            return true;
        }

        /// <summary>
        /// Query 2: Sorted Subset
        /// Find the listings with the top 5 highest number of reviews for the entire dataset.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public bool querySortedSubset(String collectionName)
        {
            return _querySortedSubset(collectionName);
        }

        /// <summary>
        /// Query 3: Subset-search
        /// For all listings that are classified as a house that have been updated within a week, what percentage of these have a strict cancellation policy?
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public bool querySubsetSearch(String collectionName)
        { 
            return _querySubsetSearch(collectionName);
        }

        /// Query 4: Average
        /// Find the average host response rate for listings with a price per night over $1000.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public bool queryAverage(String collectionName)
        { 
            return _queryAverage(collectionName);
        }

        /// <summary>
        /// Query 5: Join
        /// Return the most recent review for all listings from Portland with greater than 3 bedrooms that is also a house.
        /// </summary>
        /// <param name="firstCollection">String representing one collection</param>
        /// <param name="secondCollection">String representing a second collection</param>
        public bool queryJoin(String firstCollection, String secondCollection)
        { 
            return _queryJoin(firstCollection, secondCollection);
        }

        /// <summary>
        /// Query 6: Update
        /// Update all listings that have more than 2 bedrooms and more than 2 bathrooms from Portland to require guest phone verification.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public bool queryUpdate(String collectionName)
        {
            return _queryUpdate(collectionName);
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
                logger.Error($"Configuration error: {err}");
                throw new UnauthorizedAccessException();
            }
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
                return true;
            }
            return false;
        }

        /// <summary>Check if a collection exists</summary>
        /// <returns>True if collection exists, False otherwise</returns>
        private bool _collectionExists(String collectionName)
        {
            // modified from https://stackoverflow.com/questions/25017219/how-to-check-if-collection-exists-in-mongodb-using-c-sharp-driver
            BsonDocument filter = new BsonDocument("name", collectionName);
            var collections = new ListCollectionNamesOptions { Filter = filter };
            return _db.ListCollectionNames(collections).Any();
        }

        /// <summary>Accesses and returns a specified collection</summary>
        /// <returns>True if successfully accessed, False, otherwise</returns>
        private bool _getCollection(String collectionName)
        {
            if (!this._collectionExists(collectionName))
            {
                try
                {
                    // Adds a collection if it doesn't exist
                    this._collections.TryAdd(collectionName, this._db.GetCollection<BsonDocument>(collectionName));
                }
                catch (ArgumentNullException err)
                {
                    logger.Error($"\nThe collection name must be composed of valid characters:\n{err}");
                    throw new ArgumentNullException();
                }
            }
            if (!this._collectionExists(collectionName))
                return false;
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
        private async Task<long> _queryTest(IMongoCollection<BsonDocument> collection, int price_limit, int min_nights_limit)
        {
            long count = 0;

            logger.Info($"Here are all of the {min_nights_limit}-night stays that cost at most ${price_limit}:");
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
                                    Console.WriteLine($", which accomodates {result["accomodates"]} people ");
                                }
                                Console.WriteLine($" for the lovely {result["minimum_nights_avg_ntm"]} price of ${result["price"]}!");
                            });
            return count;
        }

        /// <summary>Query 1: Find the number of listings available with greater than 2 bedrooms in Portland.</summary>
        /// <param name="collection">String name of collection</param>
        /// <param name="bedroom_limit">Integer representing maximum bedrooms</param>
        /// <param name="zipcode_start_limit">Integer representing starting zipcode range</param>
        /// <param name="zipcode_end_limit">Integer representing ending zipcode range</param>
        /// <param name="city_limit">String defining a city as its own limit e.g. "Portland"</param>
        /// <returns>A long integer of the result count</returns>
        private async Task<long> _queryCount(IMongoCollection<BsonDocument> collection, int bedroom_limit, 
                                           int zipcode_start_limit = -1, int zipcode_end_limit = -1, string city_limit = "")
        {
            /*  Implementation Strategy:
            *      1. Perform a filter where the city is equal to “Portland” ($eq) 
            *      2. Perform a secondary filter where the number of bedrooms is greater than 2 ($gt)
            */
            FilterDefinition<BsonDocument> filter;

            // Try to query by zipcode range
            if (zipcode_start_limit >= 0 && zipcode_end_limit > 0)
            {
                // Here's one way of doing the filter and...
                var b = Builders<BsonDocument>.Filter;
                filter = (b.Gte("zipcode", zipcode_start_limit) &
                          b.Lte("zipcode", zipcode_end_limit) &
                          b.Gt("bedrooms", bedroom_limit));
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

        /// <summary>
        /// Query 2: Sorted Subset
        /// Find the listings with the top 5 highest number of reviews for the entire dataset.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        private bool _querySortedSubset(String collectionName)
        {
            /*  Implementation Strategy:
            *      1. Perform a sort ($sort) on the number_of_reviews decreasing (-1).
            *      2. Project a limited number of columns to add additional complexity.
            *      3. Limit to the top 5 results ($limit).
            */
            // TODO: stub
            return false;
        }


        /// <summary>
        /// Query 3: Subset-search
        /// For all listings that are classified as a house that have been updated within a week, 
        /// what percentage of these have a strict cancellation policy?
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        private bool _querySubsetSearch(String collectionName)
        {
            /*  Implementation Strategy:
            *      1. Perform a filter on the data to only grab documents where property_type is equal to “House”.
            *      2. Perform another filter where the calendar_updated is less than one week. 
            *         This will require more effort for string comparison when needing to consider values such as “today”, “6 days ago”, “one week ago”. 
            *      3. Capture the number of documents in this resulting set.
            *      4. Perform a grouping on the filtered set to group on the cancellation_policy and count using $group and $sum. 
            *         This can then be formatted to get this count divided by the total number of documents to gather a percentage
            */
            // TODO: stub
            return false;
        }

        /// <summary>
        /// Query 4: Average
        /// Find the average host response rate for listings with a price per night over $1000.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        private bool _queryAverage(String collectionName)
        {
            /*  Implementation Strategy:
            *      1. Perform a filter to find listings with price greater than $1000 ($gt)
            *      2. Perform an average aggregation for the average host_response_rate
            */
            // TODO: stub
            return false;
        }

        /// <summary>
        /// Query 5: Join
        /// Return the most recent review for all listings from Portland with greater than 3 bedrooms that is also a house.
        /// </summary>
        /// <param name="firstCollection">String representing one collection</param>
        /// <param name="secondCollection">String representing a second collection</param>
        private bool _queryJoin(String firstCollection, String secondCollection)
        {
            /*  Implementation Strategy:
            *      1. Perform a filter on the data to find all listings with city equal to “Portland” and greater than 3 bedrooms. 
            *      2. Perform a lookup between the listings and reviews tables with id and listing_id as the lookup criteria.
            *      3. For each host id, return the max date from the reviews ($max). (This possibly can be completed before the lookup for efficiency)
            */
            // TODO: stub
            return false;
        }

        /// <summary>
        /// Query 6: Update
        /// Update all listings that have more than 2 bedrooms and more than 2 bathrooms from Portland to require guest phone verification.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        private bool _queryUpdate(String collectionName)
        { 
            /*  Implementation Strategy:
            *      1. Perform an updateMany on all listings from Portland 
            *         with greater than 2 bedrooms and 2 bathrooms to set the require_guest_phone_verification field as true. 
            */
            // TODO: stub
            return false;
        }
    }
}
