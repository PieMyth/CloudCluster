using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Reflection;
using System.Configuration;

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

        // Output folder for query results
        private const string _outputFolder = @"query_output";

        // Atlas cluster connection string
        private string _connection;

        public string Connection 
        {
            get { return this._connection; }
            set { this._connection = value; }
        }

        // database name
        private const string _dbName = "airbnb";

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

        // test boolean that turns on local tests if true, does not test if false
        private bool _test;

        /// <summary>Default constructor</summary>
        public Driver()
        {
            this._client = null;
            this._db = null;
            this._connection = ConfigurationManager.AppSettings.Get("connectionStrings");
            this._collections = new Dictionary<string, IMongoCollection<BsonDocument>>();
            this._test = false;
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
        public bool collectionExists(string collectionName)
        {
            return _collectionExists(collectionName);
        }

        /// <summary>Accesses and retrieves a specified collection</summary>
        /// <returns>True if successfully accessed, False, otherwise</returns>
        public bool getCollection(string collectionName)
        {
            return this._getCollection(collectionName);
        }

        /// <summary>Creates a new collection.</summary>
        /// <returns>True if successfully created, False, if collection exists or failed to be created</returns>
        public bool addCollection(string collectionName)
        {
            return this._addCollection(collectionName);
        }

        /// <summary>Deletes an existing collection.</summary>
        /// <returns>True if deleted collection, False, if otherwise</returns>
        public bool deleteCollection(string collectionName)
        {
            return this._deleteCollection(collectionName);
        }

        /// <summary>A Query that counts the total number of documents in a collection</summary>
        /// <param name="collectionName">The string collection to query</param>
        /// <returns>A total count of documents of long type</returns>
        public long queryCountDocuments(string collectionName)
        {
            return this._queryCountDocuments(collectionName);
        }

        /// <summary>
        /// Run a test query on the Listings collection
        /// Queries for all listings of up to price_limit with a min_nights_limit 
        /// </summary>
        /// <param name="collectionName">The string collection to query</param>
        /// <returns>Returns the amount of documents from query</returns>
        public bool queryTest(string collectionName)
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
        public bool queryCount(string collectionName)
        {
            // Prepare the external file to store this query's output
            FileInfo file = null;
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file for storing query output, clears out previous text
            using (StreamWriter fout =
                new StreamWriter(file.FullName))
            {
                string output;
                DateTime start;

                // Run query 1 - Count query
                Console.WriteLine('\n' + new string('-', 100) + '\n');
                output = "Query 1 - Count query";
                logger.Info(output);
                fout.WriteLine(output);

                // Using zipcode range (I'm only doing downtown portland zip codes)
                start = DateTime.UtcNow;
                Task<long> queryCountResult = this._queryCount(this._collections[collectionName], 2, 97201, 97210);
                output = $"There are {queryCountResult.Result} listings with over 2 bedrooms in zipcode range from 97201 - 972010.";
                output += $"\nQuery run time: {DateTime.UtcNow - start}";
                logger.Info(output);
                fout.WriteLine(output);

                // Using city name
                start = DateTime.UtcNow;
                Task<long> otherCountQueryResult = this._queryCount(this._collections[collectionName], 2, city_limit: "Portland");
                output = $"There are {otherCountQueryResult.Result} listings with over 2 bedrooms in the city of Portland.";
                output += $"\nQuery run time: {DateTime.UtcNow - start}";
                logger.Info(output);
                fout.WriteLine(output);
            }
            return true;
        }

        /// <summary>
        /// Query 2: Sorted Subset
        /// Find the listings with the top 5 highest number of reviews for the entire dataset.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public async Task<bool> querySortedSubset(string collectionName)
        {
            string queryName = "Query 2 - Sorted Subset";
            FileInfo file = null;
            DateTime start;

            // Prepare the external file to store this query's output
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file (clearing previous content if necessary)
            // Record start/end time metrics, query results to file
            using (StreamWriter fout = new StreamWriter(file.FullName))
            {
                start = this._startQueryMetrics(queryName, fout);

                // Run the query
                await this._querySortedSubset(this._collections[collectionName], fout);
                string result =  $"Query run time: {DateTime.UtcNow - start}";
                this._stopQueryMetrics(fout, start);
            }
            return true;
        }

        /// <summary>
        /// Query 3: Subset-search
        /// For all listings that are classified as a house that have been updated within a week, what percentage of these have a strict cancellation policy?
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public bool querySubsetSearch(string collectionName)
        { 
            string queryName = "Query 3 - Subset Search";
            FileInfo file = null;
            DateTime start;

            // Prepare the external file to store this query's output
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file (clearing previous content if necessary)
            // Record start/end time metrics, query results to file
            using (StreamWriter fout = new StreamWriter(file.FullName))
            {
                start = this._startQueryMetrics(queryName, fout);

                // Run the query
                this._querySubsetSearch(collectionName, fout);
                this._stopQueryMetrics(fout, start);
            }
            return true;
        }

        /// Query 4: Average
        /// Find the average host response rate for listings with a price per night over $1000.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public bool queryAverage(string collectionName)
        { 
            string queryName = "Query 4 - Average";
            FileInfo file = null;
            DateTime start;

            // Prepare the external file to store this query's output
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file (clearing previous content if necessary)
            // Record start/end time metrics, query results to file
            using (StreamWriter fout = new StreamWriter(file.FullName))
            {
                start = this._startQueryMetrics(queryName, fout);

                // Run the query
                this._queryAverage(collectionName, fout);
                this._stopQueryMetrics(fout, start);
            }
            return true;
        }

        /// <summary>
        /// Query 5: Update
        /// Update all listings that have more than 2 bedrooms and more than 2 bathrooms from Portland to require guest phone verification.
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        public bool queryUpdate(string collectionName)
        {
            string queryName = "Query 5 - Update";
            FileInfo file = null;
            DateTime start;

            // Prepare the external file to store this query's output
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file (clearing previous content if necessary)
            // Record start/end time metrics, query results to file
            using (StreamWriter fout = new StreamWriter(file.FullName))
            {
                start = this._startQueryMetrics(queryName, fout);

                // Run the query
                this._queryUpdate(collectionName, fout);
                this._stopQueryMetrics(fout, start);
            }
            return true;
        }

        /// <summary>
        /// Query 6: Join
        /// Return the most recent review for all listings from Portland with greater than 3 bedrooms that is also a house.
        /// </summary>
        /// <param name="firstCollection">String representing one collection</param>
        /// <param name="secondCollection">String representing a second collection</param>
        public bool queryJoin(String firstCollection, String secondCollection)
        {
            string queryName = "Query 6 - Join";
            FileInfo file = null;
            DateTime start;

            // Prepare the external file to store this query's output
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file (clearing previous content if necessary)
            // Record start/end time metrics, query results to file
            using (StreamWriter fout = new StreamWriter(file.FullName))
            {
                start = this._startQueryMetrics(queryName, fout);

                // Run the query
                this._queryJoin(this._collections[firstCollection], this._collections[secondCollection], fout);
                this._stopQueryMetrics(fout, start);
            }
            return true;
        }

        /// <summary>
        /// Query 7: Second Join
        /// Returns the listings of the person who has travelled the most.
        /// </summary>
        /// <param name="firstCollection">String representing one collection</param>
        /// <param name="secondCollection">String representing a second collection</param>
        public bool queryFrequentTraveller(String firstCollection, String secondCollection)
        {
            string queryName = "Query 7 - Another join";
            long count = -1;
            FileInfo file = null;
            DateTime start;

            // Prepare the external file to store this query's output
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file (clearing previous content if necessary)
            // Record start/end time metrics, query results to file
            using (StreamWriter fout = new StreamWriter(file.FullName))
            {
                start = this._startQueryMetrics(queryName, fout);

                // Run the query
                count = this._queryFrequentTraveller(this._collections[firstCollection], secondCollection, fout);
                if ( count < 0)
                {
                    this._stopQueryMetrics(fout, start);
                    return false;
                }
                this._stopQueryMetrics(fout, start);
                String result = $"Returned {count} documents!";
                logger.Info(result);
                fout.WriteLine(result);
            }
            return true;
        }

        /// <summary>Establishes connection to database</summary>
        /// <returns>True if connection was established, False, otherwise</returns>
        private bool _establishConnection()
        {
            try
            {
                this._client = new MongoClient(this._connection);
            }
            catch (MongoConfigurationException err)
            {
                logger.Error($"Error: Configuration error: {err}");
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
        private bool _collectionExists(string collectionName)
        {
            // modified from https://stackoverflow.com/questions/25017219/how-to-check-if-collection-exists-in-mongodb-using-c-sharp-driver
            BsonDocument filter = new BsonDocument("name", collectionName);
            var collections = new ListCollectionNamesOptions { Filter = filter };
            var result = false;

            try
            {
                result = _db.ListCollectionNames(collections).Any();

            }
            catch (MongoAuthenticationException)
            {
                throw new UnauthorizedAccessException();
            }
            return _db.ListCollectionNames(collections).Any();
        }

        /// <summary>Accesses and returns a specified collection</summary>
        /// <returns>True if successfully accessed, False, otherwise</returns>
        private bool _getCollection(string collectionName)
        {
            if (!this._collectionExists(collectionName))
                return false;

            try
            {
                // Adds a collection if it doesn't exist
                this._collections.TryAdd(collectionName, this._db.GetCollection<BsonDocument>(collectionName));
            }
            catch (ArgumentNullException err)
            {
                logger.Error($"\nError: The collection name must be composed of valid characters:\n{err}");
                throw new ArgumentNullException();
            }
            return true;
        }

        /// <summary>Creates a new collection.</summary>
        /// <returns>True if successfully created, False, if collection exists or failed to be created</returns>
        private bool _addCollection(string collectionName)
        {
            if (this._collectionExists(collectionName))
                return false;

            try
            {
                // Adds a collection if it doesn't exist
                this._collections.TryAdd(collectionName, this._db.GetCollection<BsonDocument>(collectionName));
            }
            catch (ArgumentNullException err)
            {
                logger.Error($"\nError: The collection name must be composed of valid characters:\n{err}");
                throw new ArgumentNullException();
            }
            return true;
        }

        /// <summary>Deletes an existing collection.</summary>
        /// <returns>True if successfully deleted, False, if otherwise</returns>
        private bool _deleteCollection(string collectionName)
        {
            if (!this._collectionExists(collectionName))
                return false;

            try
            {
                // Removes collection from local dict & from db
                if (!this._collections.Remove(collectionName))
                    return false;
                this._db.DropCollection(collectionName);
            }
            catch (ArgumentNullException err)
            {
                logger.Error($"\nError: The collection name must be composed of valid characters:\n{err}");
                throw new ArgumentNullException();
            }
            
            // Check that the collection doesn't exist anymore
            if (this._collectionExists(collectionName))
                return false;
            return true;
        }

        /// <summary>A Query that counts the total number of documents in a collection</summary>
        /// <param name="collectionName">The string collection to query</param>
        /// <returns>A total count of documents of long type</returns>
        private long _queryCountDocuments(string collectionName)
        {
            BsonDocument filter = new BsonDocument();
            if (!this._getCollection(collectionName))
                return 0;
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
                                Console.WriteLine($" for at least {result["minimum_nights_avg_ntm"]} people for the lovely price of ${result["price"]}!");
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
        /// <param name="collection">The collection containing the reviews</param>
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        private async Task _querySortedSubset(IMongoCollection<BsonDocument> collection, StreamWriter fout)
        {
            int count = 0;  // count top five reviewed results

            // Create index on review numbers
            string opDescription = "Creating new index on number_of_reviews, descending";
            logger.Info(opDescription);
            string indexToCreate = "number_of_reviews";
            _ = this._createIndexDescending(collection, indexToCreate);

            // Create filter
            FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Gte(indexToCreate, 0);

            // Record the result description
            string resultDescription = "Top five listings with sorted number of reviews:";
            logger.Info(resultDescription);

            // Run the query
            await collection.Find(filter)
                            .Limit(5)
                            .ForEachAsync(doc =>
                            {
                                count++;
                                var res = doc.ToDictionary();
                                logger.Info($"({count}) ID: {res["id"]}, number of reviews: {res[indexToCreate]}, " +
                                            $"Description: A {res["property_type"]} located in {res["city"]}, {res["state"]}.");
                            });
        }

        /// <summary>
        /// Query 3: Subset-search
        /// For all listings that are classified as a house that have been updated within a week, 
        /// what percentage of these have a strict cancellation policy?
        /// </summary>
        /// <param name="collectionName">String representing the collection</param>
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        private bool _querySubsetSearch(string collectionName, StreamWriter fout)
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
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        private bool _queryAverage(string collectionName, StreamWriter fout)
        {
            /*  Implementation Strategy:
            *      1. Perform a filter to find listings with price greater than $1000 ($gt)
            *      2. Perform an average aggregation for the average host_response_rate
            */
            // TODO: stub
            return false;
        }

        /// <summary>
        /// Query 5: Update
        /// Update all listings that have more than 2 bedrooms and more than 2 bathrooms from Portland to require guest phone verification.
        /// </summary>
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        /// <param name="collectionName">String representing the collection</param>
        private bool _queryUpdate(string collectionName, StreamWriter fout)
        {
            /*  Implementation Strategy:
            *      1. Perform an updateMany on all listings from Portland 
            *         with greater than 2 bedrooms and 2 bathrooms to set the require_guest_phone_verification field as true. 
            */
            // TODO: stub
            return false;
        }

        /// <summary>
        /// Query 6: Join
        /// Return the most recent review for all listings from Portland with greater than 3 bedrooms that is also a house.
        /// </summary>
        /// <param name="firstCollection">String representing one collection</param>
        /// <param name="secondCollection">String representing a second collection</param>
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        private bool _queryJoin(IMongoCollection<BsonDocument> firstCollection, IMongoCollection<BsonDocument> secondCollection, StreamWriter fout)
        {

            /*  Implementation Strategy:
           *      1. Perform a filter on the data to find all House listings with city equal to Portland and greater than 3 bedrooms. 
           *      2. Perform a lookup between the listings and reviews tables with id and listing_id as the lookup criteria.
           *      3. For each host id, return the max date from the reviews ($max). (This possibly can be completed before the lookup for efficiency)
           */


            // Prepare the external file to store this query's output
            System.IO.FileInfo file = null;
            if (!_prepareQueryOutput(MethodBase.GetCurrentMethod().Name, ref file))
                return false;

            // Open external file for storing query output, clears out previous text

            String output;
            DateTime start;

            Console.WriteLine('\n' + new string('-', 100) + '\n');
            output = "Query 5 - Join";
            start = DateTime.UtcNow;
            var aggregate = firstCollection.Aggregate().Match(new BsonDocument("city","Portland")).Match(new BsonDocument("bedrooms",new BsonDocument("$gt",3)))
                .Match(new BsonDocument("property_type","House"))
                .Lookup("reviews", "id", "listing_id", "result").Unwind(x => x["result"]).
                Group(BsonDocument.Parse("{'_id': '$id', 'host_name': {$first: '$host_name'}, 'result': {$first: '$result'}}"))
                .ToList();
            output += $"\nQuery run time: {DateTime.UtcNow - start}";
            var resultsCount = aggregate.Count;
            output += $"\nNumber of listings with their most recent review = {resultsCount} \n";
            logger.Info(output);
            fout.WriteLine(output);

            return true;
        }

        /// <summary>
        /// Query 7: Second Join
        /// Returns the listings of the person who has travelled the most.
        /// </summary>
        /// <param name="firstCollection">String representing one collection</param>
        /// <param name="secondCollection">String representing a second collection</param>
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        private long _queryFrequentTraveller(IMongoCollection<BsonDocument> firstCollection, String secondCollection, StreamWriter fout)
        {
            /* Implementation Strategy:
             *    1. Get the most common reviewer_id, reviewer name, and the listing id
             *    2. Merge with the listing by listing id and retrieve the city and neighborhood
             */
            long count = 0;
            String output = "";
            var query1 = firstCollection.Aggregate().Group(BsonDocument.Parse("{_id: '$reviewer_id', 'count': {$sum: 1}, 'reviewer_name': {'$min': '$reviewer_name'}}"))
                                                    .Sort("{ 'count': -1 }")
                                                    .Limit(1)
                                                    .Project("{'_id': 0, 'reviewer_id': '$_id', 'reviewer_name': 1}")
                                                    .First();

            if (!query1.Contains("reviewer_id") || !query1.Contains("reviewer_name"))
            {
                output = "Failed to retrieve most travelled reviewer!";
                logger.Error(output);
                fout.WriteLine(output);
                return -1;
            }
            int reviewerId = query1.GetValue("reviewer_id").ToInt32();
            String reviewerName = query1.GetValue("reviewer_name").ToString();

            var query2 = firstCollection.Aggregate().Match($"{{reviewer_id: {reviewerId}}}")
                                                    .Project("{ '_id': 0, 'reviewer_name': 1, 'reviewer_id': 1, 'listing_id': 1, 'date': 1 }")
                                                    .Sort("{ date: 1 }")
                                                    .Lookup(secondCollection, "listing_id", "id", "listings")
                                                    .Unwind("listings")
                                                    .Project("{ '_id':0, 'reviewer_name':1, 'date':1, 'location': {$concat: ['$listings.neighbourhood_cleansed', ', ', '$listings.smart_location']} }");

            output = reviewerName + " is the most frequent traveller!";
            Console.WriteLine(output);
            fout.WriteLine(output);
            var results = query2.ToList();
            foreach(BsonDocument doc in results)
            {
                count++;
                var result = doc.ToDictionary();
                output = $"[{count}]\t{result["date"]} - {result["location"]}";
                Console.WriteLine(output);
                fout.WriteLine(output);
            };
            return count;
        }

        /// <summary> Records the starting metrics of a query
        /// <param name="queryName">String name of query to record</param>
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        private DateTime _startQueryMetrics(string queryName, StreamWriter fout)
        { 
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            fout.WriteLine(queryName);
            DateTime now = DateTime.UtcNow;
            fout.WriteLine($"Query began at: {now}");
            return now;
        }

        /// <summary> Records the final metrics of a query</summary>
        /// <param name="fout">StreamWriter stream for for writing out query results</param>
        /// <paramref name="start"/>The DateTime time that the query started</param>
        private void _stopQueryMetrics(StreamWriter fout, DateTime start)
        {
            DateTime stop = DateTime.UtcNow;
            string output = $"\nQuery run time: {stop - start}";
            logger.Info($"{output}");
            fout.Write($"Query ended at: {stop}");
            fout.Write(output);
        }

        /// <summary>Appends a path segment to the current working directory</summary>
        /// <param name="pathName">The path segment to append</param>
        /// <returns>An absolute address to the cwd's + passed-in path segment</returns>
        private string _getOutputPath(string pathName)
        {  
            // The project root folder, "mongoCluster", is  ~/bin/Debug/netcoreapp3.0/<assemblyExecutable.exe
            return Path.Combine(
                    Path.Combine(
                        Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location),
                        "../../..")
                   , @pathName);
        }

        /// <summary>Creates the path to a file if it does not already exist</summary>
        /// <param name="filePath">Path to the file to create</param>
        /// <returns>True if path exists or was created successfully, False, otherwise</returns>
        private bool _createFile(ref FileInfo filePath)
        {
            try
            {
                // Does nothing if file already exists
                filePath.Directory.Create();
            }
            catch (IOException err)
            {
                logger.Error($"Error: Failed to create query output directory: {err}");
                return false;
            }
            return true;
        }

        /// <summary>Creates the directories and file for storing query output to an external file.</summary>
        /// <param name="file">A reference to the file path to create</param>
        /// <param name="functionName">The function name to store</param>
        /// <returns>True if created file, False, otherwise</returns>
        private bool _prepareQueryOutput(string functionName, ref FileInfo file)
        {
            // Create the absolute path to the output .txt file for this query
            string fileName = functionName + ".txt";
            string filePath = _getOutputPath(Path.Combine(_outputFolder, fileName));
            file = new FileInfo(_getOutputPath(filePath));

            // Create the directories for the output path if they do not already exist
            return this._createFile(ref file);
        }

        /// <summary>
        /// Create index. Note that this is an idempotent operation so calling it multiple times
        /// will not create any change except when the index is first created
        /// </summary>
        /// <param name="collection">The collection to index</param>
        /// <param name="newIndex">Name of index to create</param>
        /// <returns>
        /// A Task with result of the newly created index. 
        /// However, note that if the index already exists, then the "newIndex" value will not be returned
        /// </returns>
        private async Task<string> _createIndexDescending(IMongoCollection<BsonDocument> collection, string newIndex)
        { 
            var keys = Builders<BsonDocument>.IndexKeys.Descending(newIndex);
            var model = new CreateIndexModel<BsonDocument>(keys);
            var createIndex = await collection.Indexes.CreateOneAsync(model).ConfigureAwait(false);

            // TEST: If test is true, then all existing indexes can be listed to confirm the correct index exists
            if (this._test)
            {
                Task<string> indexes = this._listIndexes(collection);
                string result = indexes.Result;
                Console.WriteLine($"Indexes after creating {newIndex} \n{result}");
                logger.Info($"Indexes after creating {newIndex} \n{result}");
            }

            return createIndex;
        }

        /// <summary>List a collections available indexes</summary>
        /// <param name="collection">The collection containing the reviews</param>
        /// <returns>Default true for listing indexes</returns>
        private async Task<string> _listIndexes(IMongoCollection<BsonDocument> collection)
        {
            string indexes = "";

            using (var cursor = await collection.Indexes.ListAsync())
            {
                await cursor.ForEachAsync(doc => indexes += $"{doc.ToString()}\n");
            }
            return indexes;
        }
    }
}
