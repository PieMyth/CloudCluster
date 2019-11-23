using System;
/*
using System.Linq;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

using MongoDB.Bson;
using MongoDB.Driver;

using CsvHelper;
using MongoDB.Bson.Serialization;
using NLog;
// !!! Via nuget in visual studios, install Nlog.Config to get the logger config file !!!
*/

namespace mongoCluster
{
    /*
    class LongHolder
    {
        public long counter = 0;
    }
    */

    class Program
    {
        // The format in which to print the current time
        /*
        const string date_fmt = "G";

        // For logging purposes
        private static Logger logger = LogManager.GetCurrentClassLogger();
        */

        static void Main(string[] args)
        {
            Console.WriteLine("Hello World Test");
            /*
            // Don't forget to whitelist your ip!
            // These should all be turned into constants
            // string connection_string = "mongodb+srv://username:password@cluster_ip/test?retryWrites=true&w=majority";
            string connection_string = "mongodb+srv://test:test@cluster_ip/test?retryWrites=true&w=majority";
            string db_name = "airbnb";
            bool do_import = true;   // true if importing data --> turn into constant
            
            // Wait until connection completes before proceeding, and then create db if it doesn't already exist
            MongoClient client = new MongoClient(connection_string);
            IMongoDatabase db = client.GetDatabase(db_name);


            // (collection_name, import_folder, import_chunk_size)
            IList<Tuple<string, string, int>> sources = new List<Tuple<string, string, int>>();
            sources.Add(Tuple.Create("Listings", @"C:/Users/mduer/git/CloudCluster/Listings", 1000));
            sources.Add(Tuple.Create("Reviews", @"C:/Users/mduer/git/CloudCluster/Reviews", 25000));

            // Import data to db from specified folders
            if (do_import)
            {
                IMongoCollection<BsonDocument> col;
                foreach (Tuple<string, string, int> src in sources)
                {
                    // Set the collection name to the first element in the tuple
                    col = db.GetCollection<BsonDocument>(src.Item1);
                    logger.Info($"Importing documets from '{src.Item2}'...");

                    // Import all .json and .csv files from the location specified in the second element of the tuple
                    // The third element of the tuple specifies the max amt of rows to be read at a time before inserting to db
                    var count = import_folder(src.Item2, col, src.Item3);
                    logger.Info($"Imported {count.Result.counter} documents from {src.Item2}!");
                }
            }

            IMongoCollection<BsonDocument> collection = db.GetCollection<BsonDocument>("Listings");

            // (price_limit, min_nights_limit)
            List<Tuple<int, int>> test_inputs = new List<Tuple<int, int>>();
            test_inputs.Add(Tuple.Create(20, 1));
            test_inputs.Add(Tuple.Create(100, 7));
            test_inputs.Add(Tuple.Create(700, 31));
            foreach (Tuple<int, int> input in test_inputs)
            {
                Console.WriteLine('\n' + new string('-', 100) + '\n');
                logger.Debug($"Querying with price_limit={input.Item1} and min_nights_limit={input.Item2}");
                var count = Test_Query(collection, input.Item1, input.Item2);
                logger.Info($"Returned {count.Result} records!");

            }

            // Run query 1 - Count query
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 1 - Count query");
            // Using zipcode range (I'm only doing downtown portland zip codes)
            Task<long> result_query_1 = run_query_1(collection, 2, 97201, 97210);
            logger.Info($"There are {result_query_1.Result} listings with over 2 bedrooms in zipcode range from 97201 - 972010.");

            // Using city name
            Task<long> other_result_query_1 = run_query_1(collection, 2, city_limit: "Portland");
            logger.Info($"There are {other_result_query_1.Result} listings with over 2 bedrooms in the city of Portland.");



            // Run query 2 - Sorted Subset
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 2 - Sorted Subset");
            run_query_2(collection);


            // Run query 3 - Subset-search
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 3 - Subset-search");
            run_query_3(collection);


            // Run query 4 - Average
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 4 - Average");
            run_query_4(collection);


            // Run query 5 - Join
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 5 - Join");
            run_query_5(collection);


            // Run query 6 - Update
            Console.WriteLine('\n' + new string('-', 100) + '\n');
            logger.Info("Query 6 - Update");
            run_query_6(collection);

            // Keep terminal open when program finishes
            Console.ReadLine();
        }
        */

            // Import all .csv and .json files from the src string into the collection in batches of import_chunk_size
            /*
            static async Task<LongHolder> import_folder(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
            {
                // Check that src directory exists
                if (!System.IO.Directory.Exists(src))
                {
                    throw new ArgumentException();
                }

                // Loop through all files in the directory
                var files = System.IO.Directory.EnumerateFiles(src);

                LongHolder total_count = new LongHolder();

                // Use Regex to get the file extension
                Regex pattern = new Regex(@"\.[a-zA-Z]+$");
                foreach (string file in files)
                {
                    logger.Debug($"Importing file: {file}");
                    long count = 0;
                    // Check file format
                    switch (pattern.Match(file).Value)
                    {
                        case ".json":
                            count = await import_json(file, collection); // TODO this is a waste of async :/
                            break;
                        case ".csv":
                            count = await import_csv(file, collection, import_chunk_size);
                            break;
                        default:
                            logger.Error($"File extension not supported for importing: {file}");
                            continue;
                    }

                    // Check amt of records read
                    if (count > 0)
                    {
                        logger.Info($"Successfully imported file: {file}");
                        lock (total_count) // This lock is useless right now... count can change! Use .ContinueWith()
                        {
                            total_count.counter += count;
                        }
                        logger.Debug($"count = {count}; total_count = {total_count.counter}");
                    }
                    else
                    {
                        logger.Error($"Failed to import the file: {file}");
                    }
                }
                return total_count;
            }
            */

            // Read in an entire .json file as a string, turn it into a bson document, and import it all at once into the db
            /*
            static async Task<long> import_json(string src, IMongoCollection<BsonDocument> collection)
            {
                string data = System.IO.File.ReadAllText(src);
                var document = BsonSerializer.Deserialize<BsonDocument>(data);
                long count = document.ElementCount;
                logger.Debug($"Translated {src} into a BSON document with {count} elements!");
                await collection.InsertOneAsync(document);
                logger.Debug($"Finished importing {src} into {collection.CollectionNamespace}");
                return count;
            }
            */

            // Read in rows from a .csv file in a stream
            /*
            static async Task<long> import_csv(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
            {
                long count = 0;
                var documents = new List<BsonDocument>();

                using (var fin = new StreamReader(src))
                using (var csv = new CsvReader(fin))
                using (var cr = new CsvDataReader(csv))
                {
                    // Create utility arrays that can hold all elements of the header row and any given data row
                    int amt_cols = cr.FieldCount;
                    Debug.Assert(amt_cols > 0);
                    var headers = new object[amt_cols];
                    var records = new object[amt_cols];
                    logger.Debug($"Parsed {amt_cols} columns in file {src}");
                    var ret = cr.GetValues(headers);
                    Debug.Assert(ret > 0);
                    logger.Debug($"Returned {ret} : " + string.Join(',', headers));

                    // Keep reading until EOF
                    while (cr.Read())
                    {
                        // Read record line
                        ret = cr.GetValues(records);
                        Debug.Assert(ret > 0);

                        // Create a dictionary mapping each header element to its respective record element
                        // Weed out any empty string elements
                        var zipped = headers.Zip(records, (h, r) => new { h, r } )
                                            .Where(item => item.r.ToString() != "")
                                            .ToDictionary(item => item.h, item => {
                                                int i;
                                                double d;
                                                string r = item.r.ToString();
                                                if (r.StartsWith('$')) r = r.Substring(1);
                                                if (int.TryParse(r, out i)) return i;
                                                if (double.TryParse(r, out d)) return d;
                                                return item.r;                           
                                            });

                        // Add dictionary to import buffer
                        documents.Add(zipped.ToBsonDocument());
                        ++count;

                        // Add documents in batches to the db
                        if (count % import_chunk_size == 0)
                        {
                            await collection.InsertManyAsync(documents);
                            logger.Debug($"Uploded {import_chunk_size} records to {collection.CollectionNamespace.CollectionName}...");
                            documents.Clear();
                        }
                    }

                    // Add any remaining docs to the db
                    if (documents.Count != 0)
                    {
                        await collection.InsertManyAsync(documents);
                    }
                }
                return count;
            }     
            */

            // Run a test query on the Listings collection
            // Queries for all listings of up to price_limit with a min_nights_limit 
            // Returns the amount of documents from query
            /*
            static async Task<long> Test_Query(IMongoCollection<BsonDocument> collection, int price_limit, int min_nights_limit)
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
            */

            // Query 1: Count Query
            // Find the number of listings available with greater than 2 bedrooms in Portland.
            /*  Implementation Strategy:
             *      1. Perform a filter where the city is equal to “Portland” ($eq) 
             *      2. Perform a secondary filter where the number of bedrooms is greater than 2 ($gt)
             */
            /*
           static async Task<long> run_query_1(IMongoCollection<BsonDocument> collection, int bedroom_limit, 
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
               else if(city_limit != "")
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
           */

            //  Query 2: Sorted Subset
            //  Find the listings with the top 5 highest number of reviews for the entire dataset.
            /*  Implementation Strategy:
             *      1. Perform a sort ($sort) on the number_of_reviews decreasing (-1).
             *      2. Project a limited number of columns to add additional complexity.
             *      3. Limit to the top 5 results ($limit).
             */
            /*
           static async Task<long> run_query_2(IMongoCollection<BsonDocument> collection)
           {
               return 0;
           }
           */

            //  Query 3: Subset-search
            //  For all listings that are classified as a house that have been updated within a week, what percentage of these have a strict cancellation policy?
            /*  Implementation Strategy:
             *      1. Perform a filter on the data to only grab documents where property_type is equal to “House”.
             *      2. Perform another filter where the calendar_updated is less than one week. This will require more effort for string comparison when needing to consider values such as “today”, “6 days ago”, “one week ago”. 
             *      3. Capture the number of documents in this resulting set.
             *      4. Perform a grouping on the filtered set to group on the cancellation_policy and count using $group and $sum. This can then be formatted to get this count divided by the total number of documents to gather a percentage
             */
            /*
           static async Task<long> run_query_3(IMongoCollection<BsonDocument> collection)
           {
               return 0;
           }
           */

            //  Query 4: Average
            //  Find the average host response rate for listings with a price per night over $1000.
            /*  Implementation Strategy:
             *      1. Perform a filter to find listings with price greater than $1000 ($gt)
             *      2. Perform an average aggregation for the average host_response_rate
             */
            /*
           static async Task<long> run_query_4(IMongoCollection<BsonDocument> collection)
           {
               return 0;
           }
           */

            //  Query 5: Join
            //  Return the most recent review for all listings from Portland with greater than 3 bedrooms that is also a house.
            /*  Implementation Strategy:
             *      1. Perform a filter on the data to find all listings with city equal to “Portland” and greater than 3 bedrooms. 
             *      2. Perform a lookup between the listings and reviews tables with id and listing_id as the lookup criteria.
             *      3. For each host id, return the max date from the reviews ($max). (This possibly can be completed before the lookup for efficiency)
             */
            /*
           static async Task<long> run_query_5(IMongoCollection<BsonDocument> collection)
           {
               return 0;
           }
           */

            //  Query 6: Updating
            //  Update all listings that have more than 2 bedrooms and more than 2 bathrooms from Portland to require guest phone verification.
            /*  Implementation Strategy:
             *      1. Perform an updateMany on all listings from Portland with greater than 2 bedrooms and 2 bathrooms to set the require_guest_phone_verification field as true. 
             */
            /*
           static async Task<long> run_query_6(IMongoCollection<BsonDocument> collection)
           {
               return 0;
           }
            */
        }
    }
}
