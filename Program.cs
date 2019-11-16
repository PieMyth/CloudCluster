using MongoDB.Bson;
using MongoDB.Driver;

using System;
using System.Linq;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

using CsvHelper;
using MongoDB.Bson.Serialization;
using NLog;


namespace CS488
{
    class IntHolder
    {
        public int counter = 0;
    }

    class Program
    {
        // The format in which to print the current time
        const string date_fmt = "G";

        // For logging purposes
        private static Logger logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            gcp_connect(); // Warning bc I am not awaiting this function, but it's ok bc I'm not doing concurrency rn
            Console.ReadLine();
        }

        static async Task gcp_connect()
        {   
            var client = new MongoClient("mongodb+srv://username:password@server_ip/test?retryWrites=true&w=majority"); // Change this with the external IP of your primary instance!    D
            IMongoDatabase db = client.GetDatabase("airbnb"); // Creates the database if it doesn't already exist

            logger.Info("About to query...\n");
            // (collection_name, import_folder, import_chunk_size)
            IList<Tuple<string, string, int>> sources = new List<Tuple<string, string, int>>();
            sources.Add(Tuple.Create("Listings", @"C:/Users/Whyve/Documents/CS488/Dataset/CSV/Listings", 1000));
            sources.Add(Tuple.Create("Reviews", @"C:/Users/Whyve/Documents/CS488/Dataset/CSV/Reviews", 25000));

            IMongoCollection<BsonDocument> collection;
/*
            foreach (Tuple<string, string, int> src in sources)
            {
                collection = db.GetCollection<BsonDocument>(src.Item1);
                logger.Info($"Importing documets from '{src.Item2}'...");
                var count = import_folder(src.Item2, collection, src.Item3);
                logger.Info($"Imported {count.Result.counter} documents from {src.Item2}!");
            }
*/
            collection = db.GetCollection<BsonDocument>(sources[0].Item1);
            Console.WriteLine("Here are all the one-night stays that cost at most $20:");
            Console.WriteLine(new string('-', 50));
            var filter = "{price: {'$lte': 20}, minimum_nights_avg_ntm: 1}"; // Example of how you may query the db
            var projection = "{_id:0, id:1, price:1, neighbourhood_cleansed:1, accomodates:1, smart_location:1}";

            var result = await collection.Find(filter).Project(projection).ToListAsync();
            int result_count = 0;
            await collection.Find(filter)
                            .Project(projection)
                            .ForEachAsync(async document =>
                            {
                                result_count++;
                                var result = document.ToDictionary();
                                // Console.WriteLine($"{document.ToString()}");
                                Console.Write($"Book out at listing #{result["id"]} in {result["neighbourhood_cleansed"]} - {result["smart_location"]}");
                                if (result.ContainsKey("accomodates")) {
                                    Console.Write($", which accomodates {result["accomodates"]} people ");
                                }
                                Console.WriteLine($" for the lovely price of ${result["price"]}!");
                            });
            logger.Info($"Returned {result_count} records!");
        }

        static async Task<IntHolder> import_folder(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
        {
            // Check that src directory exists
            if (!System.IO.Directory.Exists(src))
            {
                throw new ArgumentException();
            }

            // Loop through all files in the directory
            var files = System.IO.Directory.EnumerateFiles(src);

            IntHolder total_count = new IntHolder();
            Regex pattern = new Regex(@"\.[a-zA-Z]+$");
            foreach (string file in files)
            {
                logger.Debug($"Importing file: {file}");
                int count = 0;
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
                        logger.Error($"Unable to import non-json file: {file}");
                        continue;
                }

                // Check amt of records read
                if (count > 0)
                {
                    logger.Info($"Successfully imported file: {file}");
                    lock (total_count)
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

        static async Task<int> import_json(string src, IMongoCollection<BsonDocument> collection)
        {
            string data = System.IO.File.ReadAllText(src);
            //logger.Debug(data);
            var document = BsonSerializer.Deserialize<BsonDocument>(data);
            int count = document.ElementCount;
            logger.Debug($"Translated {src} into a BSON document with {count} elements!");
            await collection.InsertOneAsync(document);
            logger.Debug($"Finished importing {src} into {collection.CollectionNamespace}");
            return count;
        }

        static async Task<int> import_csv(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
        {
            int count = 0;
            var documents = new List<BsonDocument>();

            using (var fin = new StreamReader(src))
            using (var csv = new CsvReader(fin))
            using (var cr = new CsvDataReader(csv))
            {
                var headers = new object[cr.FieldCount];
                var records = new object[cr.FieldCount];
                logger.Debug($"Parsed {cr.FieldCount} columns in file {src}");
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

                    if (count % import_chunk_size == 0) // Insert documents in batches of insert_amt size
                    {
                        await collection.InsertManyAsync(documents); // TODO -> use InsertManyAsync for concurrency?
                        logger.Debug($"Uploded {import_chunk_size} records to {collection.CollectionNamespace.CollectionName}...");
                        documents.Clear();
                    }
                }
                if (documents.Count != 0) // Insert remainder documents
                {
                    await collection.InsertManyAsync(documents);
                }
            }
            return count;
        }     
    }
}
