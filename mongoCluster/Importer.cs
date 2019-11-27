using System.Configuration;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;

using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

using System.Threading.Tasks;
using NLog;
using CsvHelper;


namespace mongoCluster
{
    class LongHolder
    {
        public long counter = 0;
    }

    class Importer
    {
        IList<Tuple<string, string, int>> sources = new List<Tuple<string, string, int>>();
        private string _listingsDir = ConfigurationManager.AppSettings.Get("listingsFolder");
        private string _reviewsDir = ConfigurationManager.AppSettings.Get("reviewsFolder");
        private const string _listingsCollectionName = "listings";
        private const string _reviewsCollectionName = "reviews";
        private static Logger logger = LogManager.GetCurrentClassLogger();  // for logging purposes


        public bool begin(ref Driver driver)
        {
            return _begin(ref driver);
        }
 
        private bool _begin(ref Driver driver)
        { 
            // Tuple contains (collection_name, import_folder, import_chunk_size)
            sources.Add(Tuple.Create(_listingsCollectionName, @_listingsDir, 1000));
            sources.Add(Tuple.Create(_reviewsCollectionName, @_reviewsDir, 25000));

            logger.Info("Importing Data");
            foreach (Tuple<string, string, int> src in sources)
            {
                // Set the collection name to the first element in the tuple
                if (!driver.getCollection(src.Item1))
                {
                    if (driver.addCollection(src.Item1))
                    {
                        logger.Info($"Creating collection: '{src.Item1}'");
                    } else
                    {
                        logger.Error($"Error: Failed to create collection: '{src.Item1}'!");
                        return false;
                    }
                }
                
                logger.Info($"Importing documents from '{src.Item2}'...");

                // Import all .json and .csv files from the location specified in the second element of the tuple
                // The third element of the tuple specifies the max amt of rows to be read at a time before inserting to db
                var count = importFolder(src.Item2, driver.Collections[src.Item1], src.Item3);
                if (count.Result.counter > 0)
                {
                    logger.Info($"Imported {count.Result.counter} documents from {src.Item2}!");
                } else { 
                    logger.Error($"Error: Failed to import from folder: {src.Item2}!");
                    Console.WriteLine("Did you configure App.configure to point to your import folders?\n");
                    Console.WriteLine("Did you remember to unzip the dataset folder?\n");
                    return false;
                }
                
            }
            return true;
        }

        // Import all .csv and .json files from the src string into the collection in batches of import_chunk_size
        static async Task<LongHolder> importFolder(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
        {
            // Check that src directory exists
            if (!System.IO.Directory.Exists(src))
            {
                logger.Error($"Error: Directory doesn't exist. Check if path is correct: {src}");
                throw new DirectoryNotFoundException();
            }

            // Loop through all files in the directory
            var files = System.IO.Directory.EnumerateFiles(src);

            LongHolder total_count = new LongHolder();

            // Use Regex to get the file extension
            Regex pattern = new Regex(@"\.[a-zA-Z]+$");
            foreach (string file in files)
            {
                logger.Info($"Importing file: {file}");
                long count = 0;
                // Check file format
                switch (pattern.Match(file).Value)
                {
                    case ".json":
                        count = await importJSON(file, collection); // TODO this is a waste of async :/
                        break;
                    case ".csv":
                        count = await importCSV(file, collection, import_chunk_size);
                        break;
                    default:
                        logger.Error($"Error: File extension not supported for importing: {file}");
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
                    logger.Error($"Error: Failed to import the file: {file}");
                }
            }
            return total_count;
        }

        // Read in an entire .json file as a string, turn it into a bson document, and import it all at once into the db
        static async Task<long> importJSON(string src, IMongoCollection<BsonDocument> collection)
        {
            string data = System.IO.File.ReadAllText(src);
            var document = BsonSerializer.Deserialize<BsonDocument>(data);
            long count = document.ElementCount;
            logger.Debug($"Translated {src} into a BSON document with {count} elements!");
            await collection.InsertOneAsync(document);
            logger.Debug($"Finished importing {src} into {collection.CollectionNamespace}");
            return count;
        }

        // Read in rows from a .csv file in a stream
        static async Task<long> importCSV(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
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
    }
}
