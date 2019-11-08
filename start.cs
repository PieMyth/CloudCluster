using MongoDB.Driver;

namespace mongo
{
    class Program
    {
        static void Main(string[] args)
        {
            var databaseName = "airbnb";
            var connectionString = "mongodb://cluster0-shard-00-00-lgn2s.gcp.mongodb.net:27017/";
            var client = new MongoClient(connectionString + databaseName);
        }
    }
}
