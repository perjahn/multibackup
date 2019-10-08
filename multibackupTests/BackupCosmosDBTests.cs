using multibackup;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;

namespace multibackup.Tests
{
    public class BackupCosmosDBTests
    {
        const string DocumentDBconnectionString = "AccountEndpoint=https://abc-def.documents.azure.com:443/;AccountKey=abcABC123==;Database=abc";
        const string MongoDBconnectionString = "mongodb://abc:abcABC123==@abc-def.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";

        [Test]
        public void IsDocumentDBTest()
        {
            string connstr = DocumentDBconnectionString;

            bool result = BackupCosmosDB.IsDocumentDB(connstr);

            Assert.IsTrue(result);
        }

        [Test]
        public void IsNotDocumentDBTest()
        {
            string connstr = MongoDBconnectionString;

            bool result = BackupCosmosDB.IsDocumentDB(connstr);

            Assert.IsFalse(result);
        }

        [Test]
        public void IsMongoDBTest()
        {
            string connstr = MongoDBconnectionString;

            bool result = BackupCosmosDB.IsMongoDB(connstr);

            Assert.IsTrue(result);
        }

        [Test]
        public void IsNotMongoDBTest()
        {
            string connstr = DocumentDBconnectionString;

            bool result = BackupCosmosDB.IsMongoDB(connstr);

            Assert.IsFalse(result);
        }
    }
}