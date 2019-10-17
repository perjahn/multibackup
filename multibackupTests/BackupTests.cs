using NUnit.Framework;
using System.Collections.Generic;

namespace multibackup.Tests
{
    public class BackupTests
    {
        [Test]
        public void TestExcludeBackupJobs()
        {
            var results = new[] { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4 };

            for (int i = 0; i < 16; i++)
            {
                bool backupSqlServer = i / 8 % 2 == 1;
                bool backupCosmosDB = i / 4 % 2 == 1;
                bool backupMongoDB = i / 2 % 2 == 1;
                bool backupAzureStorage = i % 2 == 1;

                var backupjobs = GenerateBackupJobs();

                BackupJob.ExcludeBackupJobs(backupjobs, backupSqlServer, backupCosmosDB, backupMongoDB, backupAzureStorage);

                Assert.AreEqual(results[i], backupjobs.Count, $"i: {i}, backupSqlServer: {backupSqlServer}, backupCosmosDB: {backupCosmosDB}, backupMongoDB: {backupMongoDB}, backupAzureStorage: {backupAzureStorage}");
            }
        }

        List<BackupJob> GenerateBackupJobs()
        {
            var backupjobs = new List<BackupJob>
            {
                new BackupSqlServer
                {
                    Name = string.Empty,
                    Tags = new Dictionary<string, object> { [string.Empty] = string.Empty },
                    TargetAccount = string.Empty,
                    TargetCertfile = string.Empty,
                    TargetServer = string.Empty,
                    Zipfile = string.Empty,
                    ZipPassword = string.Empty,
                    Type = BackupType.SqlServer,
                    ConnectionString = string.Empty
                },
                new BackupCosmosDB
                {
                    Name = string.Empty,
                    Tags = new Dictionary<string, object> { [string.Empty] = string.Empty },
                    TargetAccount = string.Empty,
                    TargetCertfile = string.Empty,
                    TargetServer = string.Empty,
                    Zipfile = string.Empty,
                    ZipPassword = string.Empty,
                    Type = BackupType.CosmosDB,
                    ConnectionString = string.Empty,
                    Collection = string.Empty
                },
                new BackupMongoDB
                {
                    Name = string.Empty,
                    Tags = new Dictionary<string, object> { [string.Empty] = string.Empty },
                    TargetAccount = string.Empty,
                    TargetCertfile = string.Empty,
                    TargetServer = string.Empty,
                    Zipfile = string.Empty,
                    ZipPassword = string.Empty,
                    Type = BackupType.MongoDB,
                    ConnectionString = string.Empty
                },
                new BackupAzureStorage
                {
                    Name = string.Empty,
                    Tags = new Dictionary<string, object> { [string.Empty] = string.Empty },
                    TargetAccount = string.Empty,
                    TargetCertfile = string.Empty,
                    TargetServer = string.Empty,
                    Zipfile = string.Empty,
                    ZipPassword = string.Empty,
                    Type= BackupType.AzureStorage,
                    Url = string.Empty,
                    Key = string.Empty
                }
            };

            return backupjobs;
        }
    }
}
