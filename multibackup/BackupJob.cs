using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace multibackup
{
    class BackupJob
    {
        public string Type { get; set; }
        public string Name { get; set; }

        public string ConnectionString { get; set; }
        public string Collection { get; set; }
        public string Url { get; set; }
        public string Key { get; set; }
        public string ZipPassword { get; set; }

        public Dictionary<string, string> Tags { get; set; }


        public static BackupJob[] LoadBackupJobs(string jsonfile)
        {
            Log.Information("Reading: {jsonfile}", jsonfile);
            string content = File.ReadAllText(jsonfile);
            dynamic json = JObject.Parse(content);


            List<BackupJob> backupjobs = new List<BackupJob>();

            if (json.backupjobs == null || json.backupjobs.Count == 0)
            {
                Log.Error("Couldn't find any backupjobs in file: {jsonfile}", jsonfile);
                throw new Exception($"Couldn't find any backupjobs in file: '{jsonfile}'");
            }

            for (int i = 0; i < json.backupjobs.Count; i++)
            {
                dynamic backupjob = json.backupjobs[i];
                backupjob.index = i;
            }

            for (int i = 0; i < json.backupjobs.Count;)
            {
                dynamic backupjob = json.backupjobs[i];

                if (backupjob.type == null)
                {
                    Log.Warning("Backup job {index} missing type field, ignoring backup job.", backupjob.index.Value);
                    json.backupjobs[i].Remove();
                    continue;
                }
                if (backupjob.name == null)
                {
                    Log.Warning("Backup job {index} missing name field, ignoring backup job.", backupjob.index.Value);
                    json.backupjobs[i].Remove();
                    continue;
                }
                if (backupjob.zippassword == null)
                {
                    Log.Warning("Backup job {index} missing zippassword field, ignoring backup job.", backupjob.index.Value);
                    json.backupjobs[i].Remove();
                    continue;
                }

                string type = backupjob.type.Value;
                string name = backupjob.name.Value;
                string connectionstring = null;
                string collection = null;
                string url = null;
                string key = null;
                string zippassword = backupjob.zippassword.Value;
                Dictionary<string, string> tags = new Dictionary<string, string>();


                if (type == "sql")
                {
                    if (backupjob.connectionstring == null)
                    {
                        Log.Warning("Backup job {index} ({type}, {name}) is missing connectionstring field, ignoring backup job.",
                            backupjob.index.Value, type, name);
                        json.backupjobs[i].Remove();
                        continue;
                    }
                    connectionstring = backupjob.connectionstring.Value;
                }
                else if (type == "cosmosdb")
                {
                    if (backupjob.connectionstring == null)
                    {
                        Log.Warning("Backup job {index} ({type}, {name}) is missing connectionstring field, ignoring backup job.",
                            backupjob.index.Value, type, name);
                        json.backupjobs[i].Remove();
                        continue;
                    }
                    connectionstring = backupjob.connectionstring.Value;
                    if (backupjob.collection == null)
                    {
                        Log.Warning("Backup job {index} ({type}, {name}) is missing collection field, ignoring backup job.",
                            backupjob.index.Value, type, name);
                        json.backupjobs[i].Remove();
                        continue;
                    }
                    collection = backupjob.collection.Value;
                }
                else if (type == "azurestorage")
                {
                    if (backupjob.url == null)
                    {
                        Log.Warning("Backup job {index} ({type}, {name}) is missing url field, ignoring backup job.",
                            backupjob.index.Value, type, name);
                        json.backupjobs[i].Remove();
                        continue;
                    }
                    url = backupjob.url.Value;
                    if (backupjob.key == null)
                    {
                        Log.Warning("Backup job {index} ({type}, {name}) is missing key field, ignoring backup job.",
                            backupjob.index.Value, type, name);
                        json.backupjobs[i].Remove();
                        continue;
                    }
                    key = backupjob.key.Value;
                }
                else
                {
                    Log.Warning("Backup job {index} ({type}, {name}) has unsupported backup type, ignoring backup job.",
                        backupjob.index.Value, type, name);
                    json.backupjobs[i].Remove();
                    continue;
                }


                bool founddup = false;
                for (int j = 0; j < i; j++)
                {
                    if (json.backupjobs[j].name.Value == backupjob.name.Value && json.backupjobs[j].type.Value == backupjob.type.Value)
                    {
                        Log.Warning("Backup job {index} ({type}, {name}) duplicate name of {duplicate}, ignoring backup job.",
                            backupjob.index.Value, type, name, json.backupjobs[j].index.Value);
                        json.backupjobs[i].Remove();
                        founddup = true;
                        break;
                    }
                }
                if (founddup)
                {
                    continue;
                }

                if (backupjob.tags != null)
                {
                    foreach (JProperty jsontag in backupjob.tags)
                    {
                        tags.Add(jsontag.Name, jsontag.Value.ToString());
                    }
                }

                backupjobs.Add(new BackupJob()
                {
                    Type = type,
                    Name = name,
                    ConnectionString = connectionstring,
                    Collection = collection,
                    Url = url,
                    Key = key,
                    ZipPassword = zippassword,
                    Tags = tags
                });

                i++;
            }

            if (backupjobs.Count == 0)
            {
                Log.Error("Couldn't find any valid backup jobs in file: {jsonfile}", jsonfile);
                throw new Exception($"Couldn't find any valid backup jobs in file: '{jsonfile}'");
            }

            Log.Information("Found {backupjobs} valid backup jobs.", backupjobs.Count);

            return backupjobs.ToArray();
        }

        static string GetHashString(string value)
        {
            using (var crypto = new SHA256Managed())
            {
                return string.Concat(crypto.ComputeHash(Encoding.UTF8.GetBytes(value)).Select(b => b.ToString("x2")));
            }
        }

        public static void LogBackupJobs(BackupJob[] backupjobs)
        {
            Log.Information("Backuping...");
            var typecounts = new Dictionary<string, int>();
            for (int i = 0; i < backupjobs.Length; i++)
            {
                BackupJob backupjob = backupjobs[i];

                string type = backupjob.Type ?? "sql";
                if (typecounts.ContainsKey(type))
                {
                    typecounts[type]++;
                }
                else
                {
                    typecounts[type] = 1;
                }

                if (type == "sql")
                {
                    Log.Information("{BackupjobName}: {BackupjobType} {ConnectionstringHash} {ZippasswordHash}",
                        backupjob.Name, type, GetHashString(backupjob.ConnectionString), GetHashString(backupjob.ZipPassword));
                }
                else if (type == "cosmosdb")
                {
                    Log.Information("{BackupjobName}: {BackupjobType} {ConnectionstringHash} {ZippasswordHash}",
                        backupjob.Name, type, GetHashString(backupjob.ConnectionString), GetHashString(backupjob.ZipPassword));
                }
                else if (type == "azurestorage")
                {
                    Log.Information("{BackupjobName}: {BackupjobType} {StorageurlHash} {ZippasswordHash}",
                        backupjob.Name, type, GetHashString(backupjob.Url), GetHashString(backupjob.ZipPassword));
                }
            }
            Log.Information("Backup counts: sql: {sql}, cosmosdb: {cosmosdb}, azurestorage: {azurestorage}, total: {totalcount}",
                typecounts["sql"], typecounts["cosmosdb"], typecounts["azurestorage"], backupjobs.Length);
        }
    }
}
