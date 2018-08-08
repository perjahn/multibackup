using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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

        public Dictionary<string, object> Tags { get; set; }


        public static BackupJob[] LoadBackupJobs(string[] jsonfiles)
        {
            List<BackupJob> backupjobs = new List<BackupJob>();

            foreach (var jsonfile in jsonfiles)
            {
                Log.Information("Reading: {Jsonfile}", jsonfile);
                string content = File.ReadAllText(jsonfile);
                dynamic json = JObject.Parse(content);

                Dictionary<string, string> filetags = new Dictionary<string, string>();
                if (json.tags != null)
                {
                    foreach (JProperty jsontag in json.tags)
                    {
                        filetags.Add(jsontag.Name, jsontag.Value.ToString());
                    }
                }

                if (json.backupjobs == null || json.backupjobs.Count == 0)
                {
                    Log.Error("Couldn't find any backupjobs in file: {Jsonfile}", jsonfile);
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
                        Log.Warning("Backup job {Index} missing type field, ignoring backup job.", backupjob.index.Value);
                        json.backupjobs[i].Remove();
                        continue;
                    }
                    if (backupjob.name == null)
                    {
                        Log.Warning("Backup job {Index} missing name field, ignoring backup job.", backupjob.index.Value);
                        json.backupjobs[i].Remove();
                        continue;
                    }
                    if (backupjob.zippassword == null)
                    {
                        Log.Warning("Backup job {Index} missing zippassword field, ignoring backup job.", backupjob.index.Value);
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
                    var tags = new Dictionary<string, object>();
                    foreach (var filetag in filetags)
                    {
                        tags.Add(filetag.Key, filetag.Value);
                    }


                    if (type == "sqlserver")
                    {
                        if (backupjob.connectionstring == null)
                        {
                            Log.Warning("Backup job {Index} ({Jobtype}, {Jobname}) is missing connectionstring field, ignoring backup job.",
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
                            Log.Warning("Backup job {Index} ({Jobtype}, {Jobname}) is missing connectionstring field, ignoring backup job.",
                                backupjob.index.Value, type, name);
                            json.backupjobs[i].Remove();
                            continue;
                        }
                        connectionstring = backupjob.connectionstring.Value;
                        if (backupjob.collection == null)
                        {
                            Log.Warning("Backup job {Index} ({Jobtype}, {Jobname}) is missing collection field, ignoring backup job.",
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
                            Log.Warning("Backup job {Index} ({Jobtype}, {Jobname}) is missing url field, ignoring backup job.",
                                backupjob.index.Value, type, name);
                            json.backupjobs[i].Remove();
                            continue;
                        }
                        url = backupjob.url.Value;
                        if (backupjob.key == null)
                        {
                            Log.Warning("Backup job {Index} ({Jobtype}, {Jobname}) is missing key field, ignoring backup job.",
                                backupjob.index.Value, type, name);
                            json.backupjobs[i].Remove();
                            continue;
                        }
                        key = backupjob.key.Value;
                    }
                    else
                    {
                        Log.Warning("Backup job {Index} ({Jobtype}, {Jobname}) has unsupported backup type, ignoring backup job.",
                            backupjob.index.Value, type, name);
                        json.backupjobs[i].Remove();
                        continue;
                    }


                    bool founddup = false;
                    for (int j = 0; j < i; j++)
                    {
                        if (json.backupjobs[j].name.Value == backupjob.name.Value && json.backupjobs[j].type.Value == backupjob.type.Value)
                        {
                            Log.Warning("Backup job {Index} ({Jobtype}, {Jobname}) duplicate name of {Duplicate}, ignoring backup job.",
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
                    Log.Error("Couldn't find any valid backup jobs in file: {Jsonfile}", jsonfile);
                    throw new Exception($"Couldn't find any valid backup jobs in file: '{jsonfile}'");
                }
            }

            Log.Information("Found {Backupjobs} valid backup jobs.", backupjobs.Count);

            return backupjobs.ToArray();
        }

        public static void LogBackupJobs(BackupJob[] backupjobs)
        {
            Log.Information("Backuping...");
            var typecounts = new Dictionary<string, int>();
            typecounts["sqlserver"] = 0;
            typecounts["cosmosdb"] = 0;
            typecounts["azurestorage"] = 0;
            for (int i = 0; i < backupjobs.Length; i++)
            {
                BackupJob backupjob = backupjobs[i];

                string type = backupjob.Type ?? "sqlserver";
                typecounts[type]++;

                using (new ContextLogger(backupjob.Tags))
                {
                    Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}", backupjob.Name, type);
                }
            }

            Log
                .ForContext("SqlServerCount", typecounts["sqlserver"])
                .ForContext("CosmosDBCount", typecounts["cosmosdb"])
                .ForContext("AzureStorageCount", typecounts["azurestorage"])
                .ForContext("TotalCount", backupjobs.Length)
                .Information("Backup counts");
        }
    }
}
