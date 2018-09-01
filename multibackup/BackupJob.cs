using Destructurama.Attributed;
using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace multibackup
{
    class BackupJob
    {
        public string Name { get; set; }
        [NotLogged]
        public string ZipPassword { get; set; }
        public Dictionary<string, object> Tags { get; set; }


        protected virtual void Export(string backuppath) => throw new NotImplementedException();

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


                    if (string.Compare(type, "sqlserver", false) == 0)
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
                    else if (string.Compare(type, "cosmosdb", false) == 0)
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
                    else if (string.Compare(type, "azurestorage", false) == 0)
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
                        if (json.backupjobs[j].name.Value == backupjob.name.Value)
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

                    BackupJob job;

                    if (string.Compare(type, "sqlserver", true) == 0)
                    {
                        job = new BackupSqlServer { ConnectionString = connectionstring };
                    }
                    else if (string.Compare(type, "cosmosdb", true) == 0)
                    {
                        job = new BackupCosmosDB { ConnectionString = connectionstring, Collection = collection };
                    }
                    else
                    {
                        job = new BackupAzureStorage { Url = url, Key = key };
                    }

                    job.Name = name;
                    job.ZipPassword = zippassword;
                    job.Tags = tags;

                    backupjobs.Add(job);

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
            var typecounts = new Dictionary<string, int>
            {
                ["SqlServer"] = 0,
                ["CosmosDB"] = 0,
                ["AzureStorage"] = 0
            };
            for (int i = 0; i < backupjobs.Length; i++)
            {
                BackupJob backupjob = backupjobs[i];

                string type = backupjob is BackupSqlServer ? "SqlServer" : backupjob is BackupCosmosDB ? "CosmosDB" : "AzureStorage";
                typecounts[type]++;

                using (new ContextLogger(backupjob.Tags))
                {
                    if (backupjob is BackupSqlServer)
                    {
                        Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, ConnectionString: {HashedConnectionString}, Zippassword: {HashedZippassword}",
                            backupjob.Name, type,
                            LogHelper.GetHashString((backupjob as BackupSqlServer).ConnectionString),
                            LogHelper.GetHashString(backupjob.ZipPassword));
                    }
                    else if (backupjob is BackupCosmosDB)
                    {
                        Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, ConnectionString: {HashedConnectionString}, Collection: {HashedCollection}, Zippassword: {HashedZippassword}",
                            backupjob.Name, type,
                            LogHelper.GetHashString((backupjob as BackupCosmosDB).ConnectionString), LogHelper.GetHashString((backupjob as BackupCosmosDB).Collection),
                            LogHelper.GetHashString(backupjob.ZipPassword));
                    }
                    else if (backupjob is BackupAzureStorage)
                    {
                        Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, Url: {HashedUrl}, Key: {HashedKey}, Zippassword: {HashedZippassword}",
                            backupjob.Name, type,
                            LogHelper.GetHashString((backupjob as BackupAzureStorage).Url), LogHelper.GetHashString((backupjob as BackupAzureStorage).Key),
                            LogHelper.GetHashString(backupjob.ZipPassword));
                    }
                }
            }

            Log
                .ForContext("SqlServerCount", typecounts["SqlServer"])
                .ForContext("CosmosDBCount", typecounts["CosmosDB"])
                .ForContext("AzureStorageCount", typecounts["AzureStorage"])
                .ForContext("TotalCount", backupjobs.Length)
                .Information("Backup counts");
        }

        public static void ExportBackups(BackupJob[] backupjobs, string backupfolder, string date, bool backupSqlServer, bool backupCosmosDB, bool backupAzureStorage, string zipfolder)
        {
            Stopwatch watch = Stopwatch.StartNew();

            Log.Information("Creating folder: {Backupfolder}", backupfolder);
            Directory.CreateDirectory(backupfolder);

            Log.Information("Creating folder: {Zipfolder}", zipfolder);
            Directory.CreateDirectory(zipfolder);

            foreach (var backupjob in backupjobs)
            {
                string backuppath;

                using (new ContextLogger(backupjob.Tags))
                {
                    Log.Logger = Log
                        .ForContext("Jobname", backupjob.Name)
                        .ForContext("Jobtype", backupjob is BackupSqlServer ? "SqlServer" : backupjob is BackupCosmosDB ? "CosmosDB" : "AzureStorage");

                    if (backupjob is BackupSqlServer)
                    {
                        backuppath = Path.Combine(backupfolder, $"sqlserver_{backupjob.Name}_{date}.bacpac");
                        if (backupSqlServer)
                        {
                            backupjob.Export(backuppath);
                        }
                    }
                    else if (backupjob is BackupCosmosDB)
                    {
                        backuppath = Path.Combine(backupfolder, $"cosmosdb_{backupjob.Name}_{date}.json");
                        if (backupCosmosDB)
                        {
                            backupjob.Export(backuppath);
                        }
                    }
                    else
                    {
                        backuppath = Path.Combine(backupfolder, $"azurestorage_{backupjob.Name}_{date}");
                        if (backupAzureStorage)
                        {
                            backupjob.Export(backuppath);
                        }
                    }


                    string zipfile = Path.ChangeExtension(Path.GetFileName(backuppath), ".7z");

                    string oldfolder = null;
                    try
                    {
                        oldfolder = Directory.GetCurrentDirectory();
                        Directory.SetCurrentDirectory(zipfolder);

                        backupjob.EncryptBackup(backuppath, zipfile);

                        if (File.Exists(zipfile))
                        {
                            Statistics.SuccessCount++;
                        }
                        else
                        {
                            Log.Warning("Backupjob failed");
                        }
                    }
                    finally
                    {
                        if (oldfolder != null)
                        {
                            Directory.SetCurrentDirectory(oldfolder);
                        }
                    }
                }
            }

            Log
                .ForContext("ElapsedMS", (long)watch.Elapsed.TotalMilliseconds)
                .Information("Done exporting");
        }

        public static void SyncBackups(string zipfolder, string targetServer, string targetAccount)
        {
            string rsyncbinary = Tools.RsyncBinary;

            string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(rsyncbinary)));
            string logfile = GetLogFileName(appfolder, "SyncBackups");

            string source = "/cygdrive/" + char.ToLower(zipfolder[0]) + zipfolder.Substring(2).Replace("\\", "/");
            string target = $"{targetAccount}@{targetServer}:.";

            string binfolder = Path.GetDirectoryName(rsyncbinary);
            string synccert = Path.Combine(Path.GetDirectoryName(binfolder), "synccert", "rsync_id_rsa.txt");

            string oldfolder = null;
            try
            {
                oldfolder = Directory.GetCurrentDirectory();
                Directory.SetCurrentDirectory(binfolder);

                for (int tries = 1; tries <= 5; tries++)
                {
                    using (new ContextLogger(new Dictionary<string, object>() { ["Tries"] = tries }))
                    {
                        string[] files = Directory.GetFiles(zipfolder);

                        Log
                            .ForContext("FileCount", files.Length)
                            .Information("Syncing backup files: {Source} -> {Target}", source, target);

                        Stopwatch watch = Stopwatch.StartNew();

                        string args = $"--checksum --remove-source-files -a -l -e './ssh -o StrictHostKeyChecking=no -i {synccert}' {source} {target} --log-file {logfile}";

                        int result = RunCommand(Path.GetFileName(rsyncbinary), args);
                        watch.Stop();
                        Statistics.SyncTime += watch.Elapsed;
                        long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                        if (new FileInfo(logfile).Length > 0)
                        {
                            Log.Information("Reading logfile: {Logfile}", logfile);
                            string[] rows = File.ReadAllLines(logfile).Where(l => !l.Contains(".d..t...... ") && !l.Contains("<f..t...... ")).ToArray();
                            Log.ForContext("LogfileContent", LogHelper.TruncateLogFileContent(rows)).Information("rsync results");
                        }

                        Log.Information("Deleting logfile: {Logfile}", logfile);
                        File.Delete(logfile);

                        if (result == 0)
                        {
                            Log
                                .ForContext("ElapsedMS", elapsedms)
                                .ForContext("Zipfolder", zipfolder)
                                .Information("Sync success");
                            return;
                        }
                        else
                        {
                            Log
                                .ForContext("Binary", rsyncbinary)
                                .ForContext("Commandargs", LogHelper.Mask(args, new[] { targetServer, targetAccount }))
                                .ForContext("Result", result)
                                .ForContext("ElapsedMS", elapsedms)
                                .ForContext("Zipfolder", zipfolder)
                                .Warning("Sync fail");
                        }
                    }
                }
            }
            finally
            {
                if (oldfolder != null)
                {
                    Directory.SetCurrentDirectory(oldfolder);
                }
            }

            Statistics.SuccessCount = 0;
        }

        void EncryptBackup(string backuppath, string zipfile)
        {
            string sevenzipbinary = Tools.SevenzipBinary;

            if ((this is BackupSqlServer || this is BackupCosmosDB) && !File.Exists(backuppath))
            {
                Log.Warning("Backup file not found, ignoring: {Backuppath}", backuppath);
                return;
            }
            if (this is BackupAzureStorage && !Directory.Exists(backuppath))
            {
                Log.Warning("Backup folder not found, ignoring: {Backuppath}", backuppath);
                return;
            }

            Stopwatch watch = Stopwatch.StartNew();

            string compression = this is BackupSqlServer ? "-mx0" : "-mx9";

            string args = $"a {compression} {zipfile} {backuppath} -sdel -mhe -p{ZipPassword}";

            int result = RunCommand(sevenzipbinary, args);
            watch.Stop();
            Statistics.ZipTime += watch.Elapsed;
            long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

            if (result == 0 && File.Exists(zipfile) && new FileInfo(zipfile).Length > 0)
            {
                long size = new FileInfo(zipfile).Length;
                long sizemb = size / 1024 / 1024;
                Statistics.CompressedSize += size;
                Log
                    .ForContext("ElapsedMS", elapsedms)
                    .ForContext("Zipfile", zipfile)
                    .ForContext("Size", size)
                    .ForContext("SizeMB", sizemb)
                    .Information("Zip success");
            }
            else
            {
                Log
                    .ForContext("Binary", sevenzipbinary)
                    .ForContext("Commandargs", LogHelper.Mask(args, ZipPassword))
                    .ForContext("Result", result)
                    .ForContext("ElapsedMS", elapsedms)
                    .ForContext("Zipfile", zipfile)
                    .Warning("Zip fail");
            }
        }

        protected static string GetLogFileName(string appfolder, string jobname)
        {
            string logfolder = Path.Combine(appfolder, "logs");
            if (!Directory.Exists(logfolder))
            {
                Directory.CreateDirectory(logfolder);
            }

            string logfile = Path.Combine(logfolder, $"{jobname}_{DateTime.UtcNow:yyyyMMdd}.log");
            if (File.Exists(logfile))
            {
                Log.Information("Deleting old logfile: {Logfile}", logfile);
                File.Delete(logfile);
            }
            return logfile;
        }

        protected void RobustDelete(string folder)
        {
            if (Directory.Exists(folder))
            {
                string[] files = Directory.GetFiles(folder, "*", SearchOption.AllDirectories);
                foreach (string filename in files)
                {
                    try
                    {
                        File.SetAttributes(filename, File.GetAttributes(filename) & ~FileAttributes.ReadOnly);
                    }
                    catch (Exception ex) when (ex is UnauthorizedAccessException || ex is IOException)
                    {
                        // Will be dealt with when deleting the folder.
                    }
                }

                for (int tries = 1; tries <= 10; tries++)
                {
                    Log
                        .ForContext("Tries", tries)
                        .Information("Deleting folder: {Folder}", folder);
                    try
                    {
                        Directory.Delete(folder, true);
                        return;
                    }
                    catch (Exception ex) when (tries < 10 && (ex is UnauthorizedAccessException || ex is IOException))
                    {
                        Thread.Sleep(2000);
                    }
                }
            }
        }

        protected static int RunCommand(string binary, string args)
        {
            Process process = new Process
            {
                StartInfo = new ProcessStartInfo(binary, args)
                {
                    UseShellExecute = false
                }
            };

            Log.Debug("Running: >>{Binary}<< >>{Commandargs}<<", binary, args);

            process.Start();
            process.WaitForExit();

            Log.Debug("Ran: >>{Binary}<< >>{Commandargs}<< ExitCode: {ExitCode}", binary, args, process.ExitCode);

            return process.ExitCode;
        }

        protected void KillProcesses(string processName)
        {
            foreach (Process process in Process.GetProcessesByName(processName))
            {
                Log.Information("Killing: {ProcessName}", process.MainModule);
                process.Kill();
            }
        }
    }
}
