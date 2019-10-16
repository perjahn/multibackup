using Destructurama.Attributed;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace multibackup
{
    public class BackupJob
    {
        public string Name { get; set; }
        [NotLogged]
        public string ZipPassword { get; set; }
        public Dictionary<string, object> Tags { get; set; }
        [NotLogged]
        public string TargetServer { get; set; }
        [NotLogged]
        public string TargetAccount { get; set; }
        [NotLogged]
        public string TargetCertfile { get; set; }
        [NotLogged]
        public string Zipfile { get; set; }


        protected virtual void Export(string backuppath) => throw new NotImplementedException();

        public static BackupJob[] LoadBackupJobs(string[] jsonfiles, string defaultTargetServer, string defaultTargetAccount, string defaultTargetCertfile)
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
                    else if (string.Compare(type, "mongodb", false) == 0)
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
                    else if (string.Compare(type, "mongodb", true) == 0)
                    {
                        job = new BackupMongoDB { ConnectionString = connectionstring };
                    }
                    else
                    {
                        job = new BackupAzureStorage { Url = url, Key = key };
                    }

                    job.Name = name;
                    job.ZipPassword = zippassword;
                    job.Tags = tags;

                    job.TargetServer = backupjob.targetserver ?? defaultTargetServer;
                    job.TargetAccount = backupjob.targetaccount ?? defaultTargetAccount;
                    job.TargetCertfile = backupjob.targetcertfile ?? defaultTargetCertfile;

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
                ["MongoDB"] = 0,
                ["AzureStorage"] = 0
            };
            for (int i = 0; i < backupjobs.Length; i++)
            {
                BackupJob backupjob = backupjobs[i];

                string type = backupjob is BackupSqlServer ? "SqlServer" : backupjob is BackupCosmosDB ? "CosmosDB" : backupjob is BackupMongoDB ? "MongoDB" : "AzureStorage";
                typecounts[type]++;

                using (new ContextLogger(backupjob.Tags))
                {
                    if (backupjob is BackupSqlServer)
                    {
                        Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedConnectionString: {HashedConnectionString}, HashedZippassword: {HashedZippassword}",
                            backupjob.Name, type,
                            LogHelper.GetHashString((backupjob as BackupSqlServer).ConnectionString),
                            LogHelper.GetHashString(backupjob.ZipPassword));
                    }
                    else if (backupjob is BackupCosmosDB)
                    {
                        Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedConnectionString: {HashedConnectionString}, HashedCollection: {HashedCollection}, HashedZippassword: {HashedZippassword}",
                            backupjob.Name, type,
                            LogHelper.GetHashString((backupjob as BackupCosmosDB).ConnectionString), LogHelper.GetHashString((backupjob as BackupCosmosDB).Collection),
                            LogHelper.GetHashString(backupjob.ZipPassword));
                    }
                    else if (backupjob is BackupMongoDB)
                    {
                        Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedConnectionString: {HashedConnectionString}, HashedZippassword: {HashedZippassword}",
                            backupjob.Name, type,
                            LogHelper.GetHashString((backupjob as BackupMongoDB).ConnectionString),
                            LogHelper.GetHashString(backupjob.ZipPassword));
                    }
                    else if (backupjob is BackupAzureStorage)
                    {
                        Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedUrl: {HashedUrl}, HashedKey: {HashedKey}, HashedZippassword: {HashedZippassword}",
                            backupjob.Name, type,
                            LogHelper.GetHashString((backupjob as BackupAzureStorage).Url), LogHelper.GetHashString((backupjob as BackupAzureStorage).Key),
                            LogHelper.GetHashString(backupjob.ZipPassword));
                    }
                }
            }

            Log
                .ForContext("SqlServerCount", typecounts["SqlServer"])
                .ForContext("CosmosDBCount", typecounts["CosmosDB"])
                .ForContext("MongoDBCount", typecounts["MongoDB"])
                .ForContext("AzureStorageCount", typecounts["AzureStorage"])
                .ForContext("TotalCount", backupjobs.Length)
                .Information("Backup counts");
        }

        public static void ExportBackups(BackupJob[] backupjobs, string exportfolder, string date, bool backupSqlServer, bool backupCosmosDB, bool backupMongoDB, bool backupAzureStorage)
        {
            Stopwatch watch = Stopwatch.StartNew();

            Log.Information("Creating folder: {Exportfolder}", exportfolder);
            Directory.CreateDirectory(exportfolder);

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
                        backuppath = Path.Combine(exportfolder, $"sqlserver_{backupjob.Name}_{date}.bacpac");
                        if (backupSqlServer)
                        {
                            backupjob.Export(backuppath);
                        }
                    }
                    else if (backupjob is BackupCosmosDB)
                    {
                        backuppath = Path.Combine(exportfolder, $"cosmosdb_{backupjob.Name}_{date}.json");
                        if (backupCosmosDB)
                        {
                            backupjob.Export(backuppath);
                        }
                    }
                    else if (backupjob is BackupMongoDB)
                    {
                        backuppath = Path.Combine(exportfolder, $"mongodb_{backupjob.Name}_{date}");
                        if (backupMongoDB)
                        {
                            backupjob.Export(backuppath);
                        }
                    }
                    else
                    {
                        backuppath = Path.Combine(exportfolder, $"azurestorage_{backupjob.Name}_{date}");
                        if (backupAzureStorage)
                        {
                            backupjob.Export(backuppath);
                        }
                    }


                    backupjob.Zipfile = Path.ChangeExtension(backuppath, ".7z");

                    string oldfolder = null;
                    try
                    {
                        oldfolder = Directory.GetCurrentDirectory();
                        Directory.SetCurrentDirectory(exportfolder);

                        backupjob.EncryptBackup(backuppath);

                        if (File.Exists(backupjob.Zipfile))
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

        void EncryptBackup(string backuppath)
        {
            string sevenzipbinary = Tools.SevenzipBinary;

            if ((this is BackupSqlServer || this is BackupCosmosDB) && !File.Exists(backuppath))
            {
                Log.Warning("Backup file not found, ignoring: {Backuppath}", backuppath);
                return;
            }
            if ((this is BackupMongoDB || this is BackupAzureStorage) && !Directory.Exists(backuppath))
            {
                Log.Warning("Backup folder not found, ignoring: {Backuppath}", backuppath);
                return;
            }

            Stopwatch watch = Stopwatch.StartNew();

            string compression = this is BackupSqlServer ? "-mx0" : "-mx9";

            string args = $"a {compression} {Zipfile} {backuppath} -sdel -mhe -p{ZipPassword}";

            int result = RunCommand(sevenzipbinary, args);
            watch.Stop();
            Statistics.ZipTime += watch.Elapsed;
            long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

            if (result == 0 && File.Exists(Zipfile) && new FileInfo(Zipfile).Length > 0)
            {
                long size = new FileInfo(Zipfile).Length;
                long sizemb = size / 1024 / 1024;
                Statistics.CompressedSize += size;
                Log
                    .ForContext("ElapsedMS", elapsedms)
                    .ForContext("Zipfile", Zipfile)
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
                    .ForContext("Zipfile", Zipfile)
                    .Warning("Zip fail");
            }
        }

        public static void SendBackups(BackupJob[] backupjobs, string sendfolder)
        {
            Log.Information("Creating folder: {Sendfolder}", sendfolder);
            Directory.CreateDirectory(sendfolder);

            var targets = backupjobs
                .GroupBy(b => new { b.TargetServer, b.TargetAccount, b.TargetCertfile })
                .ToArray();

            Log.Information("Sending backups to {targets} target groups.", targets.Length);

            foreach (var target in targets)
            {
                int files = 0;

                foreach (var backupjob in backupjobs)
                {
                    if (backupjob.TargetServer == target.Key.TargetServer && backupjob.TargetAccount == target.Key.TargetAccount && backupjob.TargetCertfile == target.Key.TargetCertfile)
                    {
                        string sourceFile = backupjob.Zipfile;
                        string targetFile = Path.Combine(sendfolder, Path.GetFileName(backupjob.Zipfile));
                        if (File.Exists(sourceFile))
                        {
                            Log.Information("Moving: {Source} -> {Target}", sourceFile, targetFile);
                            File.Move(sourceFile, targetFile);
                            files++;
                        }
                        else
                        {
                            Log.Warning("File missing: {Source}", sourceFile);
                        }
                    }
                }

                if (files == 0)
                {
                    Log.Warning("No files to sync to server: {TargetServer}", target.Key.TargetServer);
                }
                else
                {
                    SyncBackups(sendfolder, target.Key.TargetServer, target.Key.TargetAccount, target.Key.TargetCertfile);
                }
            }
        }

        static void SyncBackups(string zipfolder, string targetServer, string targetAccount, string targetCertfile)
        {
            string rsyncbinary = Tools.RsyncBinary;

            string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(rsyncbinary)));
            string logfile = GetLogFileName(appfolder, "SyncBackups");

            string source = "/cygdrive/" + char.ToLower(zipfolder[0]) + zipfolder.Substring(2).Replace("\\", "/");
            string target = $"{targetAccount}@{targetServer}:.";

            string binfolder = Path.GetDirectoryName(rsyncbinary);
            string synccert = Path.Combine(Path.GetDirectoryName(binfolder), "synccert", $"{targetCertfile}");

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
                            .Information("Syncing backup files: {Source} -> {Target}", source, LogHelper.Mask(target, new[] { targetServer, targetAccount }));

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

        protected static void RobustDelete(string folder)
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

        public static int RunCommand(string binary, string args)
        {
            if (binary == null)
            {
                Log.Warning("Binary null.");
                return 0;
            }

            Process process = new Process
            {
                StartInfo = new ProcessStartInfo(binary, args)
                {
                    UseShellExecute = false
                }
            };

            if (Log.IsEnabled(LogEventLevel.Debug))
            {
                Log.Debug("Running: {Binary} {Commandargs}", binary, args);
            }
            else if (Log.IsEnabled(LogEventLevel.Information))
            {
                Log.Information("Running: {Binary}", binary);
            }

            process.Start();
            process.WaitForExit();

            if (Log.IsEnabled(LogEventLevel.Debug))
            {
                Log.Debug("Ran: {Binary} {Commandargs} ExitCode: {ExitCode}", binary, args, process.ExitCode);
            }
            else if (Log.IsEnabled(LogEventLevel.Information))
            {
                Log.Information("Ran: {Binary} ExitCode: {ExitCode}", binary, process.ExitCode);
            }

            return process.ExitCode;
        }

        protected static void KillProcesses(string processName)
        {
            foreach (Process process in Process.GetProcessesByName(processName))
            {
                Log.Information("Killing: {ProcessName}", process.MainModule);
                process.Kill();
            }
        }
    }
}
