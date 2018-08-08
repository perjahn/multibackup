using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace multibackup
{
    class Backup
    {
        public static void ExportBackups(BackupJob[] backupjobs, string backupfolder, string date, bool backupSqlServer, bool backupCosmosDB, bool backupAzureStorage, string zipfolder)
        {
            Stopwatch watch = Stopwatch.StartNew();

            Log.Information("Creating folder: {Backupfolder}", backupfolder);
            Directory.CreateDirectory(backupfolder);

            Log.Information("Creating folder: {Zipfolder}", zipfolder);
            Directory.CreateDirectory(zipfolder);

            foreach (var backupjob in backupjobs)
            {
                string jobtype = backupjob.Type ?? "sqlserver";
                string jobname = backupjob.Name;
                string backuppath;

                using (new ContextLogger(backupjob.Tags))
                {
                    Log.Logger = Log.ForContext("Jobname", jobname).ForContext("Jobtype", jobtype);

                    if (jobtype == "sqlserver")
                    {
                        string connstr = backupjob.ConnectionString;
                        backuppath = Path.Combine(backupfolder, $"sqlserver_{backupjob.Name}_{date}.bacpac");
                        if (backupSqlServer)
                        {
                            ExportSqlServer(jobname, connstr, backuppath);
                        }
                    }
                    else if (jobtype == "cosmosdb")
                    {
                        string connstr = backupjob.ConnectionString;
                        backuppath = Path.Combine(backupfolder, $"cosmosdb_{backupjob.Name}_{date}.json");
                        string collection = backupjob.Collection;
                        if (backupCosmosDB)
                        {
                            ExportCosmosDB(jobname, connstr, collection, backuppath);
                        }
                    }
                    else if (jobtype == "azurestorage")
                    {
                        string url = backupjob.Url;
                        string key = backupjob.Key;
                        backuppath = Path.Combine(backupfolder, $"azurestorage_{backupjob.Name}_{date}");
                        if (backupAzureStorage)
                        {
                            ExportAzureStorage(jobname, url, key, backuppath);
                        }
                    }
                    else
                    {
                        Log.Warning("Unsupported database type");
                        continue;
                    }


                    string zipfile = Path.ChangeExtension(Path.GetFileName(backuppath), ".7z");
                    string zippassword = backupjob.ZipPassword;

                    string oldfolder = null;
                    try
                    {
                        oldfolder = Directory.GetCurrentDirectory();
                        Directory.SetCurrentDirectory(zipfolder);

                        EncryptBackup(jobname, jobtype, backuppath, zipfile, zippassword);

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

        static void ExportSqlServer(string jobname, string connstr, string backupfile)
        {
            string sqlpackagebinary = Tools.sqlpackagebinary;

            for (int tries = 1; tries <= 5; tries++)
            {
                using (new ContextLogger(new Dictionary<string, object>() { ["Tries"] = tries }))
                {
                    Log.Information("Exporting: {Backupfile}", backupfile);

                    Stopwatch watch = Stopwatch.StartNew();

                    string args = $"/a:Export /tf:{backupfile} /scs:\"{connstr}\"";

                    int result = RunCommand(sqlpackagebinary, args);
                    watch.Stop();
                    Statistics.ExportSqlServerTime += watch.Elapsed;
                    long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                    if (result == 0 && File.Exists(backupfile) && new FileInfo(backupfile).Length > 0)
                    {
                        long size = new FileInfo(backupfile).Length;
                        long sizemb = size / 1024 / 1024;
                        Statistics.UncompressedSize += size;
                        Log
                            .ForContext("ElapsedMS", elapsedms)
                            .ForContext("Backupfile", backupfile)
                            .ForContext("Size", size)
                            .ForContext("SizeMB", sizemb)
                            .Information("Export success");
                        return;
                    }
                    else
                    {
                        Log
                            .ForContext("Binary", sqlpackagebinary)
                            .ForContext("Commandargs", args)
                            .ForContext("Result", result)
                            .ForContext("ElapsedMS", elapsedms)
                            .ForContext("Backupfile", backupfile)
                            .Warning("Export fail", backupfile);
                    }
                }
            }

            if (File.Exists(backupfile) && new FileInfo(backupfile).Length == 0)
            {
                Log.Information("Deleting empty file: {Backupfile}", backupfile);
                File.Delete(backupfile);
            }

            Log.Warning("Couldn't export database to file: {Backupfile}", backupfile);
        }

        static void ExportCosmosDB(string jobname, string connstr, string collection, string backupfile)
        {
            string dtbinary = Tools.dtbinary;

            for (int tries = 1; tries <= 5; tries++)
            {
                using (new ContextLogger(new Dictionary<string, object>() { ["Tries"] = tries }))
                {
                    Log.Information("Exporting: {Backupfile}", backupfile);

                    Stopwatch watch = Stopwatch.StartNew();

                    string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(dtbinary));
                    string logfile = GetLogFileName(appfolder, jobname);

                    string args = $"/ErrorLog:{logfile} /ErrorDetails:All /s:DocumentDB /s.ConnectionString:{connstr} /s.Collection:{collection} /t:JsonFile /t.File:{backupfile} /t.Prettify";

                    int result = RunCommand(dtbinary, args);
                    watch.Stop();
                    Statistics.ExportCosmosDBTime += watch.Elapsed;
                    long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                    if (new FileInfo(logfile).Length > 0)
                    {
                        Log.Information("Reading logfile: {Logfile}", logfile);
                        string[] rows = File.ReadAllLines(logfile);
                        Log.ForContext("LogfileContent", TruncateLogFileContent(rows)).Information("dt results");
                    }

                    Log.Information("Deleting logfile: {Logfile}", logfile);
                    File.Delete(logfile);

                    if (result == 0 && File.Exists(backupfile) && new FileInfo(backupfile).Length > 0)
                    {
                        long size = new FileInfo(backupfile).Length;
                        long sizemb = size / 1024 / 1024;
                        Statistics.UncompressedSize += size;
                        Log
                            .ForContext("ElapsedMS", elapsedms)
                            .ForContext("Backupfile", backupfile)
                            .ForContext("Size", size)
                            .ForContext("SizeMB", sizemb)
                            .Information("Export success");
                        return;
                    }
                    else
                    {
                        Log
                            .ForContext("Binary", dtbinary)
                            .ForContext("Commandargs", args)
                            .ForContext("Result", result)
                            .ForContext("ElapsedMS", elapsedms)
                            .ForContext("Backupfile", backupfile)
                            .Warning("Export fail");
                    }
                }
            }

            if (File.Exists(backupfile) && new FileInfo(backupfile).Length == 0)
            {
                Log.Information("Deleting empty file: {Backupfile}", backupfile);
                File.Delete(backupfile);
            }

            Log.Warning("Couldn't export database to file: {Backupfile}", backupfile);
        }

        static void ExportAzureStorage(string jobname, string url, string key, string backupfolder)
        {
            string azcopybinary = Tools.azcopybinary;

            for (int tries = 1; tries <= 5; tries++)
            {
                using (new ContextLogger(new Dictionary<string, object>() { ["Tries"] = tries }))
                {
                    Log.Information("Exporting: {Backupfolder}", backupfolder);

                    Stopwatch watch = Stopwatch.StartNew();

                    string azcopyFolder = Path.Combine(Environment.GetEnvironmentVariable("LocalAppData"), "Microsoft", "Azure", "AzCopy");
                    KillProcesses("azcopy.exe");
                    if (Directory.Exists(azcopyFolder))
                    {
                        Log.Information("Deleting useless folder: {AzcopyFolder}", azcopyFolder);
                        Directory.Delete(azcopyFolder, true);
                    }

                    if (Directory.Exists(backupfolder))
                    {
                        Log.Information("Deleting folder: {Backupfolder}", backupfolder);
                        RobustDelete(backupfolder);
                    }
                    Log.Information("Creating folder: {Backupfolder}", backupfolder);
                    Directory.CreateDirectory(backupfolder);

                    string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(azcopybinary));
                    string logfile = GetLogFileName(appfolder, jobname);
                    string subdirs = Regex.IsMatch(url, "^https://[a-z]*\\.blob\\.core\\.windows\\.net") ? " /S" : string.Empty;

                    string args = $"/Source:{url} /Dest:{backupfolder} /SourceKey:{key} /V:{logfile}" + subdirs;
                    int result = RunCommand(azcopybinary, args);
                    watch.Stop();
                    Statistics.ExportAzureStorageTime += watch.Elapsed;
                    long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                    if (new FileInfo(logfile).Length > 0)
                    {
                        Log.Information("Reading logfile: {Logfile}", logfile);
                        string[] rows = File.ReadAllLines(logfile)
                            .Where(l => !l.Contains("][VERBOSE] Downloaded entities: ") && !l.Contains("][VERBOSE] Start transfer: ") && !l.Contains("][VERBOSE] Finished transfer: "))
                            .ToArray();
                        Log.ForContext("LogfileContent", TruncateLogFileContent(rows)).Information("azcopy results");
                    }

                    Log.Information("Deleting logfile: {Logfile}", logfile);
                    File.Delete(logfile);

                    if (result == 0 && Directory.Exists(backupfolder) && Directory.GetFiles(backupfolder, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length) > 0)
                    {
                        long size = Directory.GetFiles(backupfolder, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length);
                        long sizemb = size / 1024 / 1024;
                        Statistics.UncompressedSize += size;
                        Log
                            .ForContext("ElapsedMS", elapsedms)
                            .ForContext("Backupfolder", backupfolder)
                            .ForContext("Size", size)
                            .ForContext("SizeMB", sizemb)
                            .Information("Export success");
                        return;
                    }
                    else
                    {
                        Log
                            .ForContext("Binary", azcopybinary)
                            .ForContext("Commandargs", args)
                            .ForContext("Result", result)
                            .ForContext("ElapsedMS", elapsedms)
                            .ForContext("Backupfolder", backupfolder)
                            .Warning("Export fail");
                    }
                }
            }

            if (Directory.Exists(backupfolder) && Directory.GetFiles(backupfolder, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length) == 0)
            {
                Log.Information("Deleting empty folder: {Backupfolder}", backupfolder);
                RobustDelete(backupfolder);
            }

            Log.Warning("Couldn't export database to folder: {Backupfolder}", backupfolder);
            Log.Warning("Couldn't export database to folder: {Backupfolder}", backupfolder);
        }

        static void EncryptBackup(string jobname, string jobtype, string backuppath, string zipfile, string zippassword)
        {
            string sevenzipbinary = Tools.sevenzipbinary;

            if ((jobtype == "sqlserver" || jobtype == "cosmosdb") && !File.Exists(backuppath))
            {
                Log.Warning("Backup file not found, ignoring: {Backuppath}", backuppath);
                return;
            }
            if (jobtype == "azurestorage" && !Directory.Exists(backuppath))
            {
                Log.Warning("Backup folder not found, ignoring: {Backuppath}", backuppath);
                return;
            }

            Stopwatch watch = Stopwatch.StartNew();

            string compression = jobtype == "sqlserver" ? "-mx0" : "-mx9";

            string args = $"a {compression} {zipfile} {backuppath} -sdel -mhe -p{zippassword}";

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
                    .ForContext("Commandargs", args)
                    .ForContext("Result", result)
                    .ForContext("ElapsedMS", elapsedms)
                    .ForContext("Zipfile", zipfile)
                    .Warning("Zip fail");
            }
        }

        public static void SyncBackups(string zipfolder, string targetServer, string targetAccount)
        {
            string rsyncbinary = Tools.rsyncbinary;

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
                            Log.ForContext("LogfileContent", TruncateLogFileContent(rows)).Information("rsync results");
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
                                .ForContext("Commandargs", args)
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

        static string GetLogFileName(string appfolder, string jobname)
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

        static string TruncateLogFileContent(string[] rows)
        {
            string logfilecontent = string.Join(Environment.NewLine, rows);
            if (logfilecontent.Length > 10000)
            {
                logfilecontent = logfilecontent.Substring(0, 10000) + "...";
            }

            return logfilecontent;
        }

        static void RobustDelete(string folder)
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

        static int RunCommand(string binary, string args)
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

        static void KillProcesses(string processName)
        {
            foreach (Process process in Process.GetProcessesByName(processName))
            {
                Log.Information("Killing: {ProcessName}", process.MainModule);
                process.Kill();
            }
        }
    }
}
