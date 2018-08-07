﻿using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace multibackup
{
    class Backup
    {
        public static void ExportBackups(BackupJob[] backupjobs, string backupfolder, string date, bool backupSqlDB, bool backupCosmosDB, bool backupAzureStorage, string zipfolder)
        {
            Stopwatch watch = Stopwatch.StartNew();

            Log.Information("Creating folder: {backupfolder}", backupfolder);
            Directory.CreateDirectory(backupfolder);

            Log.Information("Creating folder: {zipfolder}", zipfolder);
            Directory.CreateDirectory(zipfolder);

            foreach (var backupjob in backupjobs)
            {
                string type = backupjob.Type ?? "sql";

                string jobname = backupjob.Name;
                string backuppath;

                if (type == "sql")
                {
                    string connstr = backupjob.ConnectionString;
                    backuppath = Path.Combine(backupfolder, $"sql_{backupjob.Name}_{date}.bacpac");
                    if (backupSqlDB)
                    {
                        ExportSqlDB(jobname, connstr, backuppath);
                    }
                }
                else if (type == "cosmosdb")
                {
                    string connstr = backupjob.ConnectionString;
                    backuppath = Path.Combine(backupfolder, $"cosmosdb_{backupjob.Name}_{date}.json");
                    string collection = backupjob.Collection;
                    if (backupCosmosDB)
                    {
                        ExportCosmosDB(jobname, connstr, collection, backuppath);
                    }
                }
                else if (type == "azurestorage")
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
                    Log.Warning("Unsupported database type: {type}", type);
                    continue;
                }


                string zipfile = Path.ChangeExtension(Path.GetFileName(backuppath), ".7z");
                string zippassword = backupjob.ZipPassword;

                string oldfolder = Directory.GetCurrentDirectory();
                Directory.SetCurrentDirectory(zipfolder);

                EncryptBackup(jobname, type, backuppath, zipfile, zippassword);

                if (File.Exists(zipfile))
                {
                    Statistics.SuccessCount++;
                }
                else
                {
                    Log.Warning("Backupjob failed: Jobname: {jobname}", backupjob.Name);
                }

                Directory.SetCurrentDirectory(oldfolder);
            }

            Log.Information("Done Exporting: Time: {elapsedms}", (long)watch.Elapsed.TotalMilliseconds);
        }

        static void ExportSqlDB(string jobname, string connstr, string backupfile)
        {
            string sqlpackagebinary = Tools.sqlpackagebinary;

            for (int tries = 0; tries < 5; tries++)
            {
                Log.Information("Exporting {jobtype} {jobname} (try {tries}) -> {backupfile}",
                    "sql", jobname, tries + 1, backupfile);

                Stopwatch watch = Stopwatch.StartNew();

                string args = $"/a:Export /tf:{backupfile} /scs:\"{connstr}\"";

                int result = RunCommand(sqlpackagebinary, args);
                watch.Stop();
                Statistics.ExportSqlTime += watch.Elapsed;
                long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                if (result == 0 && File.Exists(backupfile) && new FileInfo(backupfile).Length > 0)
                {
                    long size = new FileInfo(backupfile).Length;
                    long sizemb = size / 1024 / 1024;
                    Statistics.UncompressedSize += size;
                    Log.Information("Export Success! Jobtype: {jobtype}, Jobname: {jobname}, Time: {elapsedms}, Backupfile: {backupfile}, Size: {size} ({sizemb} mb)",
                        "sql", jobname, elapsedms, backupfile, size, sizemb);
                    return;
                }
                else
                {
                    Log.Warning("Export Fail! Binary: {binary}, Args: {commandargs}, Result: {result}, Jobtype: {jobtype}, Jobname: {jobname}, Time: {elapsedms}, Backupfile: {backupfile}",
                        sqlpackagebinary, args, result, "sql", jobname, elapsedms, backupfile);
                }
            }

            if (File.Exists(backupfile) && new FileInfo(backupfile).Length == 0)
            {
                Log.Information("Deleting empty file: {backupfile}", backupfile);
                File.Delete(backupfile);
            }

            Log.Warning("Couldn't export database to file: {backupfile}", backupfile);
        }

        static void ExportCosmosDB(string jobname, string connstr, string collection, string backupfile)
        {
            string dtbinary = Tools.dtbinary;

            for (int tries = 0; tries < 5; tries++)
            {
                Log.Information("Exporting {jobtype} {jobname} (try {tries}) -> {backupfile}",
                    "cosmosdb", jobname, tries + 1, backupfile);

                Stopwatch watch = Stopwatch.StartNew();

                string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(dtbinary));
                string logfile = GetLogFile(appfolder, jobname);

                string args = $"/ErrorLog:{logfile} /ErrorDetails:All /s:DocumentDB /s.ConnectionString:{connstr} /s.Collection:{collection} /t:JsonFile /t.File:{backupfile} /t.Prettify";

                int result = RunCommand(dtbinary, args);
                watch.Stop();
                Statistics.ExportCosmosTime += watch.Elapsed;
                long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                if (new FileInfo(logfile).Length > 0)
                {
                    Log.Information("Reading logfile: {logfile}", logfile);
                    string[] rows = File.ReadAllLines(logfile);
                    Log.Information("dt results" + Environment.NewLine + "{log}", string.Join(Environment.NewLine, rows));
                }

                Log.Information("Deleting logfile: {logfile}", logfile);
                File.Delete(logfile);

                if (result == 0 && File.Exists(backupfile) && new FileInfo(backupfile).Length > 0)
                {
                    long size = new FileInfo(backupfile).Length;
                    long sizemb = size / 1024 / 1024;
                    Statistics.UncompressedSize += size;
                    Log.Information("Export Success! Jobtype: {jobtype}, Jobname: {jobname}, Time: {elapsedms}, Backupfile: {backupfile}, Size: {size} ({sizemb} mb)",
                        "cosmosb", jobname, elapsedms, backupfile, size, sizemb);
                    return;
                }
                else
                {
                    Log.Warning("Export Fail! Binary: {binary}, Args: {commandargs}, Result: {result}, Jobtype: {jobtype}, Jobname: {jobname}, Time: {elapsedms}, Backupfile: {backupfile}",
                        dtbinary, args, result, "cosmosb", jobname, elapsedms, backupfile);
                }
            }

            if (File.Exists(backupfile) && new FileInfo(backupfile).Length == 0)
            {
                Log.Information("Deleting empty file: {backupfile}", backupfile);
                File.Delete(backupfile);
            }

            Log.Warning("Couldn't export database to file: {backupfile}", backupfile);
        }

        static void ExportAzureStorage(string jobname, string url, string key, string backupfolder)
        {
            string azcopybinary = Tools.azcopybinary;

            for (int tries = 0; tries < 5; tries++)
            {
                Log.Information("Exporting {jobtype} {jobname} (try {tries}) -> {backupfolder}",
                    "azurestorage", jobname, tries + 1, backupfolder);

                Stopwatch watch = Stopwatch.StartNew();

                string azcopyFolder = Path.Combine(Environment.GetEnvironmentVariable("LocalAppData"), "Microsoft", "Azure", "AzCopy");
                KillProcesses("azcopy.exe");
                if (Directory.Exists(azcopyFolder))
                {
                    Log.Information("Deleting useless folder: {azcopyFolder}", azcopyFolder);
                    Directory.Delete(azcopyFolder, true);
                }

                if (Directory.Exists(backupfolder))
                {
                    Log.Information("Deleting folder: {backupfolder}", backupfolder);
                    RobustDelete(backupfolder);
                }
                Log.Information("Creating folder: {backupfolder}", backupfolder);
                Directory.CreateDirectory(backupfolder);

                string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(azcopybinary));
                string logfile = GetLogFile(appfolder, jobname);
                string subdirs = Regex.IsMatch(url, "^https://[a-z]*\\.blob\\.core\\.windows\\.net") ? " /S" : string.Empty;

                string args = $"/Source:{url} /Dest:{backupfolder} /SourceKey:{key} /V:{logfile}" + subdirs;
                int result = RunCommand(azcopybinary, args);
                watch.Stop();
                Statistics.ExportAzureStorageTime += watch.Elapsed;
                long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                if (new FileInfo(logfile).Length > 0)
                {
                    Log.Information("Reading logfile: {logfile}", logfile);
                    string[] rows = File.ReadAllLines(logfile);
                    Log.Information("azcopy results" + Environment.NewLine + "{log}", string.Join(Environment.NewLine, rows));
                }

                Log.Information("Deleting logfile: {logfile}", logfile);
                File.Delete(logfile);

                if (result == 0 && Directory.Exists(backupfolder) && Directory.GetFiles(backupfolder, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length) > 0)
                {
                    long size = Directory.GetFiles(backupfolder, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length);
                    long sizemb = size / 1024 / 1024;
                    Statistics.UncompressedSize += size;
                    Log.Information("Export Success! Jobtype: {jobtype}, Jobname: {jobname}, Time: {elapsedms}, Backupfolder: {backupfolder}, Size: {size} ({sizemb} mb)",
                        "azurestorage", jobname, elapsedms, backupfolder, size, sizemb);
                    return;
                }
                else
                {
                    Log.Warning("Export Fail! Binary: {binary}, Args: {commandargs}, Result: {result}, Jobtype: {jobtype}, Jobname: {jobname}, Time: {elapsedms}, Backupfolder: {backupfolder}",
                        azcopybinary, args, result, "azurestorage", jobname, elapsedms, backupfolder);
                }
            }

            if (Directory.Exists(backupfolder) && Directory.GetFiles(backupfolder, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length) == 0)
            {
                Log.Information("Deleting empty folder: {backupfolder}", backupfolder);
                RobustDelete(backupfolder);
            }

            Log.Warning("Couldn't export database to folder: {backupfolder}", backupfolder);
        }

        static void EncryptBackup(string jobname, string type, string backuppath, string zipfile, string zippassword)
        {
            string sevenzipbinary = Tools.sevenzipbinary;

            if ((type == "sql" || type == "cosmosdb") && !File.Exists(backuppath))
            {
                Log.Warning("Backup file not found, ignoring: {backuppath}", backuppath);
                return;
            }
            if (type == "azurestorage" && !Directory.Exists(backuppath))
            {
                Log.Warning("Backup folder not found, ignoring: {backuppath}", backuppath);
                return;
            }

            Stopwatch watch = Stopwatch.StartNew();

            string compression = type == "sql" ? "-mx0" : "-mx9";

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
                Log.Information("Zip Success! Jobname: {jobname}, Time: {elapsedms}, Zipfile: {zipfile}, Size: {size} ({sizemb} mb)",
                    jobname, elapsedms, zipfile, size, sizemb);
            }
            else
            {
                Log.Warning("Zip Fail! Binary: {binary}, Args: {commandargs}, Result: {result}, Jobname: {jobname}, Time: {elapsedms}, Zipfile: {zipfile}",
                    sevenzipbinary, args, result, jobname, elapsedms, zipfile);
            }
        }

        public static void SendBackups(string zipfolder, string targetServer, string targetAccount)
        {
            string rsyncbinary = Tools.rsyncbinary;

            string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(rsyncbinary)));
            string logfile = GetLogFile(appfolder, "SyncBackups");

            string source = "/cygdrive/" + char.ToLower(zipfolder[0]) + zipfolder.Substring(2).Replace("\\", "/");
            string target = $"{targetAccount}@{targetServer}:.";

            string binfolder = Path.GetDirectoryName(rsyncbinary);
            string synccert = Path.Combine(Path.GetDirectoryName(binfolder), "synccert", "rsync_id_rsa.txt");

            string oldfolder = Directory.GetCurrentDirectory();
            Directory.SetCurrentDirectory(binfolder);

            for (int tries = 0; tries < 5; tries++)
            {
                string[] files = Directory.GetFiles(zipfolder);

                Log.Information("Syncing (try {tries}) {files} backup files: {source} -> {target}",
                    tries + 1, files.Length, source, target);

                Stopwatch watch = Stopwatch.StartNew();

                string args = $"--checksum --remove-source-files -a -l -e './ssh -o StrictHostKeyChecking=no -i {synccert}' {source} {target} --log-file {logfile}";

                int result = RunCommand(Path.GetFileName(rsyncbinary), args);
                watch.Stop();
                Statistics.SendTime += watch.Elapsed;
                long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                if (new FileInfo(logfile).Length > 0)
                {
                    Log.Information("Reading logfile: {logfile}", logfile);
                    string[] rows = File.ReadAllLines(logfile).Where(l => !l.Contains(".d..t...... ") && !l.Contains("<f..t...... ")).ToArray();
                    Log.Information("Sync results" + Environment.NewLine + "{log}", string.Join(Environment.NewLine, rows));
                }

                Log.Information("Deleting logfile: {logfile}", logfile);
                File.Delete(logfile);

                if (result == 0)
                {
                    Log.Information("Sync Success! Time: {elapsedms}, Zipfolder: {zipfolder}",
                        elapsedms, zipfolder);
                    Directory.SetCurrentDirectory(oldfolder);
                    return;
                }
                else
                {
                    Log.Warning("Sync Fail! Binary: {binary}, Args: {commandargs}, Result: {result}, Time: {elapsedms}, Zipfolder: {zipfolder}",
                        rsyncbinary, args, result, elapsedms, zipfolder);
                }
            }

            Statistics.SuccessCount = 0;
            Directory.SetCurrentDirectory(oldfolder);
        }

        static string GetLogFile(string appfolder, string jobname)
        {
            string logfolder = Path.Combine(appfolder, "logs");
            if (!Directory.Exists(logfolder))
            {
                Directory.CreateDirectory(logfolder);
            }

            string logfile = Path.Combine(logfolder, $"{jobname}_{DateTime.UtcNow:yyyyMMdd}.log");
            if (File.Exists(logfile))
            {
                Log.Information("Deleting old logfile: {logfile}", logfile);
                File.Delete(logfile);
            }
            return logfile;
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
                    Log.Information("Try {tries} to delete folder: {folder}", tries, folder);
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

        static int RunCommand(string exefile, string args)
        {
            Process process = new Process
            {
                StartInfo = new ProcessStartInfo(exefile, args)
                {
                    UseShellExecute = false
                }
            };

            Log.Debug("Running: >>{exefile}<< >>{args}<<", exefile, args);

            process.Start();
            process.WaitForExit();

            Log.Debug("Ran: >>{exefile}<< >>{args}<< ExitCode: {ExitCode}", exefile, args, process.ExitCode);

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
