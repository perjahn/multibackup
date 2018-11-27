using Destructurama.Attributed;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace multibackup
{
    class BackupAzureStorage : BackupJob
    {
        [NotLogged]
        public string Url { get; set; }
        [NotLogged]
        public string Key { get; set; }

        protected override void Export(string backupfolder)
        {
            string azcopybinary = Tools.AzcopyBinary;

            for (int tries = 1; tries <= 5; tries++)
            {
                using (new ContextLogger(new Dictionary<string, object>() { ["Tries"] = tries }))
                {
                    Log.Information("Exporting: {Backupfolder}", backupfolder);

                    Stopwatch watch = Stopwatch.StartNew();

                    string azcopyFolder = backupfolder.Replace("azurestorage_", "junkfolder_");
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
                    string logfile = GetLogFileName(appfolder, Name);
                    string subdirs = Regex.IsMatch(Url, "^https://[a-z0-9]+\\.blob\\.core\\.windows\\.net") ? " /S" : string.Empty;

                    string args = $"/Source:{Url} /Dest:{backupfolder} /SourceKey:{Key} /V:{logfile} /Z:{azcopyFolder}{subdirs}";
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
                        Log.ForContext("LogfileContent", LogHelper.TruncateLogFileContent(rows)).Information("azcopy results");
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
                            .ForContext("Commandargs", LogHelper.Mask(args, new[] { Url, Key }))
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
        }
    }
}
