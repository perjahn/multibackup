using Destructurama.Attributed;
using Serilog;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace multibackup
{
    public class BackupAzureStorage : BackupJob
    {
        [NotLogged]
        public string Url { get; }
        [NotLogged]
        public string Key { get; }

        public BackupAzureStorage(string name, string zipPassword, Dictionary<string, object> tags, string targetServer, string targetAccount, string targetCertfile, string exportFolder, string date,
            string url, string key) : base(name, BackupType.AzureStorage, zipPassword, tags, targetServer, targetAccount, targetCertfile, Path.Combine(exportFolder, $"azurestorage_{name}_{date}"))
        {
            Url = url;
            Key = key;
        }

        protected override void LogBackupJob()
        {
            Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedUrl: {HashedUrl}, HashedKey: {HashedKey}, HashedZippassword: {HashedZippassword}",
                Name, Type,
                LogHelper.GetHashString(Url), LogHelper.GetHashString(Key),
                LogHelper.GetHashString(ZipPassword));
        }

        protected override bool Export()
        {
            var backupfolder = ExportPath;
            var azcopybinary = Tools.AzcopyBinary;

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

            string appfolder = PathHelper.GetParentFolder(PathHelper.GetParentFolder(azcopybinary));
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

            if (result == 0 && ContainsFiles(backupfolder))
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
                return true;
            }

            Log
                .ForContext("Binary", azcopybinary)
                .ForContext("Commandargs", LogHelper.Mask(args, new[] { Url, Key }))
                .ForContext("Result", result)
                .ForContext("ElapsedMS", elapsedms)
                .ForContext("Backupfolder", backupfolder)
                .Warning("Export fail");

            if (IsEmpty(backupfolder))
            {
                Log.Information("Deleting empty folder: {Backupfolder}", backupfolder);
                RobustDelete(backupfolder);
            }

            return false;
        }
    }
}
