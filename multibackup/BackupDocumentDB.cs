using Destructurama.Attributed;
using Serilog;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace multibackup
{
    public class BackupDocumentDB : BackupJob
    {
        [NotLogged]
        public string ConnectionString { get; }
        [NotLogged]
        public string Collection { get; }

        public BackupDocumentDB(string name, string zipPassword, Dictionary<string, object> tags, string targetServer, string targetAccount, string targetCertfile, string exportFolder, string date,
            string connectionString, string collection) : base(name, BackupType.DocumentDB, zipPassword, tags, targetServer, targetAccount, targetCertfile, Path.Combine(exportFolder, $"cosmosdb_{name}_{date}.json"))
        {
            ConnectionString = connectionString;
            Collection = collection;
        }

        protected override void LogBackupJob()
        {
            Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedConnectionString: {HashedConnectionString}, HashedCollection: {HashedCollection}, HashedZippassword: {HashedZippassword}",
                Name, Type,
                LogHelper.GetHashString(ConnectionString), LogHelper.GetHashString(Collection),
                LogHelper.GetHashString(ZipPassword));
        }

        protected override bool Export()
        {
            var backupfile = ExportPath;
            var dtbinary = Tools.DtBinary;

            Log.Information("Exporting: {Backupfile}", backupfile);

            Stopwatch watch = Stopwatch.StartNew();

            string appfolder = PathHelper.GetParentFolder(PathHelper.GetParentFolder(dtbinary));
            string logfile = GetLogFileName(appfolder, Name);

            string args = $"/ErrorLog:{logfile} /ErrorDetails:All /s:DocumentDB /s.ConnectionString:{ConnectionString} /s.Collection:{Collection} /t:JsonFile /t.File:{backupfile} /t.Prettify";

            int result = RunCommand(dtbinary, args);
            watch.Stop();
            Statistics.ExportDocumentDBTime += watch.Elapsed;
            long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

            if (new FileInfo(logfile).Length > 0)
            {
                Log.Information("Reading logfile: {Logfile}", logfile);
                string[] rows = File.ReadAllLines(logfile);
                Log.ForContext("LogfileContent", LogHelper.TruncateLogFileContent(rows)).Information("dt results");
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
                return true;
            }

            Log
                .ForContext("Binary", dtbinary)
                .ForContext("Commandargs", LogHelper.Mask(args, new[] { ConnectionString, Collection }))
                .ForContext("Result", result)
                .ForContext("ElapsedMS", elapsedms)
                .ForContext("Backupfile", backupfile)
                .Warning("Export fail");

            if (File.Exists(backupfile) && new FileInfo(backupfile).Length == 0)
            {
                Log.Information("Deleting empty file: {Backupfile}", backupfile);
                File.Delete(backupfile);
            }

            return false;
        }
    }
}
