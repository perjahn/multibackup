using Destructurama.Attributed;
using Serilog;
using System.Diagnostics;
using System.IO;

namespace multibackup
{
    public class BackupCosmosDB : BackupJob
    {
        [NotLogged]
        public string ConnectionString { get; set; }
        [NotLogged]
        public string Collection { get; set; }

        protected override void LogBackupJob()
        {
            Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedConnectionString: {HashedConnectionString}, HashedCollection: {HashedCollection}, HashedZippassword: {HashedZippassword}",
                Name, Type,
                LogHelper.GetHashString(ConnectionString), LogHelper.GetHashString(Collection),
                LogHelper.GetHashString(ZipPassword));
        }

        protected override bool Export(string exportfolder, string date)
        {
            var backupfile = Path.Combine(exportfolder, $"cosmosdb_{Name}_{date}.json");
            var dtbinary = Tools.DtBinary;

            Log.Information("Exporting: {Backupfile}", backupfile);

            Stopwatch watch = Stopwatch.StartNew();

            string appfolder = Path.GetDirectoryName(Path.GetDirectoryName(dtbinary));
            string logfile = GetLogFileName(appfolder, Name);

            string args = $"/ErrorLog:{logfile} /ErrorDetails:All /s:DocumentDB /s.ConnectionString:{ConnectionString} /s.Collection:{Collection} /t:JsonFile /t.File:{backupfile} /t.Prettify";

            int result = RunCommand(dtbinary, args);
            watch.Stop();
            Statistics.ExportCosmosDBTime += watch.Elapsed;
            long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

            if (new FileInfo(logfile).Length > 0)
            {
                Log.Information("Reading logfile: {Logfile}", logfile);
                string[] rows = File.ReadAllLines(logfile);
                Log.ForContext("LogfileContent", LogHelper.TruncateLogFileContent(rows)).Information("dt results");
            }

            Log.Information("Deleting logfile: {Logfile}", logfile);
            File.Delete(logfile);

            if (result == 0 && ContainsFiles(backupfile))
            {
                BackupPath = backupfile;
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

            if (IsEmpty(backupfile))
            {
                Log.Information("Deleting empty file: {Backupfile}", backupfile);
                File.Delete(backupfile);
            }

            return false;
        }
    }
}
