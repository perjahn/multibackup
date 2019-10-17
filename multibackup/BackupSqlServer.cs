using Destructurama.Attributed;
using Serilog;
using System.Diagnostics;
using System.IO;

namespace multibackup
{
    public class BackupSqlServer : BackupJob
    {
        [NotLogged]
        public string ConnectionString { get; set; }

        protected override void LogBackupJob()
        {
            Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedConnectionString: {HashedConnectionString}, HashedZippassword: {HashedZippassword}",
                Name, Type,
                LogHelper.GetHashString(ConnectionString),
                LogHelper.GetHashString(ZipPassword));
        }

        protected override bool Export(string exportfolder, string date)
        {
            var backupfile = Path.Combine(exportfolder, $"sqlserver_{Name}_{date}.bacpac");
            var sqlpackagebinary = Tools.SqlpackageBinary;

            Log.Information("Exporting: {Backupfile}", backupfile);

            Stopwatch watch = Stopwatch.StartNew();

            string args = $"/a:Export /tf:{backupfile} /scs:\"{ConnectionString}\"";

            int result = RunCommand(sqlpackagebinary, args);
            watch.Stop();
            Statistics.ExportSqlServerTime += watch.Elapsed;
            long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

            if (result == 0 && File.Exists(backupfile) && new FileInfo(backupfile).Length > 0)
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
                .ForContext("Binary", sqlpackagebinary)
                .ForContext("Commandargs", LogHelper.Mask(args, ConnectionString))
                .ForContext("Result", result)
                .ForContext("ElapsedMS", elapsedms)
                .ForContext("Backupfile", backupfile)
                .Warning("Export fail", backupfile);

            if (File.Exists(backupfile) && new FileInfo(backupfile).Length == 0)
            {
                Log.Information("Deleting empty file: {Backupfile}", backupfile);
                File.Delete(backupfile);
            }

            return false;
        }
    }
}
