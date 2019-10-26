using Destructurama.Attributed;
using Serilog;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace multibackup
{
    public class BackupSqlServer : BackupJob
    {
        [NotLogged]
        public string ConnectionString { get; set; }

        public BackupSqlServer(string name, string zipPassword, Dictionary<string, object> tags, string targetServer, string targetAccount, string targetCertfile, string exportFolder, string date,
            string connectionString) : base(name, BackupType.SqlServer, zipPassword, tags, targetServer, targetAccount, targetCertfile, Path.Combine(exportFolder, $"sqlserver_{name}_{date}.bacpac"))
        {
            ConnectionString = connectionString;
        }

        protected override void LogBackupJob()
        {
            Log.Information("Jobname: {Jobname}, Jobtype: {Jobtype}, HashedConnectionString: {HashedConnectionString}, HashedZippassword: {HashedZippassword}",
                Name, Type,
                LogHelper.GetHashString(ConnectionString),
                LogHelper.GetHashString(ZipPassword));
        }

        protected override bool Export()
        {
            var backupfile = ExportPath;
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
