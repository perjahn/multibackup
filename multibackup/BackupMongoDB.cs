using Destructurama.Attributed;
using Serilog;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace multibackup
{
    public class BackupMongoDB : BackupJob
    {
        [NotLogged]
        public string ConnectionString { get; set; }

        public BackupMongoDB(string name, string zipPassword, Dictionary<string, object> tags, string targetServer, string targetAccount, string targetCertfile, string exportFolder, string date,
            string connectionString) : base(name, BackupType.MongoDB, zipPassword, tags, targetServer, targetAccount, targetCertfile, Path.Combine(exportFolder, $"mongodb_{name}_{date}"))
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
            var backupfolder = ExportPath;
            var mongodumpbinary = Tools.MongodumpBinary;

            Log.Information("Exporting: {Backupfolder}", backupfolder);

            Stopwatch watch = Stopwatch.StartNew();

            string args = $"\"/uri:{ConnectionString}\" /o \"{backupfolder}\"";

            int result = RunCommand(mongodumpbinary, args);
            watch.Stop();
            Statistics.ExportMongoDBTime += watch.Elapsed;
            long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

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
                .ForContext("Binary", mongodumpbinary)
                .ForContext("Commandargs", LogHelper.Mask(args, new[] { ConnectionString }))
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
