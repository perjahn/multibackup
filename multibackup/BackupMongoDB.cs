using Destructurama.Attributed;
using Serilog;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace multibackup
{
    public class BackupMongoDB : BackupJob
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
            var backupfolder = Path.Combine(exportfolder, $"mongodb_{Name}_{date}");
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
                BackupPath = backupfolder;
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
