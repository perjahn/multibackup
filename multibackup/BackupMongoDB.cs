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

        protected override void Export(string backupfolder)
        {
            string mongodumpbinary = Tools.MongodumpBinary;

            for (int tries = 1; tries <= 5; tries++)
            {
                using (new ContextLogger(new Dictionary<string, object>() { ["Tries"] = tries }))
                {
                    Log.Information("Exporting: {Backupfolder}", backupfolder);

                    Stopwatch watch = Stopwatch.StartNew();

                    string args = $"\"/uri:{ConnectionString}\" /o \"{backupfolder}\"";

                    int result = RunCommand(mongodumpbinary, args);
                    watch.Stop();
                    Statistics.ExportMongoDBTime += watch.Elapsed;
                    long elapsedms = (long)watch.Elapsed.TotalMilliseconds;

                    if (result == 0 && Directory.Exists(backupfolder) && Directory.GetFiles(backupfolder, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length) == 0)
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
                            .ForContext("Binary", mongodumpbinary)
                            .ForContext("Commandargs", LogHelper.Mask(args, new[] { ConnectionString }))
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
