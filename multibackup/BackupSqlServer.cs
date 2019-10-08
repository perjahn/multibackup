using Destructurama.Attributed;
using Serilog;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace multibackup
{
    class BackupSqlServer : BackupJob
    {
        [NotLogged]
        public string ConnectionString { get; set; }

        protected override void Export(string backupfile)
        {
            string sqlpackagebinary = Tools.SqlpackageBinary;

            for (int tries = 1; tries <= 5; tries++)
            {
                using (new ContextLogger(new Dictionary<string, object>() { ["Tries"] = tries }))
                {
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
                        return;
                    }
                    else
                    {
                        Log
                            .ForContext("Binary", sqlpackagebinary)
                            .ForContext("Commandargs", LogHelper.Mask(args, ConnectionString))
                            .ForContext("Result", result)
                            .ForContext("ElapsedMS", elapsedms)
                            .ForContext("Backupfile", backupfile)
                            .Warning("Export fail", backupfile);
                    }
                }
            }

            if (File.Exists(backupfile) && new FileInfo(backupfile).Length == 0)
            {
                Log.Information("Deleting empty file: {Backupfile}", backupfile);
                File.Delete(backupfile);
            }

            Log.Warning("Couldn't export database to file: {Backupfile}", backupfile);
        }
    }
}
