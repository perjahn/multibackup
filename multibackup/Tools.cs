using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace multibackup
{
    class Tools
    {
        public static string SqlpackageBinary { get; set; }
        public static string DtBinary { get; set; }
        public static string AzcopyBinary { get; set; }
        public static string SevenzipBinary { get; set; }
        public static string RsyncBinary { get; set; }

        public static void Prepare(string appfolder)
        {
            SqlpackageBinary = Path.Combine(appfolder, "sqlpackage", "SqlPackage.exe");
            DtBinary = Path.Combine(appfolder, "dt", "dt.exe");
            AzcopyBinary = Path.Combine(appfolder, "azcopy", "AzCopy.exe");
            SevenzipBinary = Path.Combine(appfolder, "sevenzip", "7z.exe");
            RsyncBinary = Path.Combine(appfolder, "rsync", "bin", "rsync.exe");

            if (!File.Exists(SqlpackageBinary))
            {
                Log.Error("Couldn't find {Binary}", SqlpackageBinary);
                throw new Exception($"Couldn't find '{SqlpackageBinary}'");
            }
            if (!File.Exists(DtBinary))
            {
                Log.Error("Couldn't find {Binary}", DtBinary);
                throw new Exception($"Couldn't find '{DtBinary}'");
            }
            if (!File.Exists(AzcopyBinary))
            {
                Log.Error("Couldn't find {Binary}", AzcopyBinary);
                throw new Exception($"Couldn't find '{AzcopyBinary}'");
            }
            if (!File.Exists(SevenzipBinary))
            {
                Log.Error("Couldn't find {Binary}", SevenzipBinary);
                throw new Exception($"Couldn't find '{SevenzipBinary}'");
            }
            if (!File.Exists(RsyncBinary))
            {
                Log.Error("Couldn't find {Binary}", RsyncBinary);
                throw new Exception($"Couldn't find '{RsyncBinary}'");
            }


            Log.Information("Using {Toolname} tool: {Binary}", "sqlserver", SqlpackageBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "cosmosdb", DtBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "azurestorage", AzcopyBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "zip", SevenzipBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "rsync", RsyncBinary);
        }
    }
}
