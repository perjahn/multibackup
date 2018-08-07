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
        public static string sqlpackagebinary { get; set; }
        public static string dtbinary { get; set; }
        public static string azcopybinary { get; set; }
        public static string sevenzipbinary { get; set; }
        public static string rsyncbinary { get; set; }

        public static void Prepare(string appfolder)
        {
            sqlpackagebinary = Path.Combine(appfolder, "sqlpackage", "SqlPackage.exe");
            dtbinary = Path.Combine(appfolder, "dt", "dt.exe");
            azcopybinary = Path.Combine(appfolder, "azcopy", "AzCopy.exe");
            sevenzipbinary = Path.Combine(appfolder, "sevenzip", "7z.exe");
            rsyncbinary = Path.Combine(appfolder, "rsync", "bin", "rsync.exe");

            if (!File.Exists(sqlpackagebinary))
            {
                Log.Error("Couldn't find {sqlpackagebinary}", sqlpackagebinary);
                throw new Exception($"Couldn't find '{sqlpackagebinary}'");
            }
            if (!File.Exists(dtbinary))
            {
                Log.Error("Couldn't find {dtbinary}", dtbinary);
                throw new Exception($"Couldn't find '{dtbinary}'");
            }
            if (!File.Exists(azcopybinary))
            {
                Log.Error("Couldn't find {azcopybinary}", azcopybinary);
                throw new Exception($"Couldn't find '{azcopybinary}'");
            }
            if (!File.Exists(sevenzipbinary))
            {
                Log.Error("Couldn't find {sevenzipbinary}", sevenzipbinary);
                throw new Exception($"Couldn't find '{sevenzipbinary}'");
            }
            if (!File.Exists(rsyncbinary))
            {
                Log.Error("Couldn't find {rsyncbinary}", rsyncbinary);
                throw new Exception($"Couldn't find '{rsyncbinary}'");
            }


            Log.Information("Using export tool: {sqlpackagebinary}", sqlpackagebinary);
            Log.Information("Using export tool: {dtbinary}", dtbinary);
            Log.Information("Using export tool: {azcopybinary}", azcopybinary);
            Log.Information("Using zip tool: {sevenzipbinary}", sevenzipbinary);
            Log.Information("Using rsync tool: {rsyncbinary}", rsyncbinary);
        }
    }
}
