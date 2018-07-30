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
        public static string sqlpackageexe { get; set; }
        public static string dtexe { get; set; }
        public static string azcopyexe { get; set; }
        public static string sevenzipexe { get; set; }
        public static string rsyncexe { get; set; }

        public static void Prepare(string appfolder)
        {
            sqlpackageexe = Path.Combine(appfolder, "sqlpackage", "SqlPackage.exe");
            dtexe = Path.Combine(appfolder, "dt", "dt.exe");
            azcopyexe = Path.Combine(appfolder, "azcopy", "AzCopy.exe");
            sevenzipexe = Path.Combine(appfolder, "sevenzip", "7z.exe");
            rsyncexe = Path.Combine(appfolder, "rsync", "bin", "rsync.exe");

            if (!File.Exists(sqlpackageexe))
            {
                Log.Error("Couldn't find {sqlpackageexe}", sqlpackageexe);
                throw new Exception($"Couldn't find '{sqlpackageexe}'");
            }
            if (!File.Exists(dtexe))
            {
                Log.Error("Couldn't find {dtexe}", dtexe);
                throw new Exception($"Couldn't find '{dtexe}'");
            }
            if (!File.Exists(azcopyexe))
            {
                Log.Error("Couldn't find {azcopyexe}", azcopyexe);
                throw new Exception($"Couldn't find '{azcopyexe}'");
            }
            if (!File.Exists(sevenzipexe))
            {
                Log.Error("Couldn't find {sevenzipexe}", sevenzipexe);
                throw new Exception($"Couldn't find '{sevenzipexe}'");
            }
            if (!File.Exists(rsyncexe))
            {
                Log.Error("Couldn't find {rsyncexe}", rsyncexe);
                throw new Exception($"Couldn't find '{rsyncexe}'");
            }


            Log.Information("Using export tool: {sqlpackageexe}", sqlpackageexe);
            Log.Information("Using export tool: {dtexe}", dtexe);
            Log.Information("Using export tool: {azcopyexe}", azcopyexe);
            Log.Information("Using zip tool: {sevenzipexe}", sevenzipexe);
            Log.Information("Using rsync tool: {rsyncexe}", rsyncexe);
        }
    }
}
