using Serilog;
using System;
using System.IO;
using System.Text;

namespace multibackup
{
    class Tools
    {
        public static string SqlpackageBinary { get; private set; } = string.Empty;
        public static string DtBinary { get; private set; } = string.Empty;
        public static string MongodumpBinary { get; private set; } = string.Empty;
        public static string AzcopyBinary { get; private set; } = string.Empty;
        public static string SevenzipBinary { get; private set; } = string.Empty;
        public static string RsyncBinary { get; private set; } = string.Empty;

        public static void Prepare(string appfolder)
        {
            var errors = new StringBuilder();

            SqlpackageBinary = GetToolPath("sqlpackage", Path.Combine(appfolder, "sqlpackage", "SqlPackage.exe"), errors);
            DtBinary = GetToolPath("dt", Path.Combine(appfolder, "dt", "dt.exe"), errors);
            MongodumpBinary = GetToolPath("mongodump", Path.Combine(appfolder, "mongodump", "mongodump.exe"), errors);
            AzcopyBinary = GetToolPath("azcopy", Path.Combine(appfolder, "azcopy", "AzCopy.exe"), errors);
            SevenzipBinary = GetToolPath("7z", Path.Combine(appfolder, "sevenzip", "7z.exe"), errors);
            RsyncBinary = GetToolPath("rsync", Path.Combine(appfolder, "rsync", "bin", "rsync.exe"), errors);

            if (errors.ToString().Length != 0)
            {
                Log.Logger.Error(errors.ToString());
                throw new Exception(errors.ToString());
            }


            Log.Information("Using {Toolname} tool: {Binary}", "sqlpackage", SqlpackageBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "dt", DtBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "mongodump", MongodumpBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "azcopy", AzcopyBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "7z", SevenzipBinary);
            Log.Information("Using {Toolname} tool: {Binary}", "rsync", RsyncBinary);
        }

        private static string GetToolPath(string searchBinary, string explicitBinary, StringBuilder errors)
        {
            string? path = Environment.GetEnvironmentVariable("path");
            if (path == null)
            {
                string error = "Environment variable 'path' not set.";
                Log.Logger.Error(error);
                throw new Exception(error);
            }

            foreach (var folder in path.Split(Path.PathSeparator))
            {
                if (!Directory.Exists(folder))
                {
                    continue;
                }
                string[] files = Directory.GetFiles(folder, searchBinary);
                if (files.Length > 0)
                {
                    return files[0];
                }
            }

            if (File.Exists(explicitBinary))
            {
                return explicitBinary;
            }

            Log.Error("Couldn't find {Binary}", explicitBinary);
            errors.AppendLine($"Couldn't find '{explicitBinary}'");
            return string.Empty;
        }
    }
}
