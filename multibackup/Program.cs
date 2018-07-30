using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Collector.Serilog.Enrichers.Assembly;
using Collector.Serilog.Enrichers.Author;
using Collector.Serilog.Sinks.AzureEventHub;
using Destructurama;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Enrichers.AzureWebApps;
using Serilog.Exceptions;

namespace multibackup
{
    class Program
    {
        static int Main(string[] args)
        {
            string[] parsedArgs = ParseArgs(args, out bool backupSqlDB, out bool backupCosmosDB, out bool backupAzureStorage);
            if (parsedArgs.Length > 0)
            {
                string version = GetAppVersion();
                Console.WriteLine($"multibackup {version}{Environment.NewLine}{Environment.NewLine}Usage: multibackup.exe [-OnlyBackupSqlDB] [-OnlyBackupCosmosDB] [-OnlyBackupAzureStorage]");
                return 1;
            }

            try
            {
                DoExceptionalStuff(backupSqlDB, backupCosmosDB, backupAzureStorage);
            }
            catch (Exception ex)
            {
                Log.Error("Exception: {exception}", ex);
                return 1;
            }

            return 0;
        }

        static string[] ParseArgs(string[] args, out bool backupSqlDB, out bool backupCosmosDB, out bool backupAzureStorage)
        {
            backupSqlDB = true;
            backupCosmosDB = true;
            backupAzureStorage = true;
            if (args.Contains("-OnlyBackupSqlDB"))
            {
                backupCosmosDB = false;
                backupAzureStorage = false;
            }
            if (args.Contains("-OnlyBackupCosmosDB"))
            {
                backupSqlDB = false;
                backupAzureStorage = false;
            }
            if (args.Contains("-OnlyBackupAzureStorage"))
            {
                backupSqlDB = false;
                backupCosmosDB = false;
            }

            return args.Where(a => !a.StartsWith("-")).ToArray();
        }

        static void DoExceptionalStuff(bool backupSqlDB, bool backupCosmosDB, bool backupAzureStorage)
        {
            dynamic settings = LoadAppSettings();

            string eventHubConnectionString = settings.EventHubConnectionString;
            string serilogTeamName = settings?.SerilogTeamName ?? "Unknown team";
            string serilogDepartment = settings?.SerilogDepartment ?? "Unknown department";

            string targetServer = settings.TargetServer;
            string targetAccount = settings.TargetAccount;

            if (targetServer == null)
            {
                string errorMessage = "Missing TargetServer configuration in appsettings.json";
                Log.Error(errorMessage);
                throw new Exception(errorMessage);
            }
            if (targetAccount == null)
            {
                string errorMessage = "Missing TargetAccount configuration in appsettings.json";
                Log.Error(errorMessage);
                throw new Exception(errorMessage);
            }

            ConfigureLogging(eventHubConnectionString, serilogTeamName, serilogDepartment);

            string appfolder = Path.GetDirectoryName(Directory.GetCurrentDirectory());


            Stopwatch totalwatch = Stopwatch.StartNew();

            string jsonfile = Path.Combine(appfolder, "backupjobs.json");

            BackupJob[] backupjobs = BackupJob.LoadBackupJobs(jsonfile);

            BackupJob.LogBackupJobs(backupjobs);

            Tools.Prepare(appfolder);

            string date = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");

            string exportfolder = Path.Combine(appfolder, "export");
            string zipfolder = Path.Combine(appfolder, "backups");
            Backup.ExportBackups(backupjobs, exportfolder, date, backupSqlDB, backupCosmosDB, backupAzureStorage, zipfolder);

            Backup.SendBackups(zipfolder, targetServer, targetAccount);

            totalwatch.Stop();
            Statistics.TotalTime = totalwatch.Elapsed;


            Log.Information("Uncompressed size: {UncompressedSize} ({UncompressedSizeMB} mb), Compressed size: {CompressedSize} ({CompressedSizeMB} mb)",
                Statistics.UncompressedSize, Statistics.UncompressedSize / 1024 / 1024, Statistics.CompressedSize, Statistics.CompressedSize / 1024 / 1024);

            Log.Information("Export sql time: {ExportSqlTimeMS}, Export cosmos time: {ExportCosmosTimeMS}, Export storage time: {ExportAzureStorageTimeMS}, Zip time: {ZipTimeMS}, Send time: {SendTimeMS}, Total time: {TotalTimeMS}",
                (long)Statistics.ExportSqlTime.TotalMilliseconds, (long)Statistics.ExportCosmosTime.TotalMilliseconds, (long)Statistics.ExportAzureStorageTime.TotalMilliseconds,
                (long)Statistics.ZipTime.TotalMilliseconds, (long)Statistics.SendTime.TotalMilliseconds, (long)Statistics.TotalTime.TotalMilliseconds);

            Log.Information("Backup Finished! Total jobs: {TotalBackupJobs}, Success: {BackupSuccess}, Failed: {BackupFail}",
                backupjobs.Length, Statistics.BackupSuccess, backupjobs.Length - Statistics.BackupSuccess);
        }

        static JObject LoadAppSettings()
        {
            string configfile = "appsettings.development.json";
            if (File.Exists(configfile))
            {
                return JObject.Parse(File.ReadAllText(configfile));
            }

            configfile = "appsettings.json";
            if (File.Exists(configfile))
            {
                return JObject.Parse(File.ReadAllText(configfile));
            }

            throw new FileNotFoundException(configfile);
        }

        static void ConfigureLogging(string eventHubConnectionString, string teamName, string department)
        {
            if (!Log.Logger.GetType().Name.Equals("SilentLogger", StringComparison.CurrentCultureIgnoreCase))
            {
                return;
            }

            var config = new LoggerConfiguration()
                .Enrich.With(new AuthorEnricher(
                    teamName: teamName,
                    department: department,
                    repositoryUrl: new Uri("https://github.com/collector-bank/multibackup"),
                    serviceGroup: "Backup"))
                .Enrich.With<AzureWebAppsNameEnricher>()
                .Enrich.With<AzureWebJobsNameEnricher>()
                .Enrich.With<SourceSystemEnricher<Program>>()
                .Enrich.WithProperty("BackupSession", Guid.NewGuid())
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails()
                .Destructure.UsingAttributes();

            if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("IsDevelopment")))
            {
                Console.WriteLine("IsDevelopment set: Logging to console!");
                config = config
                    .MinimumLevel.Debug()
                    .WriteTo.Console();
            }
            else if (eventHubConnectionString == null)
            {
                Console.WriteLine("EventHubConnectionString missing in appsettings.json: Logging to console!");
                config = config
                    .MinimumLevel.Debug()
                    .WriteTo.Console();
            }
            else
            {
                Console.WriteLine($"Using event hub for logging: >>>{eventHubConnectionString}<<<");
                var eventHub = EventHubClient.CreateFromConnectionString(eventHubConnectionString);
                config = config.WriteTo.Sink(new AzureEventHubSink(eventHub));
            }

            Log.Logger = config.CreateLogger();


            string version = GetAppVersion();

            Log.Logger.Information("Logger initiliazed: {version}", version);
        }

        static string GetAppVersion()
        {
            if (typeof(Program).Assembly.GetCustomAttributes(false).SingleOrDefault(o => o.GetType() == typeof(AssemblyFileVersionAttribute)) is AssemblyFileVersionAttribute versionAttribute)
            {
                return versionAttribute.Version;
            }
            else
            {
                return string.Empty;
            }
        }
    }
}
