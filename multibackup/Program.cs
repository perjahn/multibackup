using Collector.Serilog.Enrichers.Assembly;
using Collector.Serilog.Enrichers.Author;
using Destructurama;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Enrichers.AzureWebApps;
using Serilog.Exceptions;
using Serilog.Sinks.AzureEventHub;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;

namespace multibackup
{
    class Program
    {
        static int Main(string[] args)
        {
            string[] parsedArgs = ParseArgs(args, out bool backupSqlServer, out bool backupCosmosDB, out bool backupMongoDB, out bool backupAzureStorage);
            if (parsedArgs.Length > 0)
            {
                string version = GetAppVersion();
                Console.WriteLine($"multibackup {version}{Environment.NewLine}{Environment.NewLine}Usage: multibackup.exe [-OnlyBackupSqlServer] [-OnlyBackupCosmosDB] [-OnlyBackupMongoDB] [-OnlyBackupAzureStorage]");
                return 1;
            }

            try
            {
                DoExceptionalStuff(backupSqlServer, backupCosmosDB, backupMongoDB, backupAzureStorage);
            }
            catch (Exception ex)
            {
                Log.Error("Exception: {Exception}", ex);
                return 1;
            }

            return 0;
        }

        static string[] ParseArgs(string[] args, out bool backupSqlServer, out bool backupCosmosDB, out bool backupMongoDB, out bool backupAzureStorage)
        {
            backupSqlServer = true;
            backupCosmosDB = true;
            backupMongoDB = true;
            backupAzureStorage = true;
            if (args.Contains("-OnlyBackupSqlServer"))
            {
                backupCosmosDB = false;
                backupMongoDB = false;
                backupAzureStorage = false;
            }
            if (args.Contains("-OnlyBackupCosmosDB"))
            {
                backupSqlServer = false;
                backupMongoDB = false;
                backupAzureStorage = false;
            }
            if (args.Contains("-OnlyBackupMongoDB"))
            {
                backupSqlServer = false;
                backupCosmosDB = false;
                backupAzureStorage = false;
            }
            if (args.Contains("-OnlyBackupAzureStorage"))
            {
                backupSqlServer = false;
                backupCosmosDB = false;
                backupMongoDB = false;
            }

            return args.Where(a => !a.StartsWith("-")).ToArray();
        }

        static void DoExceptionalStuff(bool backupSqlServer, bool backupCosmosDB, bool backupMongoDB, bool backupAzureStorage)
        {
            dynamic settings = LoadAppSettings();

            string eventHubConnectionString = settings.EventHubConnectionString;

            string serilogTeamName = settings.SerilogTeamName ?? "Unknown team";
            string serilogDepartment = settings.SerilogDepartment ?? "Unknown department";

            string defaultTargetServer = settings.TargetServer;
            string defaultTargetAccount = settings.TargetAccount;
            string defaultTargetCertfile = settings.TargetCertfile;

            string preBackupAction = settings.PreBackupAction;
            string preBackupActionArgs = settings.PreBackupActionArgs;
            string postBackupAction = settings.PostBackupAction;
            string postBackupActionArgs = settings.PostBackupActionArgs;
            string preSyncAction = settings.PreSyncAction;
            string preSyncActionArgs = settings.PreSyncActionArgs;
            string postSyncAction = settings.PostSyncAction;
            string postSyncActionArgs = settings.PostSyncActionArgs;

            if (defaultTargetServer == null)
            {
                string errorMessage = "Missing TargetServer configuration in appsettings.json";
                Log.Error(errorMessage);
                throw new Exception(errorMessage);
            }
            if (defaultTargetAccount == null)
            {
                string errorMessage = "Missing TargetAccount configuration in appsettings.json";
                Log.Error(errorMessage);
                throw new Exception(errorMessage);
            }
            if (defaultTargetCertfile == null)
            {
                string errorMessage = "Missing TargetCertfile configuration in appsettings.json";
                Log.Error(errorMessage);
                throw new Exception(errorMessage);
            }

            ConfigureLogging(eventHubConnectionString, serilogTeamName, serilogDepartment);

            string appfolder = PathHelper.GetParentFolder(Directory.GetCurrentDirectory());

            Stopwatch totalwatch = Stopwatch.StartNew();

            string[] jsonfiles = Directory.GetFiles(appfolder, "backupjobs*.json");

            string exportFolder = Path.Combine(appfolder, "export");
            Log.Information("Creating folder: {ExportFolder}", exportFolder);
            Directory.CreateDirectory(exportFolder);

            string date = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");

            var backupjobs = BackupJob.LoadBackupJobs(jsonfiles, defaultTargetServer, defaultTargetAccount, defaultTargetCertfile, exportFolder, date);

            BackupJob.ExcludeBackupJobs(backupjobs, backupSqlServer, backupCosmosDB, backupMongoDB, backupAzureStorage);

            BackupJob.LogBackupJobs(backupjobs);

            Tools.Prepare(appfolder);

            string sendfolder = Path.Combine(appfolder, "backups");

            if (preBackupAction != null)
            {
                BackupJob.RunCommand(preBackupAction, preBackupActionArgs);
            }
            BackupJob.ExportBackups(backupjobs, exportFolder);
            if (postBackupAction != null)
            {
                BackupJob.RunCommand(postBackupAction, postBackupActionArgs);
            }

            if (preSyncAction != null)
            {
                BackupJob.RunCommand(preSyncAction, preSyncActionArgs);
            }
            BackupJob.SendBackups(backupjobs, sendfolder);

            totalwatch.Stop();
            Statistics.TotalTime = totalwatch.Elapsed;

            Log
                .ForContext("UncompressedSize", Statistics.UncompressedSize)
                .ForContext("UncompressedSizeMB", Statistics.UncompressedSize / 1024 / 1024)
                .ForContext("CompressedSize", Statistics.CompressedSize)
                .ForContext("CompressedSizeMB", Statistics.CompressedSize / 1024 / 1024)
                .ForContext("ExportSqlServerTimeMS", (long)Statistics.ExportSqlServerTime.TotalMilliseconds)
                .ForContext("ExportCosmosDBTimeMS", (long)Statistics.ExportCosmosDBTime.TotalMilliseconds)
                .ForContext("ExportMongoDBTimeMS", (long)Statistics.ExportCosmosDBTime.TotalMilliseconds)
                .ForContext("ExportAzureStorageTimeMS", (long)Statistics.ExportAzureStorageTime.TotalMilliseconds)
                .ForContext("ZipTimeMS", (long)Statistics.ZipTime.TotalMilliseconds)
                .ForContext("SyncTimeMS", (long)Statistics.SyncTime.TotalMilliseconds)
                .ForContext("TotalTimeMS", (long)Statistics.TotalTime.TotalMilliseconds)
                .ForContext("TotalBackupJobs", backupjobs.Count)
                .ForContext("BackupSuccessCount", Statistics.SuccessCount)
                .ForContext("BackupFailCount", backupjobs.Count - Statistics.SuccessCount)
                .Information("Backup finished");
            if (postSyncAction != null)
            {
                BackupJob.RunCommand(postSyncAction, postSyncActionArgs);
            }
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
                Console.WriteLine("IsDevelopment set: Logging to console");
                config = config
                    .MinimumLevel.Debug()
                    .WriteTo.Console();
            }
            else if (eventHubConnectionString == null)
            {
                Console.WriteLine("EventHubConnectionString missing in appsettings.json: Logging to console");
                config = config
                    .MinimumLevel.Debug()
                    .WriteTo.Console();
            }
            else
            {
                Console.WriteLine($"Using event hub for logging: >>>{eventHubConnectionString}<<<");
                var eventHub = EventHubClient.CreateFromConnectionString(eventHubConnectionString);
                config = config.WriteTo.Sink(new AzureEventHubSink(eventHub, new Collector.Serilog.Sinks.AzureEventHub.ScalarValueTypeSuffixJsonFormatter()));
            }

            Log.Logger = config.CreateLogger();


            string version = GetAppVersion();

            Log.Logger.Information("Logger initiliazed: {Version}", version);
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
