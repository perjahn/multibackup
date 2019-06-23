using Collector.Serilog.Enrichers.Assembly;
using Collector.Serilog.Enrichers.Author;
using Destructurama;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Enrichers.AzureWebApps;
using Serilog.Exceptions;
using Serilog.Formatting.Json;
using Serilog.Sinks.AzureEventHub;
using System;
using System.Collections.Generic;
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
            string[] parsedArgs = ParseArgs(args, out bool backupSqlServer, out bool backupCosmosDB, out bool backupAzureStorage);
            if (parsedArgs.Length > 0)
            {
                string version = GetAppVersion();
                Console.WriteLine($"multibackup {version}{Environment.NewLine}{Environment.NewLine}Usage: multibackup.exe [-OnlyBackupSqlServer] [-OnlyBackupCosmosDB] [-OnlyBackupAzureStorage]");
                return 1;
            }

            try
            {
                DoExceptionalStuff(backupSqlServer, backupCosmosDB, backupAzureStorage);
            }
            catch (Exception ex)
            {
                Log.Error("Exception: {Exception}", ex);
                return 1;
            }

            return 0;
        }

        static string[] ParseArgs(string[] args, out bool backupSqlServer, out bool backupCosmosDB, out bool backupAzureStorage)
        {
            backupSqlServer = true;
            backupCosmosDB = true;
            backupAzureStorage = true;
            if (args.Contains("-OnlyBackupSqlServer"))
            {
                backupCosmosDB = false;
                backupAzureStorage = false;
            }
            if (args.Contains("-OnlyBackupCosmosDB"))
            {
                backupSqlServer = false;
                backupAzureStorage = false;
            }
            if (args.Contains("-OnlyBackupAzureStorage"))
            {
                backupSqlServer = false;
                backupCosmosDB = false;
            }

            return args.Where(a => !a.StartsWith("-")).ToArray();
        }

        static void DoExceptionalStuff(bool backupSqlServer, bool backupCosmosDB, bool backupAzureStorage)
        {
            dynamic settings = LoadAppSettings();

            string eventHubConnectionString = settings.EventHubConnectionString;
            string serilogTeamName = settings?.SerilogTeamName ?? "Unknown team";
            string serilogDepartment = settings?.SerilogDepartment ?? "Unknown department";

            string defaultTargetServer = settings.TargetServer;
            string defaultTargetAccount = settings.TargetAccount;
            string defaultTargetCertfile = settings.TargetCertfile;

            string preBackupAction = settings.PreBackupAction;
            string preBackupActionArgs = settings.PreBackupActionArgs;
            string postBackupAction = settings.PostBackupAction;
            string postBackupActionArgs = settings.PostBackupActionArgs;

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

            string appfolder = Path.GetDirectoryName(Directory.GetCurrentDirectory());


            Stopwatch totalwatch = Stopwatch.StartNew();

            RunCommand(preBackupAction, preBackupActionArgs);

            string[] jsonfiles = Directory.GetFiles(appfolder, "backupjobs*.json");

            BackupJob[] backupjobs = BackupJob.LoadBackupJobs(jsonfiles, defaultTargetServer, defaultTargetAccount, defaultTargetCertfile);

            BackupJob.LogBackupJobs(backupjobs);

            Tools.Prepare(appfolder);

            string date = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");

            string exportfolder = Path.Combine(appfolder, "export");
            string sendfolder = Path.Combine(appfolder, "backups");
            BackupJob.ExportBackups(backupjobs, exportfolder, date, backupSqlServer, backupCosmosDB, backupAzureStorage);

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
                .ForContext("ExportAzureStorageTimeMS", (long)Statistics.ExportAzureStorageTime.TotalMilliseconds)
                .ForContext("ZipTimeMS", (long)Statistics.ZipTime.TotalMilliseconds)
                .ForContext("SyncTimeMS", (long)Statistics.SyncTime.TotalMilliseconds)
                .ForContext("TotalTimeMS", (long)Statistics.TotalTime.TotalMilliseconds)
                .ForContext("TotalBackupJobs", backupjobs.Length)
                .ForContext("BackupSuccessCount", Statistics.SuccessCount)
                .ForContext("BackupFailCount", backupjobs.Length - Statistics.SuccessCount)
                .Information("Backup finished");

            RunCommand(postBackupAction, postBackupActionArgs);
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

        static int RunCommand(string binary, string args)
        {
            if (binary == null)
            {
                return 0;
            }

            Process process = new Process
            {
                StartInfo = new ProcessStartInfo(binary, args)
                {
                    UseShellExecute = false
                }
            };

            Log.Debug("Running: >>{Binary}<< >>{Commandargs}<<", binary, args);

            process.Start();
            process.WaitForExit();

            Log.Debug("Ran: >>{Binary}<< >>{Commandargs}<< ExitCode: {ExitCode}", binary, args, process.ExitCode);

            return process.ExitCode;
        }
    }
}
