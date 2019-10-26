using System;

namespace multibackup
{
    class Statistics
    {
        public static int SuccessCount { get; set; }
        public static long UncompressedSize { get; set; }
        public static long CompressedSize { get; set; }
        public static TimeSpan ExportSqlServerTime { get; set; }
        public static TimeSpan ExportDocumentDBTime { get; set; }
        public static TimeSpan ExportMongoDBTime { get; set; }
        public static TimeSpan ExportAzureStorageTime { get; set; }
        public static TimeSpan ZipTime { get; set; }
        public static TimeSpan SyncTime { get; set; }
        public static TimeSpan TotalTime { get; set; }
    }
}
