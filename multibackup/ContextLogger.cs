using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace multibackup
{
    class ContextLogger : IDisposable
    {
        readonly ILogger oldLogger;

        public ContextLogger(Dictionary<string, object> properties) : this(Log.Logger, properties) { }

        public ContextLogger(ILogger logger, Dictionary<string, object> properties)
        {
            oldLogger = logger;
            Log.Logger = logger;
            if (properties != null)
            {
                foreach (var property in properties)
                {
                    Log.Logger = Log.Logger.ForContext(property.Key, properties[property.Key]);
                }
            }
        }

        public void Dispose()
        {
            Log.Logger = oldLogger;
        }
    }
}
