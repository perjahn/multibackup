using Serilog;
using System;
using System.IO;

namespace multibackup
{
    class PathHelper
    {
        public static string GetParentFolder(string path)
        {
            if (path == null)
            {
                Log.Logger.Error("Invalid path.");
                throw new ArgumentNullException("Invalid path.");
            }

            var parentFolder = Path.GetDirectoryName(path);
            if (parentFolder == null)
            {
                Log.Logger.Error("Couldn't get parent folder of {binfolder}", path);
                throw new Exception($"Couldn't get parent folder of '{path}'");
            }

            return parentFolder;
        }
    }
}
