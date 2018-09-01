using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace multibackup
{
    class LogHelper
    {
        public static string TruncateLogFileContent(string[] rows)
        {
            string logfilecontent = string.Join(Environment.NewLine, rows);
            if (logfilecontent.Length > 10000)
            {
                logfilecontent = logfilecontent.Substring(0, 10000) + "...";
            }

            return logfilecontent;
        }

        public static string Mask(string text, string secret)
        {
            return text.Replace(secret, new string('*', secret.Length));
        }

        public static string Mask(string text, string[] secrets)
        {
            string result = text;

            foreach (var secret in secrets)
            {
                result = result.Replace(secret, new string('*', secret.Length));
            }

            return result;
        }

        public static string GetHashString(string value)
        {
            using (var crypto = new SHA256Managed())
            {
                return string.Concat(crypto.ComputeHash(Encoding.UTF8.GetBytes(value)).Select(b => b.ToString("x2")));
            }
        }

        public static string MaskWithHash(string text, string[] secrets)
        {
            string result = text;

            foreach (var secret in secrets)
            {
                result = result.Replace(secret, new string('*', secret.Length));
            }

            return result;
        }
    }
}
