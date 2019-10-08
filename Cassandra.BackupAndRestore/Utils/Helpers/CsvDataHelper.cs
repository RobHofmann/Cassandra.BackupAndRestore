using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.BackupAndRestore.Utils.Helpers
{
    public static class CsvDataHelper
    {
        /// <summary>
        /// Turn a string into a CSV cell output
        /// </summary>
        /// <param name="str">String to output</param>
        /// <returns>The CSV cell formatted string</returns>
        public static string ToCsvString(this string str)
        {
            bool mustQuote = false;
            var stringBuilder = new StringBuilder();
            foreach(var brokenChar in str)
            {
                byte brokenCharByte = (byte)brokenChar;
                if (brokenCharByte == 0x0a) // \n
                {
                    stringBuilder.Append("\\n");
                    mustQuote = true;
                }
                else if (brokenCharByte == 0x0d) // \r
                {
                    stringBuilder.Append("\\r");
                    mustQuote = true;
                }
                else if (brokenCharByte == 0x09) // \t
                {
                    stringBuilder.Append("\\t");
                    mustQuote = true;
                }
                else if (brokenCharByte == 0x22) // "
                {
                    stringBuilder.Append("\\\"");
                    mustQuote = true;
                }
                else if (brokenCharByte == 0x5c) // \
                {
                    stringBuilder.Append("\\\\");
                    mustQuote = true;
                }
                else if (brokenCharByte < 0x20 || brokenCharByte == 0x7f)
                {
                    stringBuilder.AppendFormat("\\x{0:x2}", brokenCharByte);
                    mustQuote = true;
                }
                else
                    stringBuilder.Append(brokenChar);
            }

            if (str.Contains(",") || str.Contains("\"") || str.Contains("\r") || str.Contains("\n") || str.Contains("\\"))
                mustQuote = true;

            if (mustQuote)
                return $"\"{stringBuilder.ToString()}\"";
            else
                return stringBuilder.ToString();
        }
    }
}
