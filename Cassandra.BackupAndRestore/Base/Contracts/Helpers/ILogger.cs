using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.BackupAndRestore.Base.Contracts.Helpers
{
    public interface ILogger
    {
        void Log(string logEntry, params string[] prefixes);

        void Error(string logEntry, params string[] prefixes);
    }
}
