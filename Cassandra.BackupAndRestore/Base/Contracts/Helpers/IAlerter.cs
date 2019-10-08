using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.BackupAndRestore.Base.Contracts.Helpers
{
    public enum AlertPriority
    {
        High,
        Low
    }

    public interface IAlerter
    {
        void Alert(string bodyMessage, AlertPriority priority);
    }
}
