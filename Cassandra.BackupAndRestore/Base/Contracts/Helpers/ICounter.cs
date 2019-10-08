using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.BackupAndRestore.Base.Contracts.Helpers
{
    interface ICounter
    {
        void IncrementCounter();
        long ReadCounter();
        void Reset();
    }
}
