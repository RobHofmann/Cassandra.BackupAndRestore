using Cassandra.BackupAndRestore.Base.Contracts.Helpers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cassandra.BackupAndRestore.Utils.Helpers
{
    public class Counter : ICounter
    {
        private long _sharedInteger;

        public void IncrementCounter()
        {
            Interlocked.Increment(ref _sharedInteger);
        }

        public long ReadCounter()
        {
            return Interlocked.Read(ref _sharedInteger);
        }

        public void Reset()
        {
            Interlocked.Exchange(ref _sharedInteger, 0);
        }
    }
}
