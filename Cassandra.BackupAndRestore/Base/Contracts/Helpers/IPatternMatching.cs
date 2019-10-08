using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.BackupAndRestore.Base.Contracts.Helpers
{
    interface IPatternMatching
    {
        bool IsInList(string input, string[] patternList);
    }
}
