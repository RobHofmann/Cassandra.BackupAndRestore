using Cassandra.BackupAndRestore.Base.Contracts.Helpers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Cassandra.BackupAndRestore.Utils.Helpers
{
    public class PatternMatching : IPatternMatching
    {
        private bool IsWildcardMatch(string input, string pattern)
        {
            string inputPattern = Regex.Escape(pattern).Replace("\\*", ".*?");
            return Regex.IsMatch(input, inputPattern);
        }

        /// <summary>
        /// You can match your input to a list of patterns. This works with wildcards.
        /// </summary>
        /// <param name="input">The string you want to match</param>
        /// <param name="patternList">A list of patterns which you want to match against</param>
        /// <returns></returns>
        public bool IsInList(string input, string[] patternList)
        {
            foreach (var pattern in patternList)
            {
                string inputPattern = Regex.Escape(pattern).Replace("\\*", ".*?");
                if (Regex.IsMatch(input, inputPattern))
                    return true;
            }
            return false;
        }
    }
}
