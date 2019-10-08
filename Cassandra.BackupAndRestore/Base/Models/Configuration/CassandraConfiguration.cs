using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.BackupAndRestore.Base.Models.Configuration
{
    public class CassandraConfiguration
    {
        public string HostName { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
    }
}
