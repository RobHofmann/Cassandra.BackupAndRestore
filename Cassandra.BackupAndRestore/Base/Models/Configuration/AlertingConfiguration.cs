using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.BackupAndRestore.Base.Models.Configuration
{
    public class AlertingConfiguration
    {
        public bool Enable { get; set; }
        public string Type { get; set; }
        public bool EnableProxy { get; set; }
        public string ProxyUrl { get; set; }
        public string ApiKey { get; set; }
        public bool EnableProxyAuthentication { get; set; }
        public string ProxyUsername { get; set; }
        public string ProxyPassword { get; set; }
    }
}
