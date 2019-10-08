using Cassandra.BackupAndRestore.Base.Contracts.Helpers;
using Cassandra.BackupAndRestore.Base.Models.Configuration;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Cassandra.BackupAndRestore.Utils.Helpers.Alerters
{
    public class OpsGenieAlerter : IAlerter
    {
        private readonly AlertingConfiguration _alertingConfiguration;

        public OpsGenieAlerter(AlertingConfiguration alertingConfiguration)
        {
            _alertingConfiguration = alertingConfiguration;
        }

        public void Alert(string bodyMessage, AlertPriority priority)
        {
            if (!_alertingConfiguration.Enable)
                return;

            var client = new RestClient("https://api.opsgenie.com/v2/alerts");

            if (_alertingConfiguration.EnableProxy)
            {
                var proxy = new WebProxy(_alertingConfiguration.ProxyUrl, false);
                if (_alertingConfiguration.EnableProxyAuthentication)
                    proxy.Credentials = new NetworkCredential(_alertingConfiguration.ProxyUsername, _alertingConfiguration.ProxyPassword);
                client.Proxy = proxy;
            }

            var request = new RestRequest(Method.POST);
            request.AddHeader("Authorization", $"GenieKey {_alertingConfiguration.ApiKey}");
            request.AddHeader("Content-Type", "application/json");
            request.AddJsonBody(new { message = bodyMessage, priority = GetOpsGeniePriority(priority) });

            //enable this instead of the above "undefined" if you need it to be P1
            //request.AddParameter("undefined", "{\r\n\t\"message\":\"This is to ignore\",\r\n\t\"priority\":\"P1\"\r\n}", ParameterType.RequestBody);

            IRestResponse response = client.Execute(request);
        }

        private string GetOpsGeniePriority(AlertPriority priority)
        {
            switch (priority)
            {
                default:
                case AlertPriority.High:
                    return "P1";
                case AlertPriority.Low:
                    return "P3";
            }
        }
    }
}
