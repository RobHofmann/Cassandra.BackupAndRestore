using Cassandra.BackupAndRestore.Base.Contracts.Services;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.BackupAndRestore.Services.Cassandra
{
    internal class CassandraService : ICassandraService
    {
        public ICluster GetCluster(string cassandraHostName, string cassandraUsername, string cassandraPassword)
        {
            var clusterBuilder = Cluster.Builder()
                .AddContactPoints(cassandraHostName)
                .WithQueryTimeout(10800000)
                .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()));

            if (cassandraUsername?.Length > 0 || cassandraPassword?.Length > 0)
                clusterBuilder.WithCredentials(cassandraUsername, cassandraPassword);

            var cluster = clusterBuilder.Build();
            return cluster;
        }

        public Task<ISession> GetConnectionAsync(ICluster cluster)
        {
            return cluster.ConnectAsync();
        }

        public Task<RowSet> GetRowsAsync(ISession session, string table)
        {
            return session.ExecuteAsync(new SimpleStatement($"SELECT * FROM {table}"));
        }

        public Task<RowSet> GetAllTablesAsync(ISession session)
        {
            return session.ExecuteAsync(new SimpleStatement("SELECT keyspace_name,table_name FROM system_schema.tables"));
        }

        public string GetKeyspaceDefinition(ICluster cluster, string keyspaceName)
        {
            return cluster.Metadata.GetKeyspace(keyspaceName).AsCqlQuery();
        }

        public string GetTableDefinition(string hostName, string userName, string password, string keyspace, string tableName)
        {
            ProcessStartInfo startinfo = new ProcessStartInfo() { RedirectStandardOutput = true, RedirectStandardError = true };
            startinfo.FileName = @"cqlsh";
            startinfo.Arguments = $"{hostName} -u {userName} -p \"{password}\" -e \"DESCRIBE TABLE {keyspace}.{tableName}\"";
            Process process = new Process();
            process.StartInfo = startinfo;
            process.StartInfo.UseShellExecute = false;
            process.Start();
            string output = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            if (process.ExitCode != 0)
                throw new Exception("CQLSH could not export any key definition", new Exception(process.StandardError.ReadToEnd()));
            return output;
        }

        private string GetClusteringColumns(List<Row> columns)
        {
            var sb = new StringBuilder();
            bool firstClusteringColumn = true;
            foreach (var column in columns)
            {
                if (column.GetValue<string>("kind") == "clustering")
                {
                    string columnName = column.GetValue<string>("column_name");
                    string clusteringOrder = column.GetValue<string>("clustering_order");

                    if (!firstClusteringColumn)
                        sb.Append(", ");

                    sb.Append($"{columnName} {clusteringOrder}");
                    firstClusteringColumn = false;
                }
            }
            return sb.ToString();
        }

        private string GetPartitionKeyColumns(List<Row> columns)
        {
            var sb = new StringBuilder();
            bool firstKeyColumn = true;
            foreach (var column in columns)
            {
                string kind = column.GetValue<string>("kind");
                if (kind == "partition_key" || kind == "clustering")
                {
                    string columnName = column.GetValue<string>("column_name");

                    if (!firstKeyColumn)
                        sb.Append(", ");

                    sb.Append(columnName);
                    firstKeyColumn = false;
                }
            }
            return sb.ToString();
        }

        private List<Row> SortColumnSchema(RowSet rows)
        {
            // Split into different key types
            List<Row> partitionKeyRows = new List<Row>();
            List<Row> clusteringRows = new List<Row>();
            List<Row> regularRows = new List<Row>();
            foreach (var row in rows)
            {
                switch(row.GetValue<string>("kind"))
                {
                    case "partition_key":
                        partitionKeyRows.Add(row);
                        break;
                    case "clustering":
                        clusteringRows.Add(row);
                        break;
                    default:
                        regularRows.Add(row);
                        break;

                }
            }

            // Order the lists by position
            partitionKeyRows = partitionKeyRows.OrderBy(x => x.GetValue<int>("position")).ToList();
            clusteringRows = clusteringRows.OrderBy(x => x.GetValue<int>("position")).ToList();
            regularRows = regularRows.OrderBy(x => x.GetValue<int>("position")).ToList();

            // Combine the lists
            partitionKeyRows.AddRange(clusteringRows);
            partitionKeyRows.AddRange(regularRows);

            return partitionKeyRows;
        }

        private string GetCassandraCompatibleJsonFromObject(object input)
        {
            StringBuilder sb = new StringBuilder();
            using (StringWriter sw = new StringWriter(sb))
            using (JsonTextWriter writer = new JsonTextWriter(sw))
            {
                writer.QuoteChar = '\'';

                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(writer, input);
            }

            return sb.ToString();
        }
    }
}