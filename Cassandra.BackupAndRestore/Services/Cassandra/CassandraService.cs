using Cassandra.BackupAndRestore.Base.Contracts.Services;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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

        public string GetTableDefinition(ISession session, string keyspaceName, string tableName)
        {
            // Fetch information from the database
            var tableSchema = session.Execute(new SimpleStatement($"SELECT * FROM system_schema.tables WHERE keyspace_name = '{keyspaceName}' AND table_name = '{tableName}'")).FirstOrDefault();
            if (tableSchema == null)
                throw new ArgumentNullException($"Couldnt find table schema for {keyspaceName}.{tableName}");
            var columnSchema = SortColumnSchema(session.Execute(new SimpleStatement($"SELECT * FROM system_schema.columns WHERE keyspace_name = '{keyspaceName}' AND table_name = '{tableName}'")));

            // Create table statement
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE {keyspaceName}.{tableName} (");

            // Append Column information
            foreach (var column in columnSchema)
                sb.AppendLine($"    {column.GetValue<string>("column_name")} {column.GetValue<string>("type")},");

            // Add Partition Key information
            sb.Append("    PRIMARY KEY (");
            sb.Append(GetPartitionKeyColumns(columnSchema));
            sb.AppendLine(")");

            // Add Clustering Order information
            sb.Append($") WITH CLUSTERING ORDER BY (");
            sb.Append(GetClusteringColumns(columnSchema));
            sb.AppendLine(")");

            // Table Options
            sb.AppendLine($"    AND bloom_filter_fp_chance = {tableSchema.GetValue<double>("bloom_filter_fp_chance").ToString("G", CultureInfo.InvariantCulture)}");
            sb.AppendLine($"    AND caching = {GetCassandraCompatibleJsonFromObject(tableSchema.GetValue<object>("caching"))}");
            sb.AppendLine($"    AND comment = '{tableSchema.GetValue<string>("comment")}'");
            sb.AppendLine($"    AND compaction = {GetCassandraCompatibleJsonFromObject(tableSchema.GetValue<object>("compaction"))}");
            sb.AppendLine($"    AND compression = {GetCassandraCompatibleJsonFromObject(tableSchema.GetValue<object>("compression"))}");
            sb.AppendLine($"    AND crc_check_chance = {tableSchema.GetValue<double>("crc_check_chance").ToString("G", CultureInfo.InvariantCulture)}");
            sb.AppendLine($"    AND dclocal_read_repair_chance = {tableSchema.GetValue<double>("dclocal_read_repair_chance").ToString("G", CultureInfo.InvariantCulture)}");
            sb.AppendLine($"    AND default_time_to_live = {tableSchema.GetValue<int>("default_time_to_live")}");
            sb.AppendLine($"    AND gc_grace_seconds = {tableSchema.GetValue<int>("gc_grace_seconds")}");
            sb.AppendLine($"    AND max_index_interval = {tableSchema.GetValue<int>("max_index_interval")}");
            sb.AppendLine($"    AND memtable_flush_period_in_ms = {tableSchema.GetValue<int>("memtable_flush_period_in_ms")}");
            sb.AppendLine($"    AND min_index_interval = {tableSchema.GetValue<int>("min_index_interval")}");
            sb.AppendLine($"    AND read_repair_chance = {tableSchema.GetValue<double>("read_repair_chance").ToString("G", CultureInfo.InvariantCulture)}");
            sb.AppendLine($"    AND speculative_retry = '{tableSchema.GetValue<string>("speculative_retry")}';");
            return sb.ToString();
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