using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.BackupAndRestore.Base.Contracts.Services
{
    internal interface ICassandraService
    {
        ICluster GetCluster(string cassandraHostName, string cassandraUsername, string cassandraPassword);

        Task<ISession> GetConnectionAsync(ICluster cluster);

        Task<RowSet> GetRowsAsync(ISession session, string table);

        Task<RowSet> GetAllTablesAsync(ISession session);

        string GetKeyspaceDefinition(ICluster cluster, string keyspaceName);

        string GetTableDefinition(ISession session, string keyspaceName, string tableName);
    }
}
