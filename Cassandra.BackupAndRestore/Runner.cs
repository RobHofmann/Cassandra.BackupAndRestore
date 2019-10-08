using Cassandra.BackupAndRestore.Base.Contracts.Helpers;
using Cassandra.BackupAndRestore.Base.Contracts.Services;
using Cassandra.BackupAndRestore.Base.Models.Configuration;
using Cassandra.BackupAndRestore.Utils.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra.BackupAndRestore
{
    internal class Runner
    {
        private readonly ILogger _logger;
        private readonly IAlerter _alerter;
        private readonly ICounter _counter;
        private readonly ICassandraService _cassandraService;
        private readonly IPatternMatching _patternMatching;
        private readonly CassandraConfiguration _cassandraConfiguration;
        private readonly BackupConfiguration _backupConfiguration;
        private readonly BackupArchivingConfiguration _backupArchivingConfiguration;
        private readonly AlertingConfiguration _alertingConfiguration;


        public Runner(ILogger logger, ICounter counter, ICassandraService cassandraService,
            CassandraConfiguration cassandraConfiguration, BackupConfiguration backupConfiguration,
            BackupArchivingConfiguration backupArchivingConfiguration, IAlerter alerter,
            AlertingConfiguration alertingConfiguration, IPatternMatching patternMatching)
        {
            _logger = logger;
            _alerter = alerter;
            _counter = counter;
            _cassandraService = cassandraService;
            _cassandraConfiguration = cassandraConfiguration;
            _backupArchivingConfiguration = backupArchivingConfiguration;
            _backupConfiguration = backupConfiguration;
            _alertingConfiguration = alertingConfiguration;
            _patternMatching = patternMatching;
        }

        public async Task RunAsync()
        {
            try
            {
                _logger.Log("Hello World! Welcome to the Cassandra Backup & Restore tool");
                _logger.Log("BACKUP & RESTORE ALL THE THINGS!!!!");

                // Create Backup Timestamp
                DateTime backupTimestamp = DateTime.UtcNow;

                // Create connection
                _logger.Log("Connecting to Cassandra cluster...");
                if (_cassandraConfiguration.UserName?.Length > 0 || _cassandraConfiguration.Password?.Length > 0)
                    _logger.Log("Using username & password");
                var cluster = _cassandraService.GetCluster(_cassandraConfiguration.HostName, _cassandraConfiguration.UserName, _cassandraConfiguration.Password);
                var session = await _cassandraService.GetConnectionAsync(cluster);
                _logger.Log("Connected to Cassandra cluster...");

                // Setup backup directory
                var backupDirectoryInfo = new DirectoryInfo(_backupConfiguration.BackupTargetFolder);
                if (!backupDirectoryInfo.Exists)
                {
                    _logger.Log("Backup target folder does not exist. Creating.");
                    backupDirectoryInfo.Create();
                    _logger.Log("Backup target folder created");
                }

                if (_backupConfiguration.EmptyBackupTargetFolderBeforeBackingUp)
                {
                    _logger.Log("Cleaning backup folder before starting backup");
                    CleanDirectory(backupDirectoryInfo);
                    _logger.Log("Cleaned backup folder before starting backup");
                }

                // Fetch all tables
                _logger.Log("Fetching all tables");
                var allTableRowSet = await _cassandraService.GetAllTablesAsync(session);
                _logger.Log("Fetched all tables");

                foreach (var tableRow in allTableRowSet)
                {
                    string keyspaceName = tableRow.GetValue<string>("keyspace_name");
                    string tableName = tableRow.GetValue<string>("table_name");
                    string fullTableName = $"{keyspaceName}.{tableName}";

                    // Check if we need to skip this table (Exclude list)
                    if (_patternMatching.IsInList(fullTableName, _backupConfiguration.ExcludeList))
                    {
                        _logger.Log("Skipping. This table matches the exclude list", fullTableName);
                        continue;
                    }

                    await DoBackupTable(cluster, session, backupTimestamp, keyspaceName, tableName, fullTableName);
                }

                if (!string.IsNullOrEmpty(_backupArchivingConfiguration.ArchiveFolder))
                {
                    _logger.Log("Archiving");
                    var archiveDirectoryInfo = new DirectoryInfo(_backupArchivingConfiguration.ArchiveFolder);
                    if (!archiveDirectoryInfo.Exists)
                        archiveDirectoryInfo.Create();

                    var timestampedArchiveDirectory = new DirectoryInfo($"{_backupArchivingConfiguration.ArchiveFolder}/{backupTimestamp.ToString(_backupConfiguration.CompressTimestampFormat)}");
                    if (!timestampedArchiveDirectory.Exists)
                        timestampedArchiveDirectory.Create();

                    CopyAll(backupDirectoryInfo, timestampedArchiveDirectory);
                    _logger.Log("Archived");

                    if (_backupArchivingConfiguration.KeepNumberOfArchives > 0)
                    {
                        _logger.Log("Old archive cleanup");
                        List<DirectoryInfo> timestampedArchiveDirectories = archiveDirectoryInfo.GetDirectories().OrderByDescending(x => x.CreationTime).ToList();
                        for (int i = _backupArchivingConfiguration.KeepNumberOfArchives; i < timestampedArchiveDirectories.Count; i++)
                            timestampedArchiveDirectories[i].Delete(true);
                        _logger.Log("Finished old archive cleanup");
                    }
                }

                _logger.Log("Done!");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                _alerter.Alert(ex.ToString(), AlertPriority.Low);
                throw;
            }
        }

        private async Task DoBackupTable(ICluster cluster, ISession session, DateTime backupTimestamp, string keyspaceName, string tableName, string fullTableName)
        {
            var keyspaceDefinitionFileInfo = new FileInfo($"{_backupConfiguration.BackupTargetFolder}/{keyspaceName}.keyspace");
            var tableDefinitionFileInfo = new FileInfo($"{_backupConfiguration.BackupTargetFolder}/{fullTableName}.table");
            var csvFileInfo = new FileInfo($"{_backupConfiguration.BackupTargetFolder}/{fullTableName}.csv");

            _logger.Log("Starting backup", fullTableName);

            _logger.Log("Backing up keyspace definition", fullTableName);
            BackupKeyspaceDefinition(cluster, keyspaceDefinitionFileInfo, keyspaceName);
            _logger.Log("Finished backing up keyspace definition", fullTableName);
            _logger.Log("Backing up table definition", fullTableName);
            BackupTableDefinition(session, tableDefinitionFileInfo, keyspaceName, tableName);
            _logger.Log("Finished backing up table definition", fullTableName);

            _logger.Log("Fetching rows", fullTableName);
            var dataRows = await _cassandraService.GetRowsAsync(session, fullTableName);
            _logger.Log("Fetched rows", fullTableName);

            _logger.Log("Start writing data to CSV", fullTableName);
            WriteDataCsv(dataRows, csvFileInfo);
            _logger.Log($"Finished writing {_counter.ReadCounter()} rows to CSV", fullTableName);

            if (_backupConfiguration.CompressBackup)
            {
                _logger.Log("Compressing", fullTableName);
                CompressBackup(keyspaceDefinitionFileInfo, tableDefinitionFileInfo, csvFileInfo, backupTimestamp, fullTableName);
                _logger.Log("Compressed", fullTableName);

                if (_backupConfiguration.DeleteLooseFilesAfterCompressing)
                {
                    _logger.Log("Deleting loose files", fullTableName);
                    DeleteLooseFilesAfterCompressing(keyspaceDefinitionFileInfo, tableDefinitionFileInfo, csvFileInfo);
                    _logger.Log("Deleted loose files", fullTableName);
                }
            }
            _logger.Log("Finished backup", fullTableName);
        }

        private void BackupKeyspaceDefinition(ICluster cluster, FileInfo keyspaceDefinitionFileInfo, string keyspaceName)
        {
            var keyspaceDefinition = _cassandraService.GetKeyspaceDefinition(cluster, keyspaceName);
            using (TextWriter tw = new StreamWriter(keyspaceDefinitionFileInfo.FullName, false, Encoding.ASCII))
                tw.Write(keyspaceDefinition);
        }

        private void BackupTableDefinition(ISession session, FileInfo tableDefinitionFileInfo, string keyspaceName, string tableName)
        {
            var tableDefinition = _cassandraService.GetTableDefinition(session, keyspaceName, tableName);
            using (TextWriter tw = new StreamWriter(tableDefinitionFileInfo.FullName, false, Encoding.ASCII))
                tw.Write(tableDefinition);
        }

        private void CompressBackup(FileInfo keyspaceDefinitionFileInfo, FileInfo tableDefinitionFileInfo, FileInfo csvFileInfo, DateTime backupTimestamp, string fullTableName)
        {
            FileInfo zipFileInfo;
            if (!string.IsNullOrEmpty(_backupConfiguration.CompressTimestampFormat))
                zipFileInfo = new FileInfo($"{_backupConfiguration.BackupTargetFolder}/{fullTableName}.{backupTimestamp.ToString(_backupConfiguration.CompressTimestampFormat)}.zip");
            else
                zipFileInfo = new FileInfo($"{_backupConfiguration.BackupTargetFolder}/{fullTableName}.zip");

            using (ZipArchive archive = ZipFile.Open(zipFileInfo.FullName, ZipArchiveMode.Create))
            {
                archive.CreateEntryFromFile(keyspaceDefinitionFileInfo.FullName, keyspaceDefinitionFileInfo.Name);
                archive.CreateEntryFromFile(tableDefinitionFileInfo.FullName, tableDefinitionFileInfo.Name);
                archive.CreateEntryFromFile(csvFileInfo.FullName, csvFileInfo.Name);
            }
        }

        private void DeleteLooseFilesAfterCompressing(FileInfo keyspaceDefinitionFileInfo, FileInfo tableDefinitionFileInfo, FileInfo csvFileInfo)
        {
            keyspaceDefinitionFileInfo.Delete();
            tableDefinitionFileInfo.Delete();
            csvFileInfo.Delete();
        }

        private void WriteDataCsv(RowSet dataRows, FileInfo csvFileInfo)
        {
            using (TextWriter tw = new StreamWriter(csvFileInfo.FullName, false, Encoding.ASCII))
            {
                tw.NewLine = "\r\n";
                _counter.Reset();
                foreach (var dataRow in dataRows)
                {
                    var stringRow = new string[dataRows.Columns.Length];
                    for (int i = 0; i < dataRows.Columns.Length; i++)
                    {
                        var data = dataRow.GetValue<object>(dataRows.Columns[i].Name);
                        if (data == null)
                        {
                            stringRow[i] = "";
                            continue;
                        }

                        if (data is DateTimeOffset)
                            stringRow[i] = ((DateTimeOffset)data).ToString("yyyy-MM-dd HH:mm:ss.fff+0000");
                        else if (data is string)
                            stringRow[i] = (data as string).ToCsvString();
                        else
                            stringRow[i] = data.ToString();
                    }
                    tw.WriteLine(string.Join(",", stringRow));
                    _counter.IncrementCounter();
                }
            }
        }

        private void CleanDirectory(DirectoryInfo di)
        {
            foreach (FileInfo file in di.GetFiles())
                file.Delete();
            foreach (DirectoryInfo dir in di.GetDirectories())
                dir.Delete(true);
        }

        private static void CopyAll(DirectoryInfo source, DirectoryInfo target)
        {
            Directory.CreateDirectory(target.FullName);

            // Copy each file into the new directory.
            foreach (FileInfo fi in source.GetFiles())
                fi.CopyTo(Path.Combine(target.FullName, fi.Name), true);

            // Copy each subdirectory using recursion.
            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories())
                CopyAll(diSourceSubDir, target.CreateSubdirectory(diSourceSubDir.Name));
        }
    }
}
