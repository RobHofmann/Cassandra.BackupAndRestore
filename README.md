# Cassandra.BackupAndRestore
Backup tool for Cassandra. This exports the keyspace definitions, table definitions & data (CSV) to a file.

# Features
 - Backup keyspace definition (CQLSH format)
 - Backup table definition (CQLSH format)
 - Backup data (output is in CSV format which is compatible with the CQL "COPY" command)
 - Clean backup folder before backing up
 - Compress backups (zip format)
 - Delete uncompressed files after compressing
 - Add a timestamp to the zip-archives (use your own format)
 - Archive to a folder
 - Keep the last X backups in your archive folder (auto cleaning old backups)
 - Define a list of keyspaces/tables to exclude from backup (wildcard is usable).
 - Support for alerting to OpsGenie if something fails (unhandled exceptions at this point)
 - Proxy support (with or without authentication) for Alerting

# Example config
```
{
  "Cassandra": {
    "HostName": "localhost",
    "UserName": "",
    "Password": ""
  },
  "Backup": {
    "BackupTargetFolder": "D:/mybackupfolder",
    "EmptyBackupTargetFolderBeforeBackingUp": true,
    "CompressBackup": true,
    "DeleteLooseFilesAfterCompressing": true,
    "CompressTimestampFormat": "yyyyMMddHHmmss",
    "ExcludeList": [
      "system.*",
      "system_auth.role_permissions",
      "*somewildcardedvalue*"
    ]
  },
  "BackupArchiving": {
    "ArchiveFolder": "D:/myarchivefolder",
    "KeepNumberOfArchives": 4
  },
  "Alerting": {
    "Enable": true,
    "Type": "OpsGenie",
    "ApiKey": "OPSGENIE-API-KEY",
    "EnableProxy": false,
    "ProxyUrl": "http://proxy.domain.local:8080",
    "EnableProxyAuthentication": true,
    "ProxyUsername": "domain\\someuser",
    "ProxyPassword": "somepassword"
  }
}
```

 - BackupTargetFolder: where to store your current backup (use forward slashes in paths)
 - EmptyBackupTargetFolderBeforeBackingUp: Clean the BackupTargetFolder before backing up (true/false)
 - CompressBackup: Create a ZIP archive of your backup (true/false)
 - DeleteLooseFilesAfterCompressing: Clean up uncompressed files after creating the ZIP archive (true/false)
 - CompressTimestampFormat: Timestamp to add to the ZIP files. (.NET DateTime Formatting)
 - ExcludeList: A list of strings containing keyspaces/tables to exclude from backups.
 - ArchiveFolder: Where to build up your archive. Use this feature in combination with KeepNumberOfArchives (use forward slashes in paths. Leave empty to disable feature)
 - KeepNumberOfArchives: The amount of backups to keep in your archive folder.
 
# Build & Run
Build and publish the app in "self-contained" mode.

Simply add executable rights to the right file and run:

## Linux
```
chmod +x Cassandra.BackupAndRestore
./Cassandra.BackupAndRestore
```

## Windows
```
Cassandra.BackupAndRestore.exe
```
