# ![App icon](static/appIcon.png) KV Store Tools Redux - Splunk App  
Rewrite of [Gemini KV Store Tools](https://splunkbase.splunk.com/app/3536/), managed by [Deductiv](https://www.deductiv.net/)  

## Utilities for the Splunk App Key-Value Store

The KV Store Tools for Splunk app includes the following features:
### Generating Commands  
- KV Store Backup: Backup KV Store collections to the file system on the search head.  
- KV Store Restore: Restore KV Store collections from backup jobs<sup>1</sup>.  Lists all existing backups in the default path if no arguments are given.
- KV Store Push: Copy KV Store collections from the local Splunk search head to a remote instance (SH/SHC)<sup>1</sup>.  
- KV Store Pull: Copy KV Store collections from a remote Splunk search head (SH/SHC) to the local instance<sup>1</sup>.  
- Delete Key: Delete KV Store records from a collection based on user input.  
### Streaming Commands
- Create Foreign Key: Creates an entry in a lookup and appends the resulting _key value to the current search results.  Useful for writing linked entries in two lookups.  
- Delete Keys: Delete KV Store records from a collection based on _key values in search results.  
### Alert Actions
- Send to Collection: Similar to outputlookup, but can be toggled on/off by users that have permissions to edit search jobs without modifying the search. This functionality has been implemented by Splunk directly into the product since this was written.  

[1]: Deletes the collections from the target host before writing (unless otherwise specified).  

* * *  
## Command Usage  

### KV Store Backup
This functionality is implemented through a generating search command.  Syntax:  

    | kvstorebackup app="app_name" collection="collection_name" path="/data/backup/kvstore" global_scope="false"  

The backup process will write one or more .json or .json.gz files (one for each collection).

**Arguments**:

- *(Optional)* app: <string> - Set the app in which to look for the collection(s).  (Default: All)
- *(Optional)* path: <string> - Set the directory path for the output files. (Default: the the setting in the app Setup page)
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)
- *(Optional)* collection: <string> - Specify the collection to backup. (Default: All)
- *(Optional)* compression: [true|false] - Specify whether or not to compress the backups. (Default: false)

**Best Practice**: In a Search Head Cluster (SHC) environment, map a shared network drive to all members so that the backed-up collections are available to all of them.

### KV Store Restore
This functionality is implemented through a generating search command.  Syntax:  

    | kvstorerestore filename="/backup/kvstore/app_name#collection_name#20170130*"  

The restore process will delete the KV Store collection and overwrite it with the contents of the backup.

**Arguments**:

- *(Optional)* filename: <string> - Specify the file to restore the data from.
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to restoring)

Running the search command with no arguments will list existing backups in the default path.

### KV Store Create Foreign Key
This functionality is implemented through a streaming search command.  Syntax (example):  

    search <events> | kvstorecreatefk collection="<collection1_name>" outputkeyfield="<key_field_name>" | outputlookup append=t <collection2_name>

**Arguments**:

- *(Required)* collection: <string> - Specify the collection to create the new record within.  
- *(Optional)* app: <string> - Specify the app to find the collection(s) within. (Default: current app)  
- *(Optional)* outputkeyfield: <string> - Specify the output field to write the new key value to. (Default: _key)  
- *(Optional)* staticvalues: <kvpairs> - Specify the static fields/values to write to the collection record.  (Default: None)  
- *(Optional)* dynamicvalues: <kvpairs> - Specify the dynamic fields/values to write to the collection record (e.g. lookup_fieldname=event_field). Uses the first non-null field value in the search results.  (Default: None)  
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to migrating)  

### KV Store Push
This functionality is implemented through a generating search command.  Configure your remote Splunk credentials in the Setup page.  Syntax:  

    | kvstorepush app="<app_name>" collection="<collection_name>" global_scope="[true|false]" append="[true|false]" target="<remote_host>"  

The replication process will delete the remote KV Store collection and overwrite it with the contents of the backup, unless append=true is set.

**Arguments**:

- *(Required)* target: <string> - Specify the hostname to upload collections to.
- *(Optional)* port: <integer> - Specify the target splunkd port on the remote host. (Default: 8089)
- *(Optional)* app: <string> - Specify the app to find the collection(s) within. (Default: All)
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)
- *(Optional)* collection: <string> - Specify the collection to migrate. (Default: All)
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to migrating)

### KV Store Pull
This functionality is implemented through a generating search command.  Requires setup of your remote Splunk credentials in the Setup page.  Syntax:  

    | kvstorepull app="<app_name>" collection="<collection_name>" global_scope="[true|false]" append="[true|false]" target="<remote_host>"  

The replication process will delete the local KV Store collection and overwrite it with the contents of the backup, unless append=true is set.

**Arguments**:

- *(Required)* target: <string> - Specify the hostname to download collections from.
- *(Optional)* port: <integer> - Specify the target splunkd port on the remote host. (Default: 8089)
- *(Optional)* app: <string> - Specify the app to find the collection(s) within. (Default: All)
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)
- *(Optional)* collection: <string> - Specify the collection to migrate. (Default: All)
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to migrating)

### KV Store Delete Keys
This functionality is implemented through a streaming search command.  Syntax (example):  

    | inputlookup lookup_name where domain="*splunk.com" | deletekeys collection="collection_name"  

Deletes records from a KV Store collection based on _key value in search results

**Arguments**:

- *(Optional)* app: <string> - Specify the app to find the collection within. (Default: All)
- *(Required)* collection: <string> - Specify the collection to delete the data from.

### KV Store Delete Key
This functionality is implemented through a generating search command.  Syntax:  

    | deletekey collection="collection_name" key="key_value"  

Deletes a specific record from a KV Store collection based on _key value

**Arguments**:

- *(Optional)* app: <string> - Specify the app to find the collection within. (Default: All)
- *(Required)* collection: <string> - Specify the collection to delete the data from.
- *(Required)* key: <string> - Specify the value for the _key field in the collection record.
