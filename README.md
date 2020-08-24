# KV Store Tools app for Splunk (Fork of Gemini KV Store Tools)

## Utilities for the Splunk App Key-Value Store

The KV Store Tools for Splunk app includes the following features:

- KV Store backup: Backup KV Store collections.
- KV Store restore: Restore KV Store collections from backup jobs. Deletes the existing collections before restoring (unless otherwise specified).
- KV Store migrate: Copy KV Store collections from one Splunk search head to another. Deletes the existing collections before restoring (unless otherwise specified).
- Delete Keys: Delete KV Store records from a collection based on _key values in search results.
- Delete Key: Delete KV Store records from a collection based on user input.
- KV Store alert action: Similar to outputlookup, but can be toggled on/off by users that have permissions to edit search jobs without modifying the search.

### KV Store Backup
This functionality is implemented through a generating search command.  Simply run or schedule a search like the following:
``` | kvstorebackup app="app_name" collection="collection_name" path="/data/backup/kvstore" global_scope="false" ```

The backup process will write one or more .json or .json.gz files (one for each collection).

**Arguments**:

- *(Optional)* app: <string> - Set the app in which to look for the collection(s).  (Default: All)
- *(Optional)* path: <string> - Set the directory path for the output files. (Default: the the setting in the app Setup page)
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)
- *(Optional)* collection: <string> - Specify the collection to backup. (Default: All)
- *(Optional)* compression: [true|false] - Specify whether or not to compress the backups. (Default: false)

**Best Practice**: In a Search Head Cluster (SHC) environment, map a shared network drive to all members so that the backed-up collections are available to all of them.

### KV Store Restore
This functionality is implemented through a generating search command.  Run a search such as:
``` | kvstorerestore filename="/backup/kvstore/app_name#collection_name#20170130*" ```

The restore process will delete the KV Store collection and overwrite it with the contents of the backup.

Arguments:

- *(Optional)* filename: <string> - Specify the file to restore the data from.
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to restoring)

Running the search command with no arguments will list existing backups in the default path.

### KV Store Migrate
This functionality is implemented through a generating search command.  Configure your remote Splunk credentials in the Setup page, then run a search such as:
``` | kvstoremigrate app="app_name" collection="collection_name" global_scope="false" append="true" target="remotehost" ```

The migration process will delete the remote KV Store collection and overwrite it with the contents of the backup, unless append=true is set.

Arguments:

- *(Required)* target: <string> - Specify the hostname to migrate collections to.
- *(Optional)* port: <integer> - Specify the target splunkd port on the remote host. (Default: 8089)
- *(Optional)* app: <string> - Specify the app to find the collection(s) within. (Default: All)
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)
- *(Optional)* collection: <string> - Specify the collection to migrate. (Default: All)
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to migrating)

### KV Store Delete Keys
This functionality is implemented through a streaming search command.  Run a search such as:
``` | inputlookup lookup_name where domain="*splunk.com" | deletekeys collection="collection_name" ```

Deletes records from a KV Store collection based on _key value in search results

Arguments:

- *(Optional)* app: <string> - Specify the app to find the collection within. (Default: All)
- *(Required)* collection: <string> - Specify the collection to delete the data from.

### KV Store Delete Key
This functionality is implemented through a generating search command.  Run a search such as:
``` | deletekey collection="collection_name" key="key_value" ```

Deletes a specific record from a KV Store collection based on _key value

Arguments:

- *(Optional)* app: <string> - Specify the app to find the collection within. (Default: All)
- *(Required)* collection: <string> - Specify the collection to delete the data from.
- *(Required)* key: <string> - Specify the value for the _key field in the collection record.
