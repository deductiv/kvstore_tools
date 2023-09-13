# ![App icon](static/appIcon.png) KV Store Tools - Splunk App by Deductiv  
## Utilities for the Splunk App Key-Value Store  
Rewrite of [Gemini KV Store Tools](https://splunkbase.splunk.com/app/3536/)  
  
The KV Store Tools for Splunk app includes the following features:  
### Backup & Restore Collections  
- KV Store Backup: Backup KV Store collections to the file system on the search head.  
- KV Store Restore: Restore KV Store collections from backup jobs<sup>1</sup>.  Lists all existing backups in the default path if no arguments are given.  
### Synchronize Collections  
- KV Store Push: Copy KV Store collections from the local Splunk search head to a remote instance (SH/SHC)<sup>1</sup>.  
- KV Store Pull: Copy KV Store collections from a remote Splunk search head (SH/SHC) to the local instance<sup>1</sup>.  
### Extra Functions  
- Delete Keys: Delete KV Store records from a collection based on _key values in search results.  
- Delete Key: Delete KV Store records from a collection based on user input.  
- Create Foreign Key: Creates an entry in a lookup and appends the resulting _key value to the current search results.  Useful for writing linked entries in two lookups within a single search.  
### Alert Actions
- Send to Collection: Similar to outputlookup, but can be toggled on/off by users that have permissions to edit search jobs without modifying the search. This functionality has been implemented by Splunk directly into the product since this was written.  

[1]: Deletes the collections from the target host before writing (unless otherwise specified).  

* * *  
## Command Usage  

### KV Store Backup  
Back up a KV Store collections to disk on the local node or object storage.  The backup process will write one or more .json or .json.gz files (one for each collection).  For search head clusters, it's recommended to have a shared volume (e.g. NFS) among all nodes or object storage for backups to reliably enforce the retention policy and the kvstorerestore command functionality.  
  
This functionality is implemented through a generating search command.  Syntax:  

    | kvstorebackup app="app_name" collection="collection_name" path="/data/backup/kvstore" global_scope="false"  

**Arguments**

- *(Optional)* target: <string> - Set the app in which to look for the collection(s).  (Default: Selected from the App Setup page)  
- *(Optional)* app: <string> - Set the app in which to look for the collection(s).  (Default: All)  
- *(Optional)* path: <string> - Set the directory path for the output files. (Default: the the setting in the app Setup page)  
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)  
- *(Optional)* collection: <string> - Specify the collection to backup. (Default: All)  
- *(Optional)* compression: [true|false] - Specify whether or not to compress the backups. (Default: false)  

### KV Store Restore  
Restore a KV Store collection backup file to the local node.  Uses the filename to determine the app name and collection to write the data to.  By default, the restore process will delete the KV Store collection and overwrite it with the contents of the backup unless append=true is set.  Running the search command with no arguments will list existing backups in the default path.  
  
This functionality is implemented through a generating search command. Syntax:  

    | kvstorerestore filename="/backup/kvstore/app_name#collection_name#20170130*"  

**Arguments**

- *(Optional)* filename: <string> - Specify the file to restore the data from.  
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to restoring)  

### KV Store Push  
Upload local KV Store collection(s) to one or more target instances.  Configure your remote Splunk credentials in the Setup page.  The replication process will delete the target KV Store collection and overwrite it with the local contents unless append=true is set.  
  
This functionality is implemented through a generating search command. Syntax:  

    | kvstorepush app="<app_name>" collection="<collection_name>" global_scope="[true|false]" append="[true|false]" target="<remote_hosts>"  

**Arguments**

- *(Required)* target: <string> - Specify the hostnames in a comma separated list to upload collections to.  
- *(Optional)* port: <integer> - Specify the target splunkd port on the remote host. (Default: 8089)  
- *(Optional)* app: <string> - Specify the app to find the collection(s) within. (Default: All)  
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)  
- *(Optional)* collection: <string> - Specify the collection to migrate. (Default: All)  
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to migrating)  

### KV Store Pull
Download local KV Store collection(s) from another instance to the local one.  Configure your remote Splunk credentials in the Setup page.  The replication process will delete the local KV Store collection and overwrite it with the remote contents unless append=true is set.  
  
This functionality is implemented through a generating search command. Syntax:  
  
    | kvstorepull app="<app_name>" collection="<collection_name>" global_scope="[true|false]" append="[true|false]" target="<remote_host>"  

**Arguments**

- *(Required)* target: <string> - Specify the hostname to download collections from.  
- *(Optional)* port: <integer> - Specify the target splunkd port on the remote host. (Default: 8089)  
- *(Optional)* app: <string> - Specify the app to find the collection(s) within. (Default: All)  
- *(Optional)* global_scope: [true|false] - Specify the whether or not to include all globally available collections. (Default: false)  
- *(Optional)* collection: <string> - Specify the collection to migrate. (Default: All)  
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to migrating)  

### KV Store Create Foreign Key  
Writes data from the search into a new KV store collection record and returns the record's _key value into the search as a new field.  The _key value becomes a foreign key reference in the search results, which can be written to a second lookup using outputlookup.  
  
This functionality is implemented through a streaming search command.  Syntax (example):  
  
    search <events> | kvstorecreatefk collection="<collection1_name>" outputkeyfield="<key_field_name>" | outputlookup append=t <collection2_name>  

**Arguments**

- *(Required)* collection: <string> - Specify the collection to create the new record within.  
- *(Optional)* app: <string> - Specify the app to find the collection(s) within. (Default: current app)  
- *(Optional)* outputkeyfield: <string> - Specify the output field to write the new key value to. (Default: _key)  
- *(Optional)* outputvalues: <kvpairs> - Specify the fields/values to write to the collection record (e.g. lookup_fieldname=$event_field$). Uses the first non-null field value in the search results. (Default: None)  
- *(Optional)* append: [true|false] - Specify whether or not to append records to the target KV Store collections. (Default: false - deletes the collection prior to migrating)  

	app
    		Syntax: app=<appname>
			Description: Specify the app where the collections lives. 
			Default: Current app

	collection
		    Syntax: collection=<collection_name>
			Description: Specify the collection to create the new record within.
			Default: None

	outputkeyfield
		    Syntax: outputkeyfield=<field_name>
			Description: Specify the output field to write the new key value to. 
			Default: [collection]_key

	outputvalues 
			Syntax: outputvalues=<kvpairs>
			Description: Specify the output fields/values to write to the new collection record. 
				Variable values use the first non-null field value in the search results.
			Example: outputvalues="kvfield1=Web Server, kvtimestamp=2020-01-01, kvstatus=$http_status$"  
			Default: None
			Special keywords: 
				$kv_now$ = Current timestamp in epoch format
				$kv_current_userid$ = Current Splunk user ID
			
	groupby 
            Syntax: groupby=<field_name>","
			Description: Specify the field to group records by.  Creates a unique _key record for each distinct value.
			Default: None 
	delimiter 
			Syntax: delimiter=","
			Description: Specify the delimiter for key-value pair strings. 
			Default: Comma (,)

### KV Store Delete Keys
Delete multiple KV Store collection records based on the _key value from the search result input.  
  
This functionality is implemented through a streaming search command.  Syntax (example):  
  
    | inputlookup lookup_name where domain="*splunk.com" | kvstoredeletekeys collection="collection_name"  
  
**Arguments**

- *(Optional)* app: <string> - Specify the app to find the collection within. (Default: All)  
- *(Required)* collection: <string> - Specify the collection to delete the data from.  

### KV Store Delete Key
Deletes a specific record from a KV Store collection based on _key value.  
  
This functionality is implemented through a generating search command.  Syntax:  
  
    | kvstoredeletekey collection="collection_name" key="key_value"  

**Arguments**

- *(Optional)* app: <string> - Specify the app to find the collection within. (Default: All)  
- *(Required)* collection: <string> - Specify the collection to delete the data from.  
- *(Required)* key: <string> - Specify the value for the _key field in the collection record.  
