
## SiaqodbCloud

SiaqodbCloud is a RESTful web service through which you can synchronize [Siaqodb](http://siaqodb.com) from client side( Windows, WindowsStore, Xamarin) with a server side NoSQL database like MongoDB or CouchDB.

## MongoDB

MongoDB - you can download it from [here](https://www.mongodb.org/downloads) ( min version 3.2.X), install it and then create a config file(eq: mongo.cfg) that looks something like this:
```json
systemLog:
    destination: file
    path: c:\data\log\mongod.log
storage:
    dbPath: c:\data\db
replication:
   oplogSizeMB: 100
   replSetName: rs0
```
Start mongod with this config(example on Windows: "C:\mongodb\bin\mongod.exe" --config "C:\mongodb\mongo.cfg" ).
The important section in the above config is the 'replication' section which will enable MongoDB to store all changes in oplog.rs collection. The entire Sync process relies on  [Mongo's oplog](https://docs.mongodb.org/manual/core/replica-set-oplog/).
After MongoDB server is started, create a database called 'siaqodb'.(If you preffer another name, you can change it in src\Repository\MongoDB\MongoDBRepo.cs). Then inside database created, create 2 collections:  'sys_accesskeys' and 'sys_synclog'. (The names can be changed also in \src\Repository\CouchDB\MongoDBRepo.cs ).

## CouchDB

CouchDB - you can download it from [here](http://couchdb.apache.org/) ( min version 1.6.1), install it and then create 2 databases: 'sys_accesskeys' and 'sys_synclog'. (The names can be changed in \src\Repository\CouchDB\CouchDBRepo.cs ). 

CouchDB uses HTTP protocol, so after the installation, go to \src\Repository\CouchDB\CouchDBRepo.cs and  modify the default CouchDB URL:

```java
  private const string DbServerUrl = @"http://127.0.0.1:5984/";
```
with your own.


## Authentication

Requests to SiaqodbCloud service are signed with a HMAC-SHA256 signature. Client code needs 'access_key_id' (which is public and included in the header of the request) and 'secret_key' which is used to build HMAC-SHA256 signature. The 'secret_key' must be provided in the client app but it's never transmitted. 
'access_key_id' and 'secret_key' should be stored in sys_accesskeys database/collection. 
Example of a JSON record stored in CouchDB database or MongoDB collection called 'sys_accesskeys':

```JSON
{
   "_id": "3ba69b5835dgdb308766b4756b00079a",
   "secretkey": "4362kljh63k4599hhgm"
}
```

## Siaqodb Sync Example

Once you have the setup ready and the WebAPI running, you can Sync Siaqodb with MongoDB or CouchDB, example:
```java
  using (SiaqodbSync syncContext = new SiaqodbSync("http://localhost:11735/v0/", 
  "3ba69b5835dgdb308766b4756b00079a", 
  "4362kljh63k4599hhgm"))
 {
    IBucket bucket = siaqodb.Documents["persons"];
    syncContext.Push(bucket);//push local changes to server
    syncContext.Pull(bucket);//pull server changes to client db
            
   }
 ```




