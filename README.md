
## SiaqodbCloud

SiaqodbCloud is a RESTful web service through which you can synchronize [Siaqodb](http://siaqodb.com) from client side( Windows, WindowsStore, Xamarin) with a server side NoSQL database like CouchDB or MongoDB. (For now only CouchDB is supported)

## Pre-Requirements

CouchDB - you can download it from [here](http://couchdb.apache.org/) ( min version 1.6.1), install it and then create 2 databases and name them: 'sys_accesskeys' and 'sys_synclog'. (The names can be changed in \src\Repository\CouchDB\CouchDBRepo.cs ). 

CouchDB uses HTTP protocol, so after installation go to \src\Repository\CouchDB\CouchDBRepo.cs and  modify the default CouchDB URL:

```java
  private const string DbServerUrl = @"http://127.0.0.1:5984/";
```
with your own.


## Authentication

Requests to SiaqodbCloud service must be signed with a HMAC-SHA256 signature.They must include information that SiaqodbCloud can use to authenticate the requestor. Requests are signed using the 'access_key_id' (which is public and included in header of the request) and 'secret_key' which is used to build HMAC-SHA256 signature. The 'secret_key' must be provided in client app but is never transmitted. If CouchDB is used as server side database then 'access_key_id' and 'secret_key' should be stored in sys_accesskeys. 
Example of a JSON record stored in CouchDB database called 'sys_accesskeys':

```JSON
{
   "_id": "3ba69b5835dgdb308766b4756b00079a",
   "_rev": "1-3537c3edcd21889191eaf0f0a2a35835",
   "secretkey": "4362kljh63k4599hhgm"
}
```

## Siaqodb Sync Example

Once you have setup ready and WebAPI running, you can now Sync Siaqodb with CouchDB, example:
```java
  using (SiaqodbSync syncContext = new SiaqodbSync("http://localhost:11735/v0/", "3ba69b5835dgdb308766b4756b00079a", "4362kljh63k4599hhgm"))
            {
                 IBucket bucket = siaqodb.Documents["persons"];
                 syncContext.Push(bucket);//push local changes to server
                 syncContext.Pull(bucket);//pull server changes to client db
            
            }
            ```




