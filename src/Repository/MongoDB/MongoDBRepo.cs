using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using SiaqodbCloudService.Models;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.Serialization.Options;

namespace SiaqodbCloudService.Repository.MongoDB
{
    class MongoDBRepo : IRepository
    {
        private const string DbServerUrl = @"mongodb://localhost:27017";
        private const string DbName = "siaqodb";
        private const string AccessKeysBucket = "sys_accesskeys";
        private const string SyncLogBucket = "sys_synclog";

        public async Task<StoreResponse> Delete(string bucketName, string key, string version)
        {
            throw new NotImplementedException();
        }

        public async Task<SiaqodbDocument> Get(string bucketName, string key, string version)
        {
            var client = new MongoClient(DbServerUrl);
            var db = client.GetDatabase(DbName);
            
            var collection = db.GetCollection<BsonDocument>(bucketName);
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("_id", key);
            if (version != null)
            {
                filter=filter & filterBuilder.Eq("_rev", version);
            }

            var doc = await collection.Find(filter).FirstOrDefaultAsync();
            if (doc != null)
            {
                return Mapper.ToSiaqodbDocument(doc);
            }
            return null;

        }

        public async Task<BatchSet> GetAllChanges(string bucketName, int limit, string anchor, string uploadAnchor)
        {
            var client = new MongoClient(DbServerUrl);
            var db = client.GetDatabase("local");

            var collection = db.GetCollection<BsonDocument>("oplog.rs");
            var filterBuilder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = filterBuilder.Eq("ns",DbName+"."+bucketName);
            if (anchor != null)
            {
               
                filter = filter & filterBuilder.Gt("ts", new BsonTimestamp(Convert.ToInt64(anchor)));
            }
            var docs= await collection.Find(filter).Limit(limit).ToListAsync();
            BatchSet bs = new BatchSet();
            SyncLogItem logItem = null;
            if (docs != null && docs.Count > 0 && !string.IsNullOrEmpty(uploadAnchor))
            {

                var dbLog = client.GetDatabase(DbName);

                var collectionLog = dbLog.GetCollection<BsonDocument>(SyncLogBucket);
                var filterLog = Builders<BsonDocument>.Filter.Eq("_id", ObjectId.Parse(uploadAnchor));
                var syncLogItem= await collectionLog.Find(filterLog).FirstOrDefaultAsync();
                if (syncLogItem != null)
                {
                    logItem = new SyncLogItem();
                    logItem.KeyVersion = new Dictionary<string, string>();
                    var logItemKV =syncLogItem["KeyVersion"].AsBsonDocument;
                    foreach (var k in logItemKV.Names)
                    {
                        logItem.KeyVersion.Add(k, logItemKV[k].ToString());
                    }
                }

            }
            
            int i = 0;
            foreach (var doc in docs)
            {

                if (i == docs.Count - 1)
                {
                    bs.Anchor = doc["ts"].AsBsonTimestamp.ToString();
                }
                if (doc["op"] == "i" || doc["op"] == "u")
                {
                    if (bs.ChangedDocuments ==null)
                        bs.ChangedDocuments = new List<SiaqodbDocument>();

                    var nestedDoc = doc["o"].AsBsonDocument;
                    if (logItem != null && logItem.KeyVersion != null && logItem.KeyVersion.ContainsKey(nestedDoc["_id"].AsString) && logItem.KeyVersion[nestedDoc["_id"].AsString] == nestedDoc["_rev"].AsString)
                        continue;
                    SiaqodbDocument siaqodbDoc = Mapper.ToSiaqodbDocument(nestedDoc);
                    bs.ChangedDocuments.Add(siaqodbDoc);
                }
                else if (doc["op"] == "d")
                {
                    if(bs.DeletedDocuments ==null)
                        bs.DeletedDocuments = new List<DeletedDocument>();

                    //check uploaded anchor- means cliet just uploaded this record and we should not return back
                    if (logItem != null && logItem.KeyVersion != null && logItem.KeyVersion.ContainsKey(doc["o"].AsBsonDocument["_id"].AsString))
                        continue;

                    DeletedDocument delDoc = new DeletedDocument();
                    delDoc.Key = doc["o"].AsBsonDocument["_id"].AsString;
                    bs.DeletedDocuments.Add(delDoc);
                }
               
                i++;
            }
            return bs;
            
        }

        public async Task<BatchSet> GetChanges(string bucketName, Filter query, int limit, string anchor, string uploadAnchor)
        {
            return await this.GetAllChanges(bucketName, limit, anchor, uploadAnchor);
        }

        public async Task<string> GetSecretAccessKey(string appKeyString)
        {
            return "4362kljh63k4599hhgm";
        }

        public async Task<StoreResponse> Store(string bucketName, SiaqodbDocument document)
        {
            var client = new MongoClient(DbServerUrl);
            var db = client.GetDatabase(DbName);

            var collection = db.GetCollection<BsonDocument>(bucketName);
            var newDoc = Mapper.ToBsonDoc(document);
            newDoc["_rev"] = this.GenerateNewVersion();
            try
            {
                var result = await collection.ReplaceOneAsync(filter: new BsonDocument { { "_id", document.Key }, { "_rev", BsonTypeMapper.MapToBsonValue(document.Version) } },
                                                                options: new UpdateOptions { IsUpsert = true },
                                                                 replacement: newDoc);
                var cnorResponse = new StoreResponse();
                cnorResponse.Version = newDoc["_rev"].AsString;
                cnorResponse.Key = document.Key;
                return cnorResponse;
            }
            catch (MongoWriteException ex)
            {
                if (ex.Message.Contains("duplicate key"))//conflict
                {
                    throw new ConflictException("There is a document with the same key and another version already stored");
                }
                throw ex;
            }

        }

        public async Task<BatchResponse> Store(string bucketName, BatchSet value)
        {
            var client = new MongoClient(DbServerUrl);
            var db = client.GetDatabase(DbName);
            
            var collection = db.GetCollection<BsonDocument>(bucketName);
            var response = new BatchResponse();
            response.BatchItemResponses = new List<BatchItemResponse>();
            SyncLogItem syncLogItem = new SyncLogItem();
            syncLogItem.KeyVersion = new Dictionary<string, string>();

            foreach (var document in value.ChangedDocuments)
            {
                var newDoc = Mapper.ToBsonDoc(document);
                newDoc["_rev"] = this.GenerateNewVersion();
                try
                {
                    var result = await collection.ReplaceOneAsync(filter: new BsonDocument { { "_id", document.Key }, { "_rev", BsonTypeMapper.MapToBsonValue(document.Version) } },
                                                                    options: new UpdateOptions { IsUpsert = true },
                                                                     replacement: newDoc);
                    BatchItemResponse itemResp = new BatchItemResponse();
                    itemResp.Key = document.Key;
                    itemResp.Version = newDoc["_rev"].AsString;
                    response.BatchItemResponses.Add(itemResp);
                    syncLogItem.KeyVersion.Add(itemResp.Key, itemResp.Version);

                }
                catch (MongoWriteException ex)
                {
                    BatchItemResponse itemResp = new BatchItemResponse();
                    itemResp.Key = document.Key;
                    itemResp.Version = document.Version;
                    if (ex.Message.Contains("duplicate key"))//conflict
                    {
                        itemResp.Error = "conflict";
                        itemResp.ErrorDesc = "conflict";//TODO better desc
                      
                    }
                    else
                    {
                        itemResp.Error = ex.Message;
                        itemResp.ErrorDesc = ex.Message;  
                    }
                    response.BatchItemResponses.Add(itemResp);
                }

            }
            //store uploadedAnchor
            if (syncLogItem.KeyVersion.Count > 0)
            {
                syncLogItem.TimeInserted = DateTime.UtcNow;
                BsonDocument syncLogDoc = new BsonDocument();
                syncLogDoc["KeyVersion"] = new BsonDocument(syncLogItem.KeyVersion);
                syncLogDoc["TimeInserted"] = syncLogItem.TimeInserted;
                var collectionLog = db.GetCollection<BsonDocument>(SyncLogBucket);
                await collectionLog.InsertOneAsync(syncLogDoc);
                response.UploadAnchor = syncLogDoc["_id"].ToString();
            }

            return response;
        }

        private BsonValue GenerateNewVersion()
        {
            Random rnd = new Random();
            return new BsonString(rnd.Next().ToString());
        }
    }
}