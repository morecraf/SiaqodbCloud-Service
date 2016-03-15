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
        private MongoClient client;
        public MongoDBRepo()
        {
            client= new MongoClient(DbServerUrl); 
        }
        public async Task<StoreResponse> Delete(string bucketName, string key, string version)
        {
            var db = client.GetDatabase(DbName);
            var collection = db.GetCollection<BsonDocument>(bucketName);
            var filter = Builders<BsonDocument>.Filter.Eq("_id", key);

            var result=await collection.DeleteOneAsync(filter);
            return new StoreResponse { Key = key, Version = version };
        }

        public async Task<SiaqodbDocument> Get(string bucketName, string key, string version)
        {
           
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

            return await this.GetChanges(bucketName,null, limit, anchor, uploadAnchor);
        }

        public async Task<BatchSet> GetChanges(string bucketName, Filter query, int limit, string anchor, string uploadAnchor)
        {
           
          
            var db = client.GetDatabase("local");

            var collection = db.GetCollection<BsonDocument>("oplog.rs");
            var filterBuilder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter = filterBuilder.Eq("ns", DbName + "." + bucketName);
            if (anchor != null)
            {
                filter = filter & filterBuilder.Gt("ts", new BsonTimestamp(Convert.ToInt64(anchor)));
            }
            var docs = await collection.Find(filter).Limit(limit).ToListAsync();
            BatchSet bs = new BatchSet();
            SyncLogItem logItem = null;
            if (docs != null && docs.Count > 0 && !string.IsNullOrEmpty(uploadAnchor))
            {
                logItem = await GetSyncLogItem(uploadAnchor);
            }

            int i = 0;
            foreach (var doc in docs)
            {

                if (i == docs.Count - 1)
                {
                    bs.Anchor = doc["ts"].AsBsonTimestamp.ToString();
                }
                i++;
                if (doc["op"] == "i" || doc["op"] == "u")
                {
                    this.AddChangedDoc(bs, doc, logItem, query);
                }
                else if (doc["op"] == "d")
                {
                    this.AddDeletedDoc(bs, doc, logItem);
                }


            }
            return bs;
        }

        private void AddDeletedDoc(BatchSet bs, BsonDocument doc, SyncLogItem logItem)
        {
            if (bs.DeletedDocuments == null)
                bs.DeletedDocuments = new List<DeletedDocument>();

            //check uploaded anchor- means cliet just uploaded this record and we should not return back
            if (logItem != null && logItem.KeyVersion != null && logItem.KeyVersion.ContainsKey(doc["o"].AsBsonDocument["_id"].AsString))
                return;

            DeletedDocument delDoc = new DeletedDocument();
            delDoc.Key = doc["o"].AsBsonDocument["_id"].AsString;
            bs.DeletedDocuments.Add(delDoc);
        }

        private void AddChangedDoc(BatchSet bs, BsonDocument doc, SyncLogItem logItem, Filter query)
        {

            if (bs.ChangedDocuments == null)
                bs.ChangedDocuments = new List<SiaqodbDocument>();

            var nestedDoc = doc["o"].AsBsonDocument;
            if (logItem != null && logItem.KeyVersion != null && logItem.KeyVersion.ContainsKey(nestedDoc["_id"].AsString) && logItem.KeyVersion[nestedDoc["_id"].AsString] == nestedDoc["_rev"].AsString)
                return;
            if (OutOfFilter(query, nestedDoc))
                return;
            SiaqodbDocument siaqodbDoc = Mapper.ToSiaqodbDocument(nestedDoc);
            bs.ChangedDocuments.Add(siaqodbDoc);

        }

        private async Task<SyncLogItem> GetSyncLogItem(string uploadAnchor)
        {
            var dbLog = client.GetDatabase(DbName);

            var collectionLog = dbLog.GetCollection<BsonDocument>(SyncLogBucket);
            var filterLog = Builders<BsonDocument>.Filter.Eq("_id", ObjectId.Parse(uploadAnchor));
            var syncLogItem = await collectionLog.Find(filterLog).FirstOrDefaultAsync();
            if (syncLogItem != null)
            {
                var logItem = new SyncLogItem();
                logItem.KeyVersion = new Dictionary<string, string>();
                var logItemKV = syncLogItem["KeyVersion"].AsBsonDocument;
                foreach (var k in logItemKV.Names)
                {
                    logItem.KeyVersion.Add(k, logItemKV[k].ToString());
                }
                return logItem;
            }
            return null;
        }

        private bool OutOfFilter(Filter query, BsonDocument nestedDoc)
        {
            if (query == null)
                return false;
            string tagName = query.TagName;
            if (query.TagName == "key")
            {
                tagName = "_id";
            }
            if (!nestedDoc.Names.Contains(tagName))
            {
                return true;
            }

            IComparable docValue = (IComparable)BsonTypeMapper.MapToDotNetValue(nestedDoc[tagName]);
            if (query.Value != null && docValue.CompareTo((IComparable)query.Value) != 0)
            {
                return true;
            }
            if (query.Start != null && docValue.CompareTo((IComparable)query.Start) < 0)
                return true;
            if (query.End != null && docValue.CompareTo((IComparable)query.End) > 0)
                return true;


            return false;
        }

        public async Task<string> GetSecretAccessKey(string appKeyString)
        {
            var db = client.GetDatabase(DbName);
            var collection = db.GetCollection<BsonDocument>(AccessKeysBucket);
            var accessKeyDoc = await collection.Find(filter: new BsonDocument { { "_id", appKeyString } }).FirstOrDefaultAsync();
            if (accessKeyDoc != null && accessKeyDoc.Names.Contains("secretkey"))
                return accessKeyDoc["secretkey"].AsString;
            return null;
        }

        public async Task<StoreResponse> Store(string bucketName, SiaqodbDocument document)
        {
           
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
                    var exists = await collection.Find(filter: new BsonDocument { { "_id", document.Key } }).FirstOrDefaultAsync();
                    if (exists == null && !string.IsNullOrEmpty( document.Version))//somebody else deleted the doc-> conflict
                    {
                        BatchItemResponse respWithError = BuildResponseWithError(document.Key, document.Version, "conflict");
                        response.ItemsWithErrors++;
                        response.BatchItemResponses.Add(respWithError);
                        continue;
                    }
                    var result = await collection.ReplaceOneAsync(filter: new BsonDocument { { "_id", document.Key }, { "_rev", BsonTypeMapper.MapToBsonValue(document.Version) } },
                                                                    options: new UpdateOptions { IsUpsert = true },
                                                                     replacement: newDoc);
                    BatchItemResponse itemResp = new BatchItemResponse
                    {
                        Key = document.Key,
                        Version = newDoc["_rev"].AsString
                    };
                    response.BatchItemResponses.Add(itemResp);
                    syncLogItem.KeyVersion.Add(itemResp.Key, itemResp.Version);

                }
                catch (MongoWriteException ex)
                {
                    string error = ex.Message;
                    if (ex.Message.Contains("duplicate key"))//conflict
                    {
                        error = "conflict";
                    }
                    var itemResp = BuildResponseWithError(document.Key, document.Version, error);
                    response.ItemsWithErrors++;
                    response.BatchItemResponses.Add(itemResp);
                }

            }
            foreach (var document in value.DeletedDocuments)
            {
                BatchItemResponse itemResp = new BatchItemResponse()
                {
                    Key = document.Key,
                    Version = document.Version
                };
                var del=await collection.DeleteOneAsync(filter: new BsonDocument { { "_id", document.Key }, { "_rev", BsonTypeMapper.MapToBsonValue(document.Version) } });
                if (del.DeletedCount == 0)
                {
                    var docFromDB= collection.Find(filter: new BsonDocument { { "_id", document.Key } }).FirstOrDefaultAsync();
                    if (docFromDB != null)
                    {
                        itemResp.Error = "conflict";
                        itemResp.ErrorDesc = "conflict";
                        response.ItemsWithErrors++;
                    }
                }
                response.BatchItemResponses.Add(itemResp);

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

        private BatchItemResponse BuildResponseWithError(string key, string version, string error)
        {
            BatchItemResponse itemResp = new BatchItemResponse()
            {
                Key = key,
                Version = version,
                Error = error,
                ErrorDesc = error
            };
            return itemResp;
        }

        private BsonValue GenerateNewVersion()
        {
            Random rnd = new Random();
            return new BsonString(rnd.Next().ToString());
        }
    }
}