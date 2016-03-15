using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SiaqodbCloudService.Models;
using MyCouch;
using SiaqodbCloudService.Repository.CouchDB;
using MyCouch.Requests;
using MyCouch.Responses;

namespace SiaqodbCloudService.Repository.CouchDB
{
    class CouchDBRepo : IRepository
    {
        private const string DbServerUrl = @"http://127.0.0.1:5984/";
        private const string AccessKeysBucket = "sys_accesskeys";
        private const string SyncLogBucket = "sys_synclog";

        public async Task<StoreResponse> Delete(string bucketName, string key, string version)
        {
            using (var client = new MyCouchClient(DbServerUrl, bucketName))
            {
                
                if (version == null)
                {
                    var response = await client.Documents.GetAsync(key);
                    if (response.IsSuccess)
                    {
                        if (response.Content != null)
                        {
                            CouchDBDocument doc = client.Serializer.Deserialize<CouchDBDocument>(response.Content);
                            version = doc._rev;
                        }

                    }
                    else if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        if (response.Reason == "no_db_file")
                            throw new BucketNotFoundException(bucketName);
                        else
                            throw new DocumentNotFoundException(key, version);
                    }
                    else throw new GenericCouchDBException(response.Reason, response.StatusCode);


                }
                var deletedResponse = await client.Documents.DeleteAsync(key, version);
                if (deletedResponse.IsSuccess)
                {
                    return new StoreResponse() { Key = deletedResponse.Id, Version = deletedResponse.Rev };
                }
                else if (deletedResponse.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    if (deletedResponse.Reason == "no_db_file")
                        throw new BucketNotFoundException(bucketName);
                    else
                        throw new DocumentNotFoundException(key, version);
                }
                else if (deletedResponse.StatusCode == System.Net.HttpStatusCode.BadRequest)
                {
                    throw new InvalidVersionFormatException();
                }
                else throw new GenericCouchDBException(deletedResponse.Reason, deletedResponse.StatusCode);


            }
        }

        public async Task<SiaqodbDocument> Get(string bucketName, string key, string version)
        {
            using (var client = new MyCouchClient(DbServerUrl , bucketName))
            {
                var startTime = DateTime.Now;
                var response = await client.Documents.GetAsync(key, version);
                if (response.IsSuccess)
                {
                    var size = response.Content == null ? 0 : response.Content.Length;
                   
                    if (size == 0) return null;
                    var doc = client.Serializer.Deserialize<CouchDBDocument>(response.Content);
                    return Mapper.ToSiaqodbDocument(doc);
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    if (response.Reason == "no_db_file")
                        throw new BucketNotFoundException(bucketName);
                    else
                        throw new DocumentNotFoundException(key, version);
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.BadRequest)
                {
                    throw new InvalidVersionFormatException();
                }
                else throw new GenericCouchDBException(response.Reason, response.StatusCode);
            }
        }

        public async Task<BatchSet> GetAllChanges(string bucketName, int limit, string anchor,string uploadAnchor)
        {
            return await this.GetChanges(bucketName, null, limit, anchor, uploadAnchor);
        }

        private async Task<SyncLogItem> GetSyncLogItem(string uploadAnchor)
        {
            if (!string.IsNullOrEmpty(uploadAnchor))
            {
                using (var clientLog = new MyCouchClient(DbServerUrl, SyncLogBucket))
                {
                    var item = await clientLog.Documents.GetAsync(uploadAnchor);
                    if (item.IsSuccess)
                    {
                        return Newtonsoft.Json.JsonConvert.DeserializeObject<SyncLogItem>(item.Content);
                    }
                }
            }
            return null;
        }

        private static void CheckBucketNotFound(string bucketName, Response response)
        {
            if (response.StatusCode == System.Net.HttpStatusCode.NotFound && response.Reason == "no_db_file")
            {
                throw new BucketNotFoundException(bucketName);
            }
            else throw new GenericCouchDBException(response.Reason, response.StatusCode);
        }

        public async Task<BatchSet> GetChanges(string bucketName, Filter query, int limit, string anchor,string uploadAnchor)
        {
            using (var client = new MyCouchClient(DbServerUrl, bucketName))
            {
                
                GetChangesRequest changesReq = new GetChangesRequest();
                changesReq.Since = anchor;
                changesReq.Limit = limit;
                changesReq.IncludeDocs = true;
                var response = await client.Changes.GetAsync(changesReq);
                if (response.IsSuccess)
                {
                    BatchSet changeSet = new BatchSet();
                    if (response.Results != null)
                    {
                        SyncLogItem logItem = await this.GetSyncLogItem(uploadAnchor);

                        foreach (var row in response.Results)
                        {

                            if (row.Deleted)
                            {
                                this.AddDeletedDoc(changeSet, row, logItem);
                            }
                            else
                            {
                                this.AddChangedDoc(changeSet, row, logItem, query,client.Serializer );
                            }
                        }
                        changeSet.Anchor = response.LastSeq;
                    }

                    return changeSet;
                }
                else CheckBucketNotFound(bucketName, response);
                return null;

            }
        
        }

        private void AddChangedDoc(BatchSet changeSet, ChangesResponse<string>.Row row, SyncLogItem logItem, Filter query, MyCouch.Serialization.ISerializer serializer)
        {
            if (changeSet.ChangedDocuments == null)
                changeSet.ChangedDocuments = new List<SiaqodbDocument>();
            CouchDBDocument co = serializer.Deserialize<CouchDBDocument>(row.IncludedDoc);
            if (co._id.StartsWith("_design/"))
                return;
            //check uploaded anchor- means cliet just uploaded this record and we should not return back
            if (logItem != null && logItem.KeyVersion != null && logItem.KeyVersion.ContainsKey(co._id) && logItem.KeyVersion[co._id] == co._rev)
                return;
            if (OutOfFilter(query, co))
                return;
            changeSet.ChangedDocuments.Add(Mapper.ToSiaqodbDocument(co));
        }

        private bool OutOfFilter(Filter query, CouchDBDocument co)
        {
            if (query == null)
                return false;
            string tagName = query.TagName;
            if (query.TagName == "key")
            {
                tagName = "_id";
            }
            else if (!co.tags.Keys.Contains(tagName))
            {
                return true;
            }
            IComparable docValue = null;
            if (query.TagName == "key")
            {
                docValue = co._id;
            }
            else
            {
                docValue = co.tags[tagName] as IComparable;
            }
            
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

        private void AddDeletedDoc(BatchSet changeSet, ChangesResponse<string>.Row row, SyncLogItem logItem)
        {
            if (changeSet.DeletedDocuments == null)
                changeSet.DeletedDocuments = new List<DeletedDocument>();
            DeletedDocument delObj = new DeletedDocument() { Key = row.Id, Version = row.Changes[0].Rev };
            //check uploaded anchor- means cliet just uploaded this record and we should not return back
            if (logItem != null && logItem.KeyVersion != null && logItem.KeyVersion.ContainsKey(delObj.Key) && logItem.KeyVersion[delObj.Key] == delObj.Version)
                return;
            changeSet.DeletedDocuments.Add(delObj);
        }

        public async Task<string> GetSecretAccessKey(string accessKeyId)
        {
            using (var client = new MyCouchClient(DbServerUrl, AccessKeysBucket))
            {
                var response = await client.Documents.GetAsync(accessKeyId);
                if (response.IsSuccess)
                {
                    var size = response.Content == null ? 0 : response.Content.Length;

                    if (size == 0) return null;
                    var doc = client.Serializer.Deserialize<AccessKey>(response.Content);
                    return doc.secretkey;
                }
                
            }
            return null;
        }

        public async Task<StoreResponse> Store(string bucketName, SiaqodbDocument document)
        {
            using (var client = new MyCouchClient(DbServerUrl, bucketName))
            {
                
                await CheckTagsViews(client, bucketName, document.Tags);
                CouchDBDocument doc = Mapper.ToCouchDBDoc(document);
                var serializedObj = client.Serializer.Serialize<CouchDBDocument>(doc);

                var response = await client.Documents.PostAsync(serializedObj);
                if (response.IsSuccess)
                {
                    var cnorResponse = new StoreResponse();
                    cnorResponse.Version = response.Rev;
                    cnorResponse.Key = response.Id;

                    await this.StartRebuildViews(client, document);
                    
                    return cnorResponse;
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    throw new BucketNotFoundException(bucketName);
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    throw new ConflictException(response.Reason);
                }
                else throw new GenericCouchDBException(response.Reason, response.StatusCode);
            }
        }

        public async Task<BatchResponse> Store(string bucketName, BatchSet batch)
        {
            using (var client = new MyCouchClient(DbServerUrl, bucketName))
            {

                var dbExists= await client.Database.HeadAsync();
                if (dbExists.IsSuccess)
                {
                    BulkRequest bulkRequest = new BulkRequest();

                    DateTime start = DateTime.Now;
                    int size = 0;
                    SiaqodbDocument crObjForUpdateViews = null;
                    if (batch.ChangedDocuments != null)
                    {
                        foreach (SiaqodbDocument obj in batch.ChangedDocuments)
                        {
                            if (obj != null)
                            {
                                if (crObjForUpdateViews == null)
                                    crObjForUpdateViews = obj;

                                await CheckTagsViews(client, bucketName, obj.Tags);
                                CouchDBDocument doc = Mapper.ToCouchDBDoc(obj);
                                var serializedObject = client.Serializer.Serialize<CouchDBDocument>(doc);
                                bulkRequest.Include(serializedObject);
                                size += serializedObject.Length;
                            }
                        }
                    }
                    if (batch.DeletedDocuments != null)
                    {
                        foreach (DeletedDocument obj in batch.DeletedDocuments)
                        {
                            if (obj != null)
                            {
                                if (obj.Version != null)//otherwise means is a non-existing object
                                {
                                    bulkRequest.Delete(obj.Key, obj.Version);
                                }
                            }

                        }
                    }
                    var response = await client.Documents.BulkAsync(bulkRequest);
                    if (response.IsSuccess)
                    {
                        var cnorResponse = new BatchResponse();
                        if (response.Rows != null)
                        {
                            cnorResponse.BatchItemResponses = new List<BatchItemResponse>();
                            SyncLogItem syncLogItem = new SyncLogItem();
                            syncLogItem.KeyVersion = new Dictionary<string, string>();
                            foreach (var row in response.Rows)
                            {
                                BatchItemResponse wresp = new BatchItemResponse();
                                if (!string.IsNullOrEmpty(row.Error))
                                {
                                    cnorResponse.ItemsWithErrors++;
                                }
                                wresp.Error = row.Error;
                                wresp.ErrorDesc = row.Reason;
                                wresp.Key = row.Id;
                                wresp.Version = row.Rev;
                                cnorResponse.BatchItemResponses.Add(wresp);
                                if (string.IsNullOrEmpty(row.Error))
                                {
                                    syncLogItem.KeyVersion.Add(row.Id, row.Rev);
                                }
                            }
                            if (syncLogItem.KeyVersion.Count > 0)
                            {
                                syncLogItem.TimeInserted = DateTime.UtcNow;
                                using (var clientLog = new MyCouchClient(DbServerUrl, SyncLogBucket))
                                {
                                    string serLogItem = Newtonsoft.Json.JsonConvert.SerializeObject(syncLogItem);
                                    var logResp = await clientLog.Documents.PostAsync(serLogItem);
                                    cnorResponse.UploadAnchor = logResp.Id;
                                }
                            }
                        }
                        if (crObjForUpdateViews != null)
                        {
                            await this.StartRebuildViews(client, crObjForUpdateViews);
                        }

                        return cnorResponse;
                    }
                    else CheckBucketNotFound(bucketName, response);
                }
                else if (dbExists.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    throw new BucketNotFoundException(bucketName);
                }
                return null;

            }
        }
        private async Task StartRebuildViews(MyCouchClient client, SiaqodbDocument crObjForUpdateViews)
        {
            //force building the index at Store time avoiding to wait on Query time
            if (crObjForUpdateViews.Tags != null)
            {
                foreach (string tagName in crObjForUpdateViews.Tags.Keys)
                {
                    string viewName = "tags_" + tagName;
                    QueryViewRequest query = new QueryViewRequest(viewName, viewName);
                    query.Stale = Stale.UpdateAfter;
                    query.Limit = 1;
                    var res = await client.Views.QueryAsync(query);

                }
            }
        }
        private async Task CheckTagsViews(MyCouchClient client, string bucketName, Dictionary<string, object> tags)
        {
            if (tags != null && tags.Count > 0)
            {
                HashSet<string> viewsCache = new HashSet<string>();


                QueryViewRequest query = new QueryViewRequest("_all_docs");
                query.StartKey = "_design/";
                query.EndKey = "_design0";

                var all =await client.Views.QueryAsync(query);
                if (!all.IsSuccess)
                    CheckBucketNotFound(bucketName, all);
                if (all.Rows != null)
                {
                    foreach (var row in all.Rows)
                    {
                        string viewName = row.Key.ToString().Replace("_design/", "");
                        viewsCache.Add(viewName);

                    }
                }


                foreach (string tagName in tags.Keys)
                {
                    string viewName = "tags_" + tagName;
                    if (!viewsCache.Contains(viewName) && tags[tagName] != null)
                    {

                        string viewJSON = @"{""_id"":""_design/" + viewName + @""",""language"":""javascript"",""views"":{""" + viewName + @""":{""map"":
                            ""function(doc) {if(doc.tags." + tagName + @"!=null)emit(doc.tags." + tagName + @", null);}""}}}";
                        var getJson = await client.Documents.PostAsync(viewJSON);
                        if (!getJson.IsSuccess)
                            CheckBucketNotFound(bucketName, getJson);
                        viewsCache.Add(viewName);



                    }
                }

            }


        }
    }
}
