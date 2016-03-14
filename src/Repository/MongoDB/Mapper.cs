using MongoDB.Bson;
using SiaqodbCloudService.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SiaqodbCloudService.Repository.MongoDB
{
    static class Mapper
    {
        public static BsonDocument ToBsonDoc(SiaqodbDocument cobj)
        {
            var doc = new BsonDocument();
            doc["_id"] = cobj.Key;
            doc["_rev"] = BsonTypeMapper.MapToBsonValue(cobj.Version);
            doc["doc"] = cobj.Content;
            foreach (string tagName in cobj.Tags.Keys)
            {
                doc[tagName] = BsonTypeMapper.MapToBsonValue(cobj.Tags[tagName]);
            }
            return doc;
        }
        public static SiaqodbDocument ToSiaqodbDocument(BsonDocument doc)
        {
            string key = doc["_id"].AsString;
            string rev = null;
            if (doc.Names.Contains("_rev"))
            {
                rev = doc["_rev"].AsString;
            }
            byte[] content = doc["doc"].AsByteArray;
            Dictionary<string, object> tags = new Dictionary<string, object>();
            foreach (string name in doc.Names)
            {
                if (name != "_id" && name != "_rev" && name != "doc")
                {
                    tags.Add(name, BsonTypeMapper.MapToDotNetValue(doc[name]));
                }
            }
            return new SiaqodbDocument() { Key = key, Version = rev, Content = content, Tags =tags };
        }
    }
}