using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SiaqodbCloudService.Repository
{
    class RepositoryFactory
    {
        public static IRepository GetRepository()
        {
            
             return new MongoDB.MongoDBRepo();
            //return new CouchDB.CouchDBRepo();
        }
    }
}
