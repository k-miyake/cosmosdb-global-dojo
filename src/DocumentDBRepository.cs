namespace todo
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    public static class DocumentDBRepository<T> where T : class
    {
        private static readonly string DatabaseId = ConfigurationManager.AppSettings["database"];
        private static readonly string CollectionId = ConfigurationManager.AppSettings["collection"];
        private static DocumentClient client;

        public static async Task<T> GetItemAsync(string id)
        {
            try
            {
                Document document = await client.ReadDocumentAsync(UriFactory.CreateDocumentUri(DatabaseId, CollectionId, id));
                return (T)(dynamic)document;
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null;
                }
                else
                {
                    throw;
                }
            }
        }

        public static async Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate)
        {
            IDocumentQuery<T> query = client.CreateDocumentQuery<T>(
                UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId),
                new FeedOptions { MaxItemCount = -1 })
                .Where(predicate)
                .AsDocumentQuery();

            List<T> results = new List<T>();
            while (query.HasMoreResults)
            {
                results.AddRange(await query.ExecuteNextAsync<T>());
            }

            return results;
        }

        public static async Task<Document> CreateItemAsync(T item)
        {
            return await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId), item);
        }

        public static async Task<Document> UpdateItemAsync(string id, T item)
        {
            return await client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(DatabaseId, CollectionId, id), item);
        }

        public static async Task DeleteItemAsync(string id)
        {
            await client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(DatabaseId, CollectionId, id));
        }

        public static string GetReadEndpoint()
        {
            var ep = client.ReadEndpoint.Host;
            return ep;
        }

        public static string GetWriteEndpoint()
        {
            var ep = client.WriteEndpoint.Host;
            return ep;
        }

        public static void Initialize()
        {
            // 接続ポリシーの作成
            ConnectionPolicy cp = new ConnectionPolicy {
                ConnectionMode = ConnectionMode.Direct,
#if RELEASE
                ConnectionProtocol = Protocol.Tcp
#endif
          
            };

            // このインスタンスのリージョンを追加
            cp.PreferredLocations.Add(ConfigurationManager.AppSettings["appRegion"]);

            // その他のリージョンを追加
            cp = AddPreferredLocations(cp).Result;

            // クライアントの生成
            client = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["endpoint"]), // エンドポイント
                ConfigurationManager.AppSettings["authKey"],
                cp
            );

            client.OpenAsync(); // パフォーマンス改善のため一度接続しておく

            CreateDatabaseIfNotExistsAsync().Wait();
            CreateCollectionIfNotExistsAsync().Wait();
        }

        // 利用可能リージョンを追加
        private static async Task<ConnectionPolicy> AddPreferredLocations(ConnectionPolicy cp)
        {
            var preClient = new DocumentClient(
                new Uri(ConfigurationManager.AppSettings["endpoint"]),
                ConfigurationManager.AppSettings["authKey"]
            );
            DatabaseAccount db = await preClient.GetDatabaseAccountAsync();
            var locations = db.ReadableLocations;

            foreach(var l in locations)
            {
                if(l.Name != ConfigurationManager.AppSettings["appRegion"])
                {
                    cp.PreferredLocations.Add(l.Name);
                }
            }
            preClient.Dispose();
            return cp;
        }

        private static async Task CreateDatabaseIfNotExistsAsync()
        {
            try
            {
                await client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseId));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    await client.CreateDatabaseAsync(new Database { Id = DatabaseId });
                }
                else
                {
                    throw;
                }
            }
        }

        private static async Task CreateCollectionIfNotExistsAsync()
        {
            try
            {
                await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    await client.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(DatabaseId),
                        new DocumentCollection { Id = CollectionId },
                        new RequestOptions { OfferThroughput = 1000 });
                }
                else
                {
                    throw;
                }
            }
        }
    }
}