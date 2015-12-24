using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using StorageProcess.TableHelpers;
using StorageProcess.TableStorageInterface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StorageProcess.Logger;
using StorageProcess.Entity;

namespace StorageProcess.TableStorage
{
    /// <summary>
    /// This Class do Table related operations
    /// </summary>    
    public class TableStorage<T> : ITableStorage<T> where T : TableEntity, new()
    {
        public TimeSpan CreateEntityElapsedTime;
        public TimeSpan GetEntitiesByPartitionKeyElapsedTime;
        public TimeSpan GetEntitiesByRowKeyElapsedTime;

        private static CloudTable cloudTable = null;
        private static CloudTableClient tableClient = null;

        /// <summary>
        /// TableStorage
        /// </summary>
        /// <param name="storageAccount">storageAccount</param>
        public TableStorage(CloudStorageAccount storageAccount)
        {
            tableClient = storageAccount.CreateCloudTableClient();
        }

        /// <summary>
        /// Table Storage
        /// </summary>
        /// <param name="storageAccount">storageAccount</param>
        /// <param name="tableName">tableName</param>
        public TableStorage(CloudStorageAccount storageAccount, string tableName)
        {
            tableClient = storageAccount.CreateCloudTableClient();
            Log.Info("Create a Table if not exists");

            try
            {
                cloudTable = tableClient.GetTableReference(tableName);

                Log.Info("Create table if not exists :{0}", tableName);
                cloudTable.CreateIfNotExistsAsync().Wait();
            }
            catch (Exception e)
            {
                Log.Warn("Exception:{0}", e.ToString());
                throw;
            }
        }

        /// <summary>
        /// Create Entities
        /// </summary>
        /// <param name="entities">entities</param>
        /// <returns></returns>
        public async Task CreateEntities(IEnumerable<T> entities)
        {
            try
            {
                TableBatchOperation batchOperation = new TableBatchOperation();

                Log.Info("Create Entities");
                DateTime startTime = DateTime.Now;

                foreach (var chunk in entities.Chunk(100))
                {
                    batchOperation = new TableBatchOperation();
                    foreach (var entity in chunk)
                    {
                        batchOperation.Insert(entity);
                    }

                    await cloudTable.ExecuteBatchAsync(batchOperation);                                                                                                           

                }

                DateTime endTime = DateTime.Now;
                CreateEntityElapsedTime = endTime - startTime;
                Log.Info("Insert Entity to Table count: {0}, Elapsed time = {1}", entities.Count(), CreateEntityElapsedTime.ToString());
            }
            catch (Exception e)
            {
                Log.Warn("Exception:{0}", e.ToString());
                throw;
            }
        }

        /// <summary>
        /// Get Entities By Partition Key
        /// </summary>
        /// <param name="partitionKey">partitionKey</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetEntitiesByPartitionKey(string partitionKey)
        {
            var query =
               new TableQuery<T>()
                   .Where(TableQuery.GenerateFilterCondition(
                       "PartitionKey",
                       QueryComparisons.Equal,
                       partitionKey));

            Log.Info("Query Entities");
            DateTime startTime = DateTime.Now;

            IEnumerable<T> result = await cloudTable.ExecuteQueryAsync(query);

            DateTime endTime = DateTime.Now;
            GetEntitiesByPartitionKeyElapsedTime = endTime - startTime;
            Log.Info("GetEntitiesByPartitionKey from Table count: {0}, Elapsed time = {1}", result.Count(), GetEntitiesByPartitionKeyElapsedTime.ToString());

            return result;
        }

        /// <summary>
        /// Gets all entities from the table with the specified rowKey
        /// </summary>
        /// <param name="rowKey">
        /// The row key of the entities to be returned.
        /// </param>
        public async Task<IEnumerable<T>> GetEntitiesByRowKeyAsync(string startRowKey, string endRowKey)
        {
            TableQuery<T> rangeQuery = new TableQuery<T>().Where(
                       TableQuery.CombineFilters(
                       TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey),
                       TableOperators.And,
                       TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, endRowKey)));

            DateTime startTime = DateTime.Now;

            IEnumerable<T> result = await cloudTable.ExecuteQueryAsync(rangeQuery);
                                                                                  
            DateTime endTime = DateTime.Now;
            GetEntitiesByRowKeyElapsedTime = endTime - startTime;
            Log.Info("GetEntitiesByRowKey from Table count: {0}, Elapsed time = {1}", result.Count(), GetEntitiesByRowKeyElapsedTime.ToString());

            return result;
        }

        /// <summary>
        /// List all table.
        /// </summary>
        /// <returns>IEnumerable<CloudTable></returns>
        public IEnumerable<CloudTable> ListAllContainer()
        {
            IEnumerable<CloudTable> listContainer = tableClient.ListTables();

            foreach (CloudTable cloudTable in listContainer)
            {
                Console.WriteLine("Cloud Table:{0},URL:{1}", cloudTable.Name, cloudTable.Uri);
            }

            return listContainer;
        }

        /// <summary>
        /// Delete table
        /// </summary>
        /// <param name="cloudTable">Cloud type table</param>
        public void DeleteTable(CloudTable cloudTable)
        {
            Console.WriteLine("Delete Cloud Table:{0}", cloudTable.Name);
            cloudTable.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Delete table
        /// </summary>
        /// <param name="cloudTable">Cloud type table</param>
        public void DeleteTable()
        {
            if (cloudTable != null)
            {
                Log.Info("Delete Cloud Table:{0}", cloudTable.Name);
                cloudTable.DeleteAsync().Wait();
            }
            else
            {
                Log.Info("The Cloud Table not be instance");
            }
        }

    }
}
