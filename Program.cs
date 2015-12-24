using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using StorageProcess.BlobStorages;
using StorageProcess.Entity;
using StorageProcess.GetCloudStorageAccounts;
using StorageProcess.Logger;
using StorageProcess.TableStorage;


namespace StorageProcess
{
    class Program
    {
        #region
        public static int InsertDataCount = Convert.ToInt32(ConfigurationSettings.AppSettings.Get("InsertDataCount"));
        public static int RunInsertDataCount = Convert.ToInt32(ConfigurationSettings.AppSettings.Get("RunInsertDataCount"));        
        public static int takeCount = InsertDataCount / 10;

        private static string containerName = "testcontainer";
        const string BlobName = "testblob";
        const string TableName = "testtable";

        private static BlobStorage blobStorage;
        private static TableStorage<CustomerEntity> tableStorage;

        #endregion

        static void Main(string[] args)
        {
            List<TimeSpan> createJsonBlobTimeSpan = new List<TimeSpan>();
            List<TimeSpan> GetJsonBlobTimeSpan = new List<TimeSpan>();
            for (int i = 1; i <= RunInsertDataCount; i++)
            {
                Log.Info("\nThe {0}th time run the CloudBlockBlob Insert and Query!", i);
                CreateAndGetJsonBlob().Wait();
                createJsonBlobTimeSpan.Add(blobStorage.InsertBlobElapsedTime);
                GetJsonBlobTimeSpan.Add(blobStorage.GetBlobElapsedTime);
            }

            List<TimeSpan> createTableTimeSpan = new List<TimeSpan>();
            List<TimeSpan> fullQueryTimeSpan = new List<TimeSpan>();
            List<TimeSpan> rangeQueryTimeSpan = new List<TimeSpan>();
            for (int i = 1; i <= RunInsertDataCount; i++)
            {
                Log.Info("\nThe {0}th time run the CloudTable Insert and Query!", i);
                CreateAndGetTable().Wait();
                createTableTimeSpan.Add(tableStorage.CreateEntityElapsedTime);
                fullQueryTimeSpan.Add(tableStorage.GetEntitiesByPartitionKeyElapsedTime);
                rangeQueryTimeSpan.Add(tableStorage.GetEntitiesByRowKeyElapsedTime);
            }

            Log.Info("--------------------------------------------------------");
            Console.WriteLine("Insert Data to CloudBlockBlob and CloudTable Average Time List: ");
            Console.WriteLine("StorageType   RunCount    InsertDataCount      AverageTime");
            Console.WriteLine("{0}   {1}     {2}     {3}", "CloudBlockBlob", RunInsertDataCount, InsertDataCount, GetTimeSpanAverage(createJsonBlobTimeSpan));
            Console.WriteLine("{0}   {1}     {2}     {3}", "CloudTable", RunInsertDataCount, InsertDataCount, GetTimeSpanAverage(createTableTimeSpan));

            Console.WriteLine("\nQuery Data from CloudBlockBlob and CloudTable Average Time List: ");
            Console.WriteLine("StorageType   RunCount    QueryDataCount      AverageTime");
            Console.WriteLine("{0}   {1}     {2}     {3}", "CloudBlockBlob", RunInsertDataCount, InsertDataCount, GetTimeSpanAverage(GetJsonBlobTimeSpan));
            Console.WriteLine("{0}   {1}     {2}     {3}", "TableFullQuery", RunInsertDataCount, InsertDataCount, GetTimeSpanAverage(fullQueryTimeSpan));
            Console.WriteLine("{0}   {1}     {2}     {3}", "TableRangeQuery", RunInsertDataCount, takeCount, GetTimeSpanAverage(rangeQueryTimeSpan));

            Log.Info("---------------------------------------------------------");

            Console.ReadLine();
        }

        /// <summary>
        /// Create and Get Json Blob
        /// </summary>
        /// <returns></returns>
        public static async Task CreateAndGetJsonBlob()
        {
            CloudStorageAccount storageAccount = GetCloudStorageAccount.CreateStorageAccount();
            string TestContainerName = containerName + DateTime.Now.ToString("yyyyMMddhhmmss").ToString();
            blobStorage = new BlobStorage(storageAccount, TestContainerName, BlobContainerPublicAccessType.Blob);

            List<CustomerEntity> insertEntityList = CustomerEntityUtility.CreateCustomerEntity(InsertDataCount);

            string serializeListEntity = JsonConvert.SerializeObject(insertEntityList);
            
            string currentBlobName = BlobName + DateTime.Now.ToString("yyyyMMddhhmmss").ToString();
            await blobStorage.CreateBlockBlobString(currentBlobName, "application/json", serializeListEntity);

            string getJsonObject = await blobStorage.GetBlockBlobDataString(currentBlobName);
            List<CustomerEntity> getListEntity = JsonConvert.DeserializeObject<List<CustomerEntity>>(getJsonObject).ToList();
            var getSameEntity = from a in getListEntity join b in insertEntityList on a.RowKey equals b.RowKey select new { a, b };

            if (getListEntity.Count == insertEntityList.Count && insertEntityList.Count == getSameEntity.Count())
            {

                Log.Info("Get blob Data success, Count:{0}", getListEntity.Count);
                blobStorage.DeleteContainer();
            }
            else
            {
                Log.Error("Get blob Data Fail, Expect Count:{0}, Actual Count{1}, blob Container name:{2}",
                   insertEntityList.Count, getListEntity.Count, TestContainerName);
            }
        }

        /// <summary>
        /// Create and Get Table
        /// </summary>
        /// <returns></returns>
        public static async Task CreateAndGetTable()
        {
            CloudStorageAccount storageAccount = GetCloudStorageAccount.CreateStorageAccount();
            string currentTableName = TableName + DateTime.Now.ToString("yyyyMMddhhmmss").ToString();
            tableStorage = new TableStorage<CustomerEntity>(storageAccount, currentTableName);

            List<CustomerEntity> insertEntityList = CustomerEntityUtility.CreateCustomerEntity(InsertDataCount);

            await tableStorage.CreateEntities(insertEntityList);

            List<CustomerEntity> getTableResult = new List<CustomerEntity>();

            getTableResult = tableStorage.GetEntitiesByPartitionKey("Jonathan").Result.ToList();
            var getSameEntity = from a in getTableResult join b in insertEntityList on a.RowKey equals b.RowKey select new { a, b };

            if (getTableResult.Count == insertEntityList.Count && insertEntityList.Count == getSameEntity.Count())
            {
                Log.Info("GetEntitiesByPartitionKey from Table Success, Count:{0}", getTableResult.Count);
            }
            else
            {
                Log.Error("GetEntitiesByPartitionKey from Table Fail, Expect Count:{0}, Actual Count{1}, table name:{2}",
                    insertEntityList.Count, getTableResult.Count, currentTableName);
            }

            getTableResult = new List<CustomerEntity>();
            string startRowKey = InsertDataCount <= 10000 ? "0" + (insertEntityList.Count / 2).ToString() : (insertEntityList.Count / 2).ToString();
            string endRowkey = InsertDataCount <= 10000 ? "0" + (insertEntityList.Count / 2 + takeCount).ToString() : (insertEntityList.Count / 2 + takeCount).ToString();

            getTableResult = tableStorage.GetEntitiesByRowKeyAsync(startRowKey, endRowkey).Result.ToList();
            getSameEntity = from a in getTableResult join b in insertEntityList on a.RowKey equals b.RowKey select new { a, b };

            if (getTableResult.Count == takeCount && takeCount == getSameEntity.Count())
            {
                Log.Info("GetEntitiesByRowKey from Table Success, Count:{0}", getTableResult.Count);
                tableStorage.DeleteTable();
            }
            else
            {
                Log.Error("GetEntitiesByRowKey from Table Fail, Expect Count:{0}, Actual Count:{1}, table name:{2}",
                    insertEntityList.Count, getTableResult.Count, currentTableName);
            }
        }

        /// <summary>
        /// Get the average time span
        /// </summary>
        /// <param name="sourceList">time source list</param>
        /// <returns>time span</returns>
        private static TimeSpan? GetTimeSpanAverage(List<TimeSpan> sourceList)
        {
            return TimeSpan.FromMilliseconds(sourceList.Average(i => i.TotalMilliseconds));
        }
    }
}


