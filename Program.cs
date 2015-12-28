using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StorageProcess.TableHelpers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using StorageProcess.BlobStorages;
using StorageProcess.Entity;
using StorageProcess.GetCloudStorageAccounts;
using StorageProcess.Logger;
using StorageProcess.TableStorage;
using Microsoft.WindowsAzure;

namespace StorageProcess
{
    class Program
    {
        #region
        public static int InsertDataCount = Convert.ToInt32(CloudConfigurationManager.GetSetting("InsertDataCount"));
        public static int RunInsertDataCount = Convert.ToInt32(CloudConfigurationManager.GetSetting("RunInsertDataCount"));
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

            List<CustomerEntity> insertEntityList = InsertDataCount.CreateCustomerEntity();

            string serializeListEntity = JsonConvert.SerializeObject(insertEntityList);

            string currentBlobName = BlobName + DateTime.Now.ToString("yyyyMMddhhmmss").ToString();
            await blobStorage.CreateBlockBlobString(currentBlobName, "application/json", serializeListEntity);

            string getJsonObject = await blobStorage.GetBlockBlobDataString(currentBlobName);
            List<CustomerEntity> getListEntity = JsonConvert.DeserializeObject<List<CustomerEntity>>(getJsonObject).ToList();

            if (getListEntity.Count == insertEntityList.Count &&
                insertEntityList.Count == GetSameEntities(getListEntity, insertEntityList).Count())
            {

                Log.Info("Get blob Data success, Count:{0}", getListEntity.Count);
                //blobStorage.DeleteContainer();
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
            //string currentTableName = "testtable20151225090911";

            tableStorage = new TableStorage<CustomerEntity>(storageAccount, currentTableName);

            List<CustomerEntity> insertEntityList = InsertDataCount.CreateCustomerEntity();

            await tableStorage.CreateEntities(insertEntityList);

            List<CustomerEntity> getTableResult = new List<CustomerEntity>();

            getTableResult = tableStorage.GetEntitiesByPartitionKey("Jonathan").Result.ToList();

            if (getTableResult.Count == insertEntityList.Count &&
                insertEntityList.Count == GetSameEntities(getTableResult, insertEntityList).Count())
            {
                Log.Info("GetEntitiesByPartitionKey from Table Success, Count:{0}", getTableResult.Count);
            }
            else
            {
                Log.Error("GetEntitiesByPartitionKey from Table Fail, Expect Count:{0}, Actual Count{1}, table name:{2}",
                    insertEntityList.Count, getTableResult.Count, currentTableName);
            }

            getTableResult = new List<CustomerEntity>();

            getTableResult = tableStorage.GetEntitiesByRowKeyAsync(
                InsertDataCount.StartRowKey(),
                InsertDataCount.EndRowKey()).Result.ToList();

            if (getTableResult.Count == takeCount &&
                takeCount == GetSameEntities(getTableResult, insertEntityList).Count())
            {
                Log.Info("GetEntitiesByRowKey from Table Success, Count:{0}", getTableResult.Count);
                //tableStorage.DeleteTable();
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

        /// <summary>
        /// Get the same entities 
        /// </summary>
        /// <param name="entities1">Entities 1</param>
        /// <param name="entities2">Entities 2</param>
        /// <returns></returns>
        private static IEnumerable<CustomerEntity> GetSameEntities(List<CustomerEntity> entities1, List<CustomerEntity> entities2)
        {
            IEnumerable<CustomerEntity> getSameEntity;
            getSameEntity = from a in entities1
                            join b in entities2 on a.RowKey
                            equals b.RowKey into result
                            from c in result
                            select c;

            return getSameEntity;
        }
    }
}


