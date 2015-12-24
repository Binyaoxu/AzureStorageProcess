using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using StorageProcess.BlobStorageInterface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StorageProcess.Logger;

namespace StorageProcess.BlobStorages
{
    /// <summary>
    /// Blob Container class that do the container operations.
    /// </summary>
    public class BlobStorage : IBlobStorage
    {
        private CloudBlobContainer container = null;
        public TimeSpan InsertBlobElapsedTime;
        public TimeSpan GetBlobElapsedTime;

        private CloudBlobClient blobClient = null;
        public BlobStorage(CloudStorageAccount storageAccount)
        {
            if (blobClient == null)
                blobClient = storageAccount.CreateCloudBlobClient();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="storageAccount"></param>
        /// <param name="containerName"></param>
        /// <param name="accessType"></param>
        public BlobStorage(CloudStorageAccount storageAccount, string containerName, BlobContainerPublicAccessType accessType)
        {
            blobClient = storageAccount.CreateCloudBlobClient();
            container = blobClient.GetContainerReference(containerName);

            try
            {
                Log.Info("Creating Blob if not exists:{0}", containerName);
                container.CreateIfNotExists();
                container.SetPermissions(new BlobContainerPermissions { PublicAccess = accessType });
            }
            catch (Exception e)
            {
                Log.Warn("Create Blob Exception:{0}", e.ToString());
                throw;
            }
        }

        /// <summary>
        ///  Create Block Blob String
        /// </summary>
        /// <param name="blobName">blob Name</param>
        /// <param name="contentType">content Type</param>
        /// <param name="dataList">string type json data List</param>
        /// <returns>string</returns>
        public async Task<string> CreateBlockBlobString(string blobName, string contentType, string dataList)
        {
            try
            {
                Log.Info("Upload Data to Container");

                DateTime startTime = DateTime.Now;
                CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(blobName);
                cloudBlockBlob.Properties.ContentType = contentType;

                cloudBlockBlob.UploadTextAsync(dataList).Wait();               

                DateTime endTime = DateTime.Now;
                InsertBlobElapsedTime = endTime - startTime;
                Log.Info("Insert Blob Name:{0}, Elapsed time = {1}", blobName, InsertBlobElapsedTime.ToString());

                return cloudBlockBlob.Uri.ToString();
            }
            catch (Exception e)
            {
                Log.Warn("CreateBlockBlobString Exception:{0}", e.ToString());
                throw;
            }
        }

        /// <summary>
        /// Get block blob Data String
        /// </summary>
        /// <param name="blobName">blob Name</param>
        /// <returns>string</returns>
        public async Task<string> GetBlockBlobDataString(string blobName)
        {
            try
            {
                Log.Info("Download Blobs in Container");
                string blobList;

                DateTime startTime = DateTime.Now;
                //StopWatch             
                CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
                blobList = await blob.DownloadTextAsync();
                                
                DateTime endTime = DateTime.Now;
                GetBlobElapsedTime = endTime - startTime;
                Log.Info("Get Blob: {0}, Elapsed time = {1}", blobName, GetBlobElapsedTime.ToString());

                return blobList;
            }
            catch (Exception e)
            {
                Log.Warn("GetBlockBlobDataString Exception:{0}", e.ToString());
                throw;
            }
        }

        /// <summary>
        ///  List Blobs In Container
        /// </summary>
        /// <returns></returns>
        public IEnumerable<IListBlobItem> ListBlobsInContainer()
        {
            Log.Info("List Blobs in Container");
            return container.ListBlobs().ToList();
        }

        /// <summary>
        /// List all container
        /// </summary>
        /// <returns>IEnumerable<CloudBlobContainer></returns>
        public IEnumerable<CloudBlobContainer> ListAllContainer()
        {
            IEnumerable<CloudBlobContainer> listContainer = blobClient.ListContainers();

            foreach (CloudBlobContainer container in listContainer)
            {
                Console.WriteLine("Container Name:{0},URL:{1}", container.Name, container.Uri);
            }

            return listContainer;
        }

        /// <summary>
        /// Delete the Container
        /// </summary>
        /// <param name="container">container</param>
        public void DeleteContainer(CloudBlobContainer container)
        {
            Console.WriteLine("Delete Container:{0}", container.Name);
            container.DeleteAsync().Wait();
        }

        /// <summary>
        /// Delete the container
        /// </summary>
        /// <param name="container">container type</param>
        public void DeleteContainer()
        {
            if (container != null)
            {
                Log.Info("Delete Container:{0}", container.Name);
                container.DeleteAsync().Wait();
            }
            else
            {
                Log.Info("The container not be instance");
            }
        }
    }
}
