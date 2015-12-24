using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using StorageProcess.BlobStorages;
using StorageProcess.Entity;
using StorageProcess.GetCloudStorageAccounts;
using StorageProcess.TableStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageProcess
{
   public class DropTestData
    {
        public static void  DropBlobContainer(string blobContainerPrefix)
        {
            CloudStorageAccount storageAccount = GetCloudStorageAccount.CreateStorageAccount();
            BlobStorage blobStorage = new BlobStorage(storageAccount);
            IEnumerable<CloudBlobContainer> containerList = blobStorage.ListAllContainer();
            foreach (var container in containerList)
            {
                if (container.Name.Contains(blobContainerPrefix))
                {
                    blobStorage.DeleteContainer(container);
                    Console.WriteLine("Delete the container success:{0}", container.Name);
                }
            }
        }

        public static void DropTable(string tablePrefix)
        {
            CloudStorageAccount storageAccount = GetCloudStorageAccount.CreateStorageAccount();
            TableStorage<TableEntity> tableStorage = new TableStorage<TableEntity>(storageAccount);
            IEnumerable<CloudTable> tableList = tableStorage.ListAllContainer();
            foreach (var table in tableList)
            {
                if (table.Name.Contains(tablePrefix))
                {
                    tableStorage.DeleteTable(table);
                    Console.WriteLine("Delete the table success:{0}", table.Name);
                }
            }
        }
    }
}
