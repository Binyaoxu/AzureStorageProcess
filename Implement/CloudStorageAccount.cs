using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using StorageProcess.Logger;
using System;

namespace StorageProcess.GetCloudStorageAccounts
{
    /// <summary>
    /// This class create Cloud storage account
    /// </summary>
    public class GetCloudStorageAccount
    {
        private static CloudStorageAccount storageAccount = null;

        private GetCloudStorageAccount()
        {
        }

        /// <summary>
        /// Create the storage account
        /// </summary>
        /// <returns>Cloud Storage Account</returns>
        public static CloudStorageAccount CreateStorageAccount()
        {
            try
            {
                if (storageAccount == null)
                {
                    storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
                }

                return storageAccount;

            }
            catch (FormatException e)
            {
                Log.Warn("Invalid storage account FormatException:{0}", e.ToString());                
                Console.ReadLine();
                throw;
            }
            catch (ArgumentException e)
            {
                Log.Warn("Invalid storage account ArgumentException:{0}", e.ToString());
                Console.ReadLine();
                throw;
            }
        }
    }
}

