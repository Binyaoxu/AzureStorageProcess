using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StorageProcess.BlobStorageInterface
{
    /// <summary>
    /// Interface for Blob Operations
    /// </summary>
    interface IBlobStorage
    {
        /// <summary>
        /// CreateBlockBlobString
        /// </summary>
        /// <param name="blobName">blobName</param>
        /// <param name="contentType">contentType</param>
        /// <param name="dataList">dataList</param>
        /// <returns>string</returns>
        Task<string> CreateBlockBlobString(string blobName, string contentType, string dataList);

        /// <summary>
        ///  GetBlockBlobDataString
        /// </summary>
        /// <param name="blobName">blobName</param>
        /// <returns>string</returns>
        Task<string> GetBlockBlobDataString(string blobName);

        /// <summary>
        /// Delete Container
        /// </summary>
        void DeleteContainer();

        /// <summary>
        /// ListBlobsInContainer
        /// </summary>
        /// <returns>IEnumerable<IListBlobItem></returns>
        IEnumerable<IListBlobItem> ListBlobsInContainer();
    }
}
