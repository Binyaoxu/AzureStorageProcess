using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace StorageProcess.TableStorageInterface
{
    /// <summary>
    /// Interface for Table Storage Operations
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITableStorage<T> where T : TableEntity, new()
    {
        /// <summary>
        ///  CreateEntities
        /// </summary>
        /// <param name="entities">entities</param>
        /// <returns></returns>
        Task CreateEntities(IEnumerable<T> entities);

        /// <summary>
        /// GetEntitiesByPartitionKey
        /// </summary>
        /// <param name="partitionKey">partitionKey</param>
        /// <returns>IEnumerable<T></returns>
        Task<IEnumerable<T>> GetEntitiesByPartitionKey(string partitionKey);

        /// <summary>
        /// Delete Table
        /// </summary>
        void DeleteTable();
    }
}
