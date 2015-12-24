using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageProcess.Entity
{
    /// <summary>
    /// Customer Entity
    /// </summary>
    public class CustomerEntity : TableEntity
    {
        public CustomerEntity() { }

        public CustomerEntity(string name, string rowKey)
        {
            this.PartitionKey = name;
            this.RowKey = rowKey;
        }

        public string PhoneNumber { get; set; }
        public string Sex { get; set; }
        public string Company { get; set; }
    }

    public static class CustomerEntityUtility
    {
        public static List<CustomerEntity> CreateCustomerEntity(int insertDataCount)
        {
            List<CustomerEntity> insertEntityList = new List<CustomerEntity>();
             
            for (int i = 0; i < insertDataCount; i++)
            {
                CustomerEntity entity = new CustomerEntity("Jonathan", string.Format("{0}", i.ToString("D5")))
                {
                    PhoneNumber = "123-456-" + string.Format("{0}", i.ToString("D5")),
                    Sex = i % 2 == 0 ? "Male" : "Famale",
                    Company = "Shanghai Minhang Dongchuan Road Number " + string.Format("{0}", i.ToString("D5")),
                };

                
                insertEntityList.Add(entity);
            }

            return insertEntityList;
        }
    }
}
