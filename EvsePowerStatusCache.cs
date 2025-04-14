using Redis.OM.Modeling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Cache
{
    [Document(StorageType = StorageType.Json, IndexName = "evse-power-idx", Prefixes = ["iothub-evse-power", "dev"])]
    public class EvsePowerStatusCache
    {

        [RedisIdField]
        [Indexed]
        public string Id { get; set; }

        [Indexed]
        public string LocationId { get; set; }
        
        [Indexed]
        public string VMLId { get; set; }

        [Indexed(Sortable = true)]
        public long ExpiredTime { get; set; }

        public int Type { get; set; }

        public int TotalEvse { get; set; }

        public int NumberOfAvailableEvse { get; set; }
    }
}
