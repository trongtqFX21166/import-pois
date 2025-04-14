using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class VfStationResponse
    {
        public int code { get; set; }
        public string message { get; set; }
        public List<BsonDocument> data { get; set; }
    }
}
