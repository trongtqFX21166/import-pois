using MongoDB.Bson;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class RunResult
    {
        public string run_id { get; set; }

        public string run_status { get; set; }

        public List<BsonDocument> data { get; set; }
    }
}
