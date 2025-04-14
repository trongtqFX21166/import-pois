using NetTopologySuite.Geometries.Prepared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class PeakApiEventMsg
    {
        public string eventType { get; set; }

        public PeakApiEventData eventData { get; set; }
    }

    public class PeakApiEventData
    {
        public string actorId { get; set; }

        public string actorTaskId { get; set; }

        public string actorRunId { get; set; }
    }
}
