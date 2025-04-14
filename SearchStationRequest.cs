using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class SearchStationRequest
    {
        public double longitude { get; set; }
        public double latitude { get; set; }
        public bool excludeFavorite { get; set; } = true;
    }
}
