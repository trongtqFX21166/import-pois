using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class VenueResponse
    {
        public string id { get; set; }

        public string name { get; set; }

        public string googlePlaceId { get; set; }

        public VenueResponseLatLng latLng { get; set; }

        public double distance { get; set; }
    }

    public class VenueResponseLatLng
    {
        public double lat { get; set; }

        public double lng { get; set; }
    }
}
