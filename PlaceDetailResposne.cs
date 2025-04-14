using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service.Models.PeakApi
{
    public class PlaceDetailResposne
    {
        public Dictionary<string, GooglePlace> data { get; set; }
    }

    public class GooglePlace
    {
        public string PlaceId { get; set; }
        public Rating Rating { get; set; }
        public int SequenceNumber { get; set; }
    }

    public class Rating
    {
        public int TotalRatings { get; set; }
        public double RatingValue { get; set; }
    }
}
