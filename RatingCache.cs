namespace Platform.IOTHub.ImportPOI.Service.Cache
{
    public class RatingCache
    {
        public double AverageRating { get; set; }

        public int TotalReviews { get; set; }

        public string Source { get; set; }

        public string SourceUrl { get; set; }
    }
}
