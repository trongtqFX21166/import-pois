using MongoDB.Driver;

namespace Platform.IOTHub.ImportPOI.Service.Cache
{
    public class PoiCategoryCache
    {
        public Guid Id { get; set; }

        public string Code { get; set; }

        public string Name { get; set; }

        public string ImageUrl { get; set; }

        public string MarkerUrl { get; set; }

        public string MarkerSelectedUrl { get; set; }

        public bool InActive { get; set; }
    }
}
