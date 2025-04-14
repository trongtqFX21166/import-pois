using Redis.OM.Modeling;

namespace Platform.IOTHub.ImportPOI.Service.Cache
{
    [Document(StorageType = StorageType.Json, IndexName = "poi-idx", Prefixes = ["iothub-search-poi"])]
    public class PoiSearchCache
    {
        [RedisIdField]
        [Indexed]
        public string Id { get; set; }

        [Indexed]
        public string ParentId { get; set; }

        [Indexed]
        public string GGplaceId { get; set; }

        [Indexed(Sortable = true)]
        public GeoLoc Geom { get; set; }

        public string Name { get; set; }

        public string Address { get; set; }

        [Indexed(Sortable = true)]
        public int Click { get; set; }

        [Indexed]
        public List<string> CategoryCodes { get; set; }

        [Indexed]
        public List<string> ConvenienceCodes { get; set; }

        [Indexed]
        public List<string> BrandCodes { get; set; }

        [Searchable]
        public string OpeningHours { get; set; }


        public RatingCache? Rating { get; set; }

        public List<PoiCategoryCache> Categories { get; set; }
        public List<PoiCategoryCache> Conveniences { get; set; }
        public List<PoiCategoryCache> Brands { get; set; }
        public List<EvsePowerCache> EvsePowers { get; set; }

    }
}
