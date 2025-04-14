using Redis.OM.Modeling;

namespace Platform.IOTHub.ImportPOI.Service.Cache
{
    [Document(StorageType = StorageType.Json, IndexName = "test-entry-poi-idx", Prefixes = ["test-iothub-entry-poi"])]
    public class EntryPoiSearchCache
    {
        [RedisIdField]
        [Indexed]
        public string Id { get; set; }

        [Indexed]
        public string ParentId { get; set; }

        [Indexed]
        public string GGplaceId { get; set; }

        [Indexed]
        public string GGParentPlaceId { get; set; }

        [Searchable]
        public string SearchText { get; set; }

        [Indexed]
        public GeoLoc Geom { get; set; }

        [Searchable(Weight = 2.0)]
        public string Name { get; set; }

        [Indexed]
        public string Address { get; set; }

        [Indexed]
        public int Click { get; set; }

    }
}
