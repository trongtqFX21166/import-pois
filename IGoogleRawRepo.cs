using MongoDB.Bson;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public interface IGoogleRawRepo
    {
        Task<IEnumerable<RawGoogle>> GetCrawlerGoogles(string[] placeIds);
        Task<IEnumerable<RawGoogle>> GetCrawlerGooglesBySearchText(string[] searchTexts);
        Task UpSertRawThirdPartyData(RawThirdPartyData poi);
        Task<List<string>> UpsertCrawGoogleData(IEnumerable<BsonDocument> rawGGPlaces);

        Task<PlaceMappingTracking> GetMappingTracking(long vmId);
        Task<IEnumerable<PlaceMappingTracking>> GetMappingTrackings(IEnumerable<long> vmIds);
        Task UpsertMappingTracking(PlaceMappingTracking tracking);

        Task<int> GetTotalInvalidMapping();
        Task<IEnumerable<PlaceMappingTracking>> GetInvalidMappings(int skip, int pageSize);
        Task<IEnumerable<PlaceMappingTracking>> GetMappingTrackings(IEnumerable<string> searchTexts);

        Task<int> GetTotalReGGMapping();
        Task<IEnumerable<PlaceMappingTracking>> GetReGGMappings(int skip, int pageSize);


        Task UpsertWazeMappingTracking(WazeMappingTracking tracking);

        Task<int> GetTotalInvalidWazeMapping();
        Task<IEnumerable<WazeMappingTracking>> GetInvalidWazeMappings(int skip, int pageSize);
        Task<IEnumerable<WazeMappingTracking>> GetWazeMappingTrackings(IEnumerable<string> searchTexts);
        Task<IEnumerable<WazeMappingTracking>> GetWazeMappingTrackingsByPlaceIds(IEnumerable<string> placeIds);

    }
}
