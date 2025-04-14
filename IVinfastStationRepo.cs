using MongoDB.Bson;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public interface IVinfastStationRepo
    {
        Task<VinfastChargerStation> GetVfStation(string locationId);
        Task<List<string>> UpsertCrawlerData(IEnumerable<BsonDocument> rawStations);
        
        Task<VinfastStationMapping> GetMapping(string vmId);
        Task UpsertVfMappingData(VinfastStationMapping mapping);
        Task<IEnumerable<VinfastStationMapping>> QueryMappingVfStations(int skip, int pageSize);
        Task<IEnumerable<VinfastStationMapping>> QueryInvalidMappingVfStations(int skip, int pageSize);

        Task<IEnumerable<VinfastChargerStation>> GetStationsToUpdate(long cutoffTimestamp, int skip, int take);
        Task UpdateVfStation(VinfastChargerStation station);
    }
}
