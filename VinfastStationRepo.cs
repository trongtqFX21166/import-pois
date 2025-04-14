using DnsClient;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public class VinfastStationRepo:IVinfastStationRepo
    {
        private readonly ILogger<VinfastStationRepo> _logger;
        private readonly IMongoDbHelper _dbHelper;

        private readonly IMongoCollection<VinfastStationMapping> _mappingCol;
        private readonly IMongoCollection<VinfastChargerStation> _stationsCol;


        public VinfastStationRepo(ILogger<VinfastStationRepo> logger, IEnumerable<IMongoDbHelper> dbHelpers)
        {
            _logger = logger;
            _dbHelper = dbHelpers.FirstOrDefault(x => x.DatabaseName == RegisterVfStationDb.DatabaseName);



            _mappingCol = _dbHelper.GetCollection<VinfastStationMapping>("mapping_vfstations");
            _stationsCol = _dbHelper.GetCollection<VinfastChargerStation>("crawler_vfstation");

        }

        public async Task<List<string>> UpsertCrawlerData(IEnumerable<BsonDocument> rawStations)
        {
            var lsWrites = new List<WriteModel<BsonDocument>>();
            List<string> locationIds = new List<string>();
            foreach (var raw in rawStations)
            {
                try
                {
                    var locationId = raw.GetValue("locationId").AsString;
                    locationIds.Add(locationId);
                    var filterDf = Builders<BsonDocument>.Filter.Eq("locationId", locationId);

                    var _o = await _dbHelper.GetCollection<BsonDocument>("crawler_vfstation").Find(filterDf).FirstOrDefaultAsync();

                    if (_o == null)
                    {
                        await _dbHelper.GetCollection<BsonDocument>("crawler_vfstation").InsertOneAsync(raw);
                    }
                    else
                    {
                        await _dbHelper.GetCollection<BsonDocument>("crawler_vfstation").ReplaceOneAsync(filterDf, raw);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"UpsertCrawlerData::Fail::{ex.Message}::{raw.ToJson()}");
                }

            }

            return locationIds;
        }

        public async Task<VinfastChargerStation> GetVfStation(string locationId)
        {
            return await _stationsCol.Find(x => x.locationId == locationId).FirstOrDefaultAsync();
        }

        public async Task<VinfastStationMapping> GetMapping(string vmId)
        {
            return await _mappingCol.Find(x => x.VMLId == vmId).FirstOrDefaultAsync();
        }

        public async Task UpsertVfMappingData(VinfastStationMapping mapping)
        {
            var mappingdb = string.IsNullOrEmpty(mapping.LocationId) ? null : await _mappingCol.Find(x => x.Id == mapping.Id).FirstOrDefaultAsync();
            if (mappingdb == null)
            {
                mapping.Createddate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _mappingCol.InsertOneAsync(mapping);
            }
            else
            {
                mapping.Id = mappingdb.Id;
                mapping.Lastmodified = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _mappingCol.ReplaceOneAsync(x => x.Id == mapping.Id, mapping);
            }
        }

        public async Task<IEnumerable<VinfastStationMapping>> QueryMappingVfStations(int skip, int pageSize)
        {
            return await _mappingCol.Find(x => x.VMIsValid).SortBy(x => x.Createddate).Skip(skip*pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<VinfastStationMapping>> QueryInvalidMappingVfStations(int skip, int pageSize)
        {
            return await _mappingCol.Find(x => string.IsNullOrWhiteSpace(x.LocationId)).SortBy(x => x.Createddate).Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<VinfastChargerStation>> GetStationsToUpdate(long cutoffTimestamp, int skip, int take)
        {
            var filter = Builders<VinfastChargerStation>.Filter.Or(
                Builders<VinfastChargerStation>.Filter.Eq(x => x.LastSyncTimestamp, 0),
                Builders<VinfastChargerStation>.Filter.Lt(x => x.LastSyncTimestamp, cutoffTimestamp)
            );

            return await _stationsCol.Find(filter)
                .Skip(skip)
                .Limit(take)
                .ToListAsync();
        }

        public async Task UpdateVfStation(VinfastChargerStation station)
        {
            var filter = Builders<VinfastChargerStation>.Filter.Eq(x => x.locationId, station.locationId);
            var options = new ReplaceOptions { IsUpsert = true };
            await _stationsCol.ReplaceOneAsync(filter, station, options);
        }
    }
}
