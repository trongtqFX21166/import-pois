using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Repository.VMPOIRaw.Models;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public class VMPoiRawRepository : IVMPoiRawRepository
    {
        private readonly ILogger<VMPoiRawRepository> _logger;
        private readonly IMongoDbHelper _dbHelper;

        private readonly IMongoCollection<RawBranch> _branchCol;
        private readonly IMongoCollection<RawCategory> _categoryCol;
        private readonly IMongoCollection<RawBrand> _brandCol;
        private readonly IMongoCollection<RawPOI> _poiCol;
        private readonly IMongoCollection<RawEntryPOI> _entrypoiCol;
        private readonly IMongoCollection<RawWorkingHour> _workinghoursCol;

        private readonly IMongoCollection<RawAdminCountry> _countryCol;
        private readonly IMongoCollection<RawAdminCity> _cityCol;
        private readonly IMongoCollection<RawAdminDistrict> _districtCol;
        private readonly IMongoCollection<RawAdminWard> _wardCol;

        private readonly IMongoCollection<ImportSummary> _importSummaryCol;
        

        public VMPoiRawRepository(ILogger<VMPoiRawRepository> logger, IEnumerable<IMongoDbHelper> dbHelpers)
        {
            _logger = logger;
            _dbHelper = dbHelpers.FirstOrDefault(x => x.DatabaseName == Register.DatabaseName);

            _branchCol = _dbHelper.GetCollection<RawBranch>("branchs");
            _brandCol = _dbHelper.GetCollection<RawBrand>("brands");
            _categoryCol = _dbHelper.GetCollection<RawCategory>("categories");
            _poiCol = _dbHelper.GetCollection<RawPOI>("pois");
            _workinghoursCol = _dbHelper.GetCollection<RawWorkingHour>("workhours");
            _entrypoiCol = _dbHelper.GetCollection<RawEntryPOI>("entry_pois");

            _countryCol = _dbHelper.GetCollection<RawAdminCountry>("country");
            _cityCol = _dbHelper.GetCollection<RawAdminCity>("city");
            _districtCol = _dbHelper.GetCollection<RawAdminDistrict>("district");
            _wardCol = _dbHelper.GetCollection<RawAdminWard>("ward");

            _importSummaryCol = _dbHelper.GetCollection<ImportSummary>("import_summary");
            
        }


        public async Task<IEnumerable<RawCategory>> GetCategories()
        {
            return await _categoryCol.Find(x => true).ToListAsync();   
        }

        public async Task UpdateCategoyImportStatus(IEnumerable<ImportStatus> importStatuses)
        {
            //todo:
            await Task.CompletedTask;
        }

        public async Task<IEnumerable<RawBrand>> GetBrands()
        {
            return await _brandCol.Find(x => true).ToListAsync();
        }

        public async Task UpdateChainImportStatus(IEnumerable<ImportStatus> importStatuses)
        {
            var bulkOps = new List<WriteModel<RawBrand>>();

            foreach (var importStatus in importStatuses)
            {
                var filter = Builders<RawBrand>.Filter.Eq(x => x.Id, importStatus.Id);
                var update = Builders<RawBrand>.Update.Set(x => x.ImportStatus, importStatus);
                var updateOne = new UpdateOneModel<RawBrand>(filter, update);
                bulkOps.Add(updateOne);
            }

            if (bulkOps.Any())
            {
                await _brandCol.BulkWriteAsync(bulkOps);
            }
        }


        public async Task<IEnumerable<RawBranch>> GetBranchs()
        {
            return await _branchCol.Find(x => true).ToListAsync();
        }

        public async Task UpdateBranchImportStatus(IEnumerable<ImportStatus> importStatuses)
        {
            var bulkOps = new List<WriteModel<RawBranch>>();

            foreach (var importStatus in importStatuses)
            {
                var filter = Builders<RawBranch>.Filter.Eq(x => x.Id, importStatus.Id);
                var update = Builders<RawBranch>.Update.Set(x => x.ImportStatus, importStatus);
                var updateOne = new UpdateOneModel<RawBranch>(filter, update);
                bulkOps.Add(updateOne);
            }

            if (bulkOps.Any())
            {
                await _branchCol.BulkWriteAsync(bulkOps);
            }
        }

        public async Task<IEnumerable<RawWorkingHour>> GetWorkingHours()
        {
            return await _workinghoursCol.Find(x => true).ToListAsync();
        }

        public async Task UpdateWorkingHourImportStatus(IEnumerable<ImportStatus> importStatuses)
        {
            var bulkOps = new List<WriteModel<RawWorkingHour>>();

            foreach (var importStatus in importStatuses)
            {
                var filter = Builders<RawWorkingHour>.Filter.Eq(x => x.Id, importStatus.Id);
                var update = Builders<RawWorkingHour>.Update.Set(x => x.ImportStatus, importStatus);
                var updateOne = new UpdateOneModel<RawWorkingHour>(filter, update);
                bulkOps.Add(updateOne);
            }

            if (bulkOps.Any())
            {
                await _workinghoursCol.BulkWriteAsync(bulkOps);
            }
        }

        public async Task<IEnumerable<RawPOI>> GetPois(bool? isParent, int skip, int pageSize)
        {
            if(isParent == null)
            {
                return await _poiCol.Find(x => true).Skip(skip * pageSize).Limit(pageSize).ToListAsync();
            }
            
            if(isParent == true)
            {
                return await _poiCol.Find(x => x.VMParentId == 0).Skip(skip * pageSize).Limit(pageSize).ToListAsync();
            }

            return await _poiCol.Find(x => x.VMParentId > 0).Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<RawPOI>> GetVfPois(int skip, int pageSize)
        {
            return await _poiCol.Find(x => x.vinfastChargerStation != null).Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<RawPOI>> GetPoisWithGGPlaceId(int skip, int pageSize)
        {
            return await _poiCol.Find(x => !string.IsNullOrEmpty(x.GGPlaceId)).Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<RawEntryPOI>> GetEntryPois(int skip, int pageSize)
        {
            return await _entrypoiCol.Find(x => x.VMParentId > 0).Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }


        public async Task<IEnumerable<RawPOI>> GetChildPois(long id)
        {
            return await _poiCol.Find(x => x.VMParentId == id).ToListAsync();
        }

        public async Task UpdatePoiImportStatus(IEnumerable<ImportStatus> importStatuses)
        {
            //todo:
            await Task.CompletedTask;
        }


        public async Task UpdatePoi(RawPOI poi)
        {
            await _poiCol.ReplaceOneAsync(x => x.Id == poi.Id, poi, new ReplaceOptions
            {
                IsUpsert = true,
            });
        }

        public async Task UpdateEntryPoi(RawEntryPOI poi)
        {
            await _entrypoiCol.ReplaceOneAsync(x => x.Id == poi.Id, poi, new ReplaceOptions
            {
                IsUpsert = true,
            });
        }

        public async Task UpdatePlaceIdByVmId(long vmId, string placeId)
        {
            await _poiCol.UpdateOneAsync(x => x.VMId == vmId, Builders<RawPOI>.Update.Set(x=>x.GGPlaceId, placeId));
        }

        public async Task<IEnumerable<RawAdminCountry>> GetCountries()
        {
            return await _countryCol.Find(x => true).ToListAsync();
        }

        public async Task<IEnumerable<RawAdminCity>> GetCities()
        {
            return await _cityCol.Find(x => true).ToListAsync();
        }

        public async Task<IEnumerable<RawAdminDistrict>> GetDistricts()
        {
            return await _districtCol.Find(x => true).ToListAsync();
        }

        public async Task<IEnumerable<RawAdminWard>> GetWards()
        {
            return await _wardCol.Find(x => true).ToListAsync();
        }

        public async Task InsertImportSummary(ImportSummary importSummary)
        {
            await _importSummaryCol.InsertOneAsync(importSummary);
        }
    }
}
