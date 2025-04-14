using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Repository.VMPOIRaw.Models;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public interface IVMPoiRawRepository
    {
        Task<IEnumerable<RawCategory>> GetCategories();
        Task UpdateCategoyImportStatus(IEnumerable<ImportStatus> importStatuses);

        Task<IEnumerable<RawBrand>> GetBrands();
        Task UpdateChainImportStatus(IEnumerable<ImportStatus> importStatuses);

        Task<IEnumerable<RawBranch>> GetBranchs();
        Task UpdateBranchImportStatus(IEnumerable<ImportStatus> importStatuses);

        Task<IEnumerable<RawWorkingHour>> GetWorkingHours();
        Task UpdateWorkingHourImportStatus(IEnumerable<ImportStatus> importStatuses);

        Task<IEnumerable<RawPOI>> GetPois(bool? isParent, int skip, int pageSize);
        Task<IEnumerable<RawPOI>> GetVfPois(int skip, int pageSize);

        Task<IEnumerable<RawEntryPOI>> GetEntryPois(int skip, int pageSize);
        Task<IEnumerable<RawPOI>> GetChildPois(long id);
        Task<IEnumerable<RawPOI>> GetPoisWithGGPlaceId(int skip, int pageSize);

        Task UpdatePoiImportStatus(IEnumerable<ImportStatus> importStatuses);
        Task UpdatePoi(RawPOI poi);
        Task UpdateEntryPoi(RawEntryPOI poi);
        Task UpdatePlaceIdByVmId(long vmId, string placeId);    

        Task<IEnumerable<RawAdminCountry>> GetCountries();
        Task<IEnumerable<RawAdminCity>> GetCities();
        Task<IEnumerable<RawAdminDistrict>> GetDistricts();
        Task<IEnumerable<RawAdminWard>> GetWards();

        Task InsertImportSummary(ImportSummary importSummary);
    }
}
