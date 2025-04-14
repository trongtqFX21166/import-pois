using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Platform.IOTHub.ImportPOI.Service.Helper;
using Platform.IOTHub.ImportPOI.Service.Models;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Models;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Models;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerImportMasterDataService : BackgroundService
    {
        private readonly ILogger<WorkerImportVMDsService> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IVMPoiRawRepository _vmPoiRawRepository;

        private readonly List<VMMappingConfig> _vmMappings;

        public WorkerImportMasterDataService(ILogger<WorkerImportVMDsService> logger
            , IOptions<List<VMMappingConfig>> vmMappings
            , IServiceProvider serviceProvider
            , IVMPoiRawRepository vmPoiRawRepository)
        {
            _logger = logger;

            _vmMappings = vmMappings.Value;

            _serviceProvider = serviceProvider;
            _vmPoiRawRepository = vmPoiRawRepository;

        }

        private List<Category> categories = new List<Category>();
        private List<Brand> brands = new List<Brand>();
        private List<Branch> branches = new List<Branch>();

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                //using var scope = _serviceProvider.CreateScope();
                //_categoryRepository = scope.ServiceProvider.GetRequiredService<ICategoryRepository>();
                //_partyTypeRepository = scope.ServiceProvider.GetRequiredService<IPartyTypeRepository>();
                //_brandRepository = scope.ServiceProvider.GetRequiredService<IBrandRepository>();
                //_poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();
                //_workingHoursRepo = scope.ServiceProvider.GetRequiredService<IWorkingHourRepository>();

                // 1. load master data
                await loadMasterData();
                _logger.LogInformation("loaded master data");

                // 2. import brands
                await importBrands();
                _logger.LogInformation("imported brands");

                //3. import branch
                await importBranchs();
                _logger.LogInformation("imported branchs");

                //4. import working hours
                await importWorkingHours();
                _logger.LogInformation("imported working hours");

                _logger.LogInformation("Done");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                Environment.Exit(1);
            }

        }

        private async Task loadMasterData()
        {
            using var scope = _serviceProvider.CreateScope();
            ICategoryRepository _categoryRepository = scope.ServiceProvider.GetRequiredService<ICategoryRepository>();
            IBrandRepository _brandRepository = scope.ServiceProvider.GetRequiredService<IBrandRepository>();
            
            categories = await _categoryRepository.GetAllCategoriesAsync();
            brands = await _brandRepository.GetAll();
        }

        private async Task importBrands()
        {
            using var scope = _serviceProvider.CreateScope();
            ICategoryRepository _categoryRepository = scope.ServiceProvider.GetRequiredService<ICategoryRepository>();
            IBrandRepository _brandRepository = scope.ServiceProvider.GetRequiredService<IBrandRepository>();

            Models.ImportSummary importSummary = new Models.ImportSummary()
            {
                Namne = "Brand",
            };

            List<ImportStatus> importStatuses = new List<ImportStatus>();

            var rawBrands = await _vmPoiRawRepository.GetBrands();

            foreach (var rawBrand in rawBrands.Where(s => !string.IsNullOrWhiteSpace(s.CatIds) && !string.IsNullOrWhiteSpace(s.ChainName)))
            {
                try
                {
                    //check category
                    var mappingCat = _vmMappings.Where(x => x.VMCatCodes.Contains(rawBrand.CatIds) || x.VMCatCodes.Contains("")).FirstOrDefault();
                    if (mappingCat == null)
                    {
                        importStatuses.Add(new ImportStatus()
                        {
                            Id = rawBrand.Id,
                            Status = PoiImportStatus.Error.ToString(),
                            Msg = PoiImportErrorMsg.NotFoundCategory.ToString()
                        });
                        continue;
                    }

                    //check brand
                    var dbBrand = await _brandRepository.GetBrand(rawBrand.ChainName);
                    if (dbBrand != null)
                    {
                        importStatuses.Add(new ImportStatus()
                        {
                            Id = rawBrand.Id,
                            Status = PoiImportStatus.Updated.ToString(),
                        });
                        continue;
                    }

                    var newBrand = new Brand()
                    {
                        CategoryId = mappingCat.VMLCatId,
                        ChainName = rawBrand.ChainName,
                        Code = TextHelper.GenerateCode(rawBrand.ChainName),
                        ImagePngUrl = string.Empty,
                        ImageUrl = string.Empty,
                        SecondaryImageUrl = string.Empty,
                        Name = rawBrand.ChainName,
                        Order = 0
                    };

                    var brandId = await _brandRepository.CreateBrand(newBrand);
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawBrand.Id,
                        Status = PoiImportStatus.Success.ToString(),
                    });
                }
                catch (Exception ex)
                {
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawBrand.Id,
                        Status = PoiImportStatus.Error.ToString(),
                        Msg = $"{PoiImportErrorMsg.UnHandleError.ToString()}|{ex.Message}"
                    });

                    _logger.LogError(ex, ex.Message);
                }
            }

            importSummary.Total = rawBrands.Count();
            importSummary.TotalAddedNew = importStatuses.Where(x => x.Status == PoiImportStatus.Success.ToString()).Count();
            importSummary.TotalIgnore = importStatuses.Where(x => x.Status == PoiImportStatus.Updated.ToString()).Count();
            importSummary.TotalError = importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Count();


            await _vmPoiRawRepository.UpdateChainImportStatus(importStatuses);


            _logger.LogInformation($"importBrands::summary::{JsonConvert.SerializeObject(importSummary)}");
        }

        private async Task importBranchs()
        {
            using var scope = _serviceProvider.CreateScope();
            ICategoryRepository _categoryRepository = scope.ServiceProvider.GetRequiredService<ICategoryRepository>();
            IBrandRepository _brandRepository = scope.ServiceProvider.GetRequiredService<IBrandRepository>();

            Models.ImportSummary importSummary = new Models.ImportSummary()
            {
                Namne = "Branch",
            };

            List<ImportStatus> importStatuses = new List<ImportStatus>();

            var rawBranchs = await _vmPoiRawRepository.GetBranchs();

            foreach (var rawBranch in rawBranchs.Where(x => !string.IsNullOrWhiteSpace(x.BranchName)))
            {
                try
                {
                    if (string.IsNullOrEmpty(rawBranch?.CatIds))
                    {
                        continue;
                    }

                    //check category
                    var mappingCat = _vmMappings.Where(x => x.VMCatCodes.Contains(rawBranch.CatIds)).FirstOrDefault();
                    if (mappingCat == null)
                    {
                        importStatuses.Add(new ImportStatus()
                        {
                            Id = rawBranch.Id,
                            Status = PoiImportStatus.Error.ToString(),
                            Msg = PoiImportErrorMsg.NotFoundCategory.ToString()
                        });
                        continue;
                    }

                    //check brand
                    var mappingBrand = mappingCat.BrandMappings?.Where(x => x.VMCodes.Contains(rawBranch.ChainName ?? ""))?.FirstOrDefault();
                    if (mappingBrand == null)
                    {
                        importStatuses.Add(new ImportStatus()
                        {
                            Id = rawBranch.Id,
                            Status = PoiImportStatus.Error.ToString(),
                            Msg = PoiImportErrorMsg.NotFoundBrand.ToString()
                        });
                        continue;
                    }

                    //check branch
                    var dbBranch = await _brandRepository.GetBranch(rawBranch.BranchName, mappingBrand.VMLId);
                    if (dbBranch != null)
                    {
                        importStatuses.Add(new ImportStatus()
                        {
                            Id = rawBranch.Id,
                            Status = PoiImportStatus.Updated.ToString()
                        });
                        continue;
                    }

                    var newBranch = new Branch()
                    {
                        Code = TextHelper.GenerateCode(rawBranch.BranchName),
                        ImagePngUrl = string.Empty,
                        ImageUrl = string.Empty,
                        SecondaryImageUrl = string.Empty,
                        Name = rawBranch.BranchName,
                        Order = 0,
                        BrandId = mappingBrand.VMLId
                    };

                    var branchId = await _brandRepository.CreateBranch(newBranch);
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawBranch.Id,
                        Status = PoiImportStatus.Success.ToString(),
                    });
                }
                catch (Exception ex)
                {
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawBranch.Id,
                        Status = PoiImportStatus.Error.ToString(),
                        Msg = $"{PoiImportErrorMsg.UnHandleError.ToString()}|{ex.Message}"
                    });

                }
            }

            importSummary.Total = rawBranchs.Count();
            importSummary.TotalAddedNew = importStatuses.Where(x => x.Status == PoiImportStatus.Success.ToString()).Count();
            importSummary.TotalIgnore = importStatuses.Where(x => x.Status == PoiImportStatus.Updated.ToString()).Count();
            importSummary.TotalError = importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Count();

            await _vmPoiRawRepository.UpdateBranchImportStatus(importStatuses);

            _logger.LogInformation($"importBranchs::summary::{JsonConvert.SerializeObject(importSummary)}");
        }

        private async Task importWorkingHours()
        {
            using var scope = _serviceProvider.CreateScope();
            IWorkingHourRepository _workingHoursRepo = scope.ServiceProvider.GetRequiredService<IWorkingHourRepository>();

            Models.ImportSummary importSummary = new Models.ImportSummary()
            {
                Namne = "WorkingHours",
            };

            List<ImportStatus> importStatuses = new List<ImportStatus>();

            var rawWHs = await _vmPoiRawRepository.GetWorkingHours();

            foreach (var rawWh in rawWHs)
            {
                try
                {
                    var dbWh = await _workingHoursRepo.Get(rawWh.WorkingTime);
                    if (dbWh != null)
                    {
                        importStatuses.Add(new ImportStatus()
                        {
                            Id = rawWh.Id,
                            Status = PoiImportStatus.Updated.ToString(),
                        });
                        continue;
                    }

                    var newDbWh = new WorkingHour
                    {
                        Id = Guid.NewGuid(),
                        DatetimeRange = rawWh.WorkingTime,

                    };

                    var _wkTimes = rawWh.WorkingTime.Split(';');
                    List<string> openningHours = new List<string>();
                    List<OpenningTime> openningTimeDisplays = new List<OpenningTime>();
                    foreach (var _wk in _wkTimes)
                    {
                        var _openningHours = TextHelper.GenerateHoursFromVMWorkingHours(_wk) ?? new List<string>();
                        if (_openningHours?.Count > 0)
                        {
                            openningHours.AddRange(_openningHours);
                        }

                        var _openningTimeDisplays = TextHelper.GenerateDisplayOpeningTimeFromVMWorkingHours(_wk);
                        if (_openningTimeDisplays?.Count > 0)
                        {
                            openningTimeDisplays.AddRange(_openningTimeDisplays);
                        }
                    }

                    newDbWh.OpenningHours = string.Join(",", openningHours);
                    newDbWh.OpenningTimeDispay = JsonConvert.SerializeObject(openningTimeDisplays, Formatting.None);

                    await _workingHoursRepo.Create(newDbWh);
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawWh.Id,
                        Status = PoiImportStatus.Success.ToString(),
                    });
                }
                catch (Exception ex)
                {
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawWh.Id,
                        Status = PoiImportStatus.Error.ToString(),
                        Msg = PoiImportErrorMsg.UnHandleError.ToString()
                    });

                    _logger.LogError(ex, ex.Message);
                }
            }

            await _vmPoiRawRepository.UpdateWorkingHourImportStatus(importStatuses); 

            importSummary.Total = rawWHs.Count();
            importSummary.TotalAddedNew = importStatuses.Where(x => x.Status == PoiImportStatus.Success.ToString()).Count();
            importSummary.TotalIgnore = importStatuses.Where(x => x.Status == PoiImportStatus.Updated.ToString()).Count();
            importSummary.TotalError = importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Count();

            _logger.LogInformation($"importBrands::summary::{JsonConvert.SerializeObject(importSummary)}");
        }

    }
}
