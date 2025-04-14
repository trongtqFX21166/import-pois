using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Platform.IOTHub.ImportPOI.Service.Helper;
using Platform.IOTHub.ImportPOI.Service.Models;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Models;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerImportAdminDataService : BackgroundService
    {
        private readonly ILogger<WorkerImportVMDsService> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IVMPoiRawRepository _vmPoiRawRepository;

        private readonly List<VMMappingConfig> _vmMappings;

        public WorkerImportAdminDataService(ILogger<WorkerImportVMDsService> logger
            , IOptions<List<VMMappingConfig>> vmMappings
            , IServiceProvider serviceProvider
            , IVMPoiRawRepository vmPoiRawRepository)
        {
            _logger = logger;

            _vmMappings = vmMappings.Value;

            _serviceProvider = serviceProvider;
            _vmPoiRawRepository = vmPoiRawRepository;


        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await importCountries();
                await importCities();
                await importDistricts();
                await importWards();

                _logger.LogInformation("Done");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                Environment.Exit(1);
            }

        }

        private async Task importCountries()
        {
            using var scope = _serviceProvider.CreateScope();
            IAdminRepository _adminRepository = scope.ServiceProvider.GetRequiredService<IAdminRepository>();
            try
            {
                // Get existing countries
                var existingCountries = await _adminRepository.GetCountries();

                var newCountry = new Country
                {
                    Id = Guid.NewGuid(),
                    Code = "VN",
                    Name = "Việt Nam",
                    Lat = 14.058324, // Approximate latitude for Vietnam
                    Lng = 108.277199, // Approximate longitude for Vietnam
                };

                // Check if Vietnam already exists by code or name
                if (!existingCountries.Any(c => c.Code == newCountry.Code || c.Name == newCountry.Name))
                {
                    await _adminRepository.UpsertCountries(new List<Country>() { newCountry });
                    _logger.LogInformation("Added {CountryName} (Code: {CountryCode}) to countries.", newCountry.Name, newCountry.Code);
                }
                else
                {
                    _logger.LogInformation("{CountryName} (Code: {CountryCode}) already exists in countries. Skipping import.", newCountry.Name, newCountry.Code);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error importing countries: {Message}", ex.Message);
            }

            _logger.LogInformation("importCountries completed.");
        }

        private async Task importCities()
        {
            using var scope = _serviceProvider.CreateScope();
            IAdminRepository _adminRepository = scope.ServiceProvider.GetRequiredService<IAdminRepository>();

            try
            {
                var countries = await _adminRepository.GetCountries();
                var existingCities = await _adminRepository.GetCities();
                var rawCities = await _vmPoiRawRepository.GetCities();

                var country = countries.FirstOrDefault();
                if (country == null)
                {
                    _logger.LogError("No country found. Cannot import cities.");
                    return;
                }

                var citiesToAdd = new List<City>();

                foreach (var rawCity in rawCities)
                {
                    var cityCode = TextHelper.GenerateCode(rawCity.Name);
                    if (!existingCities.Any(c => c.Code == cityCode || c.Name == rawCity.Name))
                    {
                        citiesToAdd.Add(new City
                        {
                            Id = Guid.NewGuid(),
                            Code = cityCode,
                            Name = rawCity.Name,
                            CountryId = country.Id,
                            Lat = 0.0,
                            Lng = 0.0,
                        });
                    }
                    else
                    {
                        _logger.LogInformation("City {CityName} (Code: {CityCode}) already exists. Skipping.", rawCity.Name, cityCode);
                    }
                }

                if (citiesToAdd.Any())
                {
                    await _adminRepository.UpsertCities(citiesToAdd);
                    _logger.LogInformation("Added {Count} new cities.", citiesToAdd.Count);
                }
                else
                {
                    _logger.LogInformation("No new cities to add.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error importing cities: {Message}", ex.Message);
            }

            _logger.LogInformation("importCities completed.");
        }

        private async Task importDistricts()
        {
            using var scope = _serviceProvider.CreateScope();
            IAdminRepository _adminRepository = scope.ServiceProvider.GetRequiredService<IAdminRepository>();


            List<ImportStatus> importStatuses = new List<ImportStatus>();

            var countries = await _adminRepository.GetCountries();
            var cities = await _adminRepository.GetCities();

            var rawDistricts = await _vmPoiRawRepository.GetDistricts();
            List<District> districtDbs = new List<District>();
            try
            {
               
                foreach(var district in rawDistricts)
                {
                    var city = cities.FirstOrDefault(c => c.Name == district.City);
                    if(city == null)
                    {
                        _logger.LogInformation($"not found city {district.City}");
                        continue;
                    }

                    districtDbs.Add(new District
                    {
                        Id = Guid.NewGuid(),
                        CityId = city.Id,
                        Code = TextHelper.GenerateCode(district.Name),
                        Name = district.Name,
                    });
                }
                
                await _adminRepository.UpsertDistricts(districtDbs.Select(x => new District
                {
                    Id = Guid.NewGuid(),
                    Code = TextHelper.GenerateCode(x.Name),
                    Name = x.Name,
                    CityId = x.CityId,
                    Lat = 0.0,
                    Lng = 0.0,
                }));

                importStatuses.Add(new ImportStatus()
                {
                    Status = PoiImportStatus.Success.ToString(),
                });
            }
            catch (Exception ex)
            {
                importStatuses.Add(new ImportStatus()
                {
                    Status = PoiImportStatus.Error.ToString(),
                    Msg = PoiImportErrorMsg.UnHandleError.ToString()
                });

                _logger.LogError(ex, ex.Message);
            }

            _logger.LogInformation($"importDistricts::{districtDbs.Count}");
        }

        private async Task importWards()
        {
            using var scope = _serviceProvider.CreateScope();
            IAdminRepository _adminRepository = scope.ServiceProvider.GetRequiredService<IAdminRepository>();

            Models.ImportSummary importSummary = new Models.ImportSummary()
            {
                Namne = "Ward",
            };

            List<ImportStatus> importStatuses = new List<ImportStatus>();

            var countries = await _adminRepository.GetCountries();
            var cities = await _adminRepository.GetCities();
            var districts = await _adminRepository.GetDistricts();

            var wards = await _vmPoiRawRepository.GetWards();
            List<Ward> WardsDb = new List<Ward>();
            try
            {
                foreach (var ward in wards)
                {
                    var district = districts.FirstOrDefault(x => x.Name == ward.District);
                    if (district == null)
                    {
                        _logger.LogInformation($"not found District {ward.District} ");
                        continue;
                    }

                    WardsDb.Add(new Ward
                    {
                        DistrictId = district.Id,
                        Code = TextHelper.GenerateCode(ward.Name),
                        Name = ward.Name,
                    });
                }

                await _adminRepository.UpsertWards(WardsDb.Select(x => new Ward
                {
                    Id = Guid.NewGuid(),
                    Code = TextHelper.GenerateCode(x.Name),
                    Name = x.Name,
                    DistrictId = x.DistrictId,
                    Lat = 0.0,
                    Lng = 0.0,
                }));

                importStatuses.Add(new ImportStatus()
                {
                    Status = PoiImportStatus.Success.ToString(),
                });
            }
            catch (Exception ex)
            {
                importStatuses.Add(new ImportStatus()
                {
                    Status = PoiImportStatus.Error.ToString(),
                    Msg = PoiImportErrorMsg.UnHandleError.ToString()
                });

                _logger.LogError(ex, ex.Message);
            }

            _logger.LogInformation($"importWards::summary::{WardsDb.Count}");
        }
    }
}
