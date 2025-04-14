using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.Configuration;
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
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Repository.VMPOIRaw.Models;
using Platform.IOTHub.Utility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Drawing.Printing;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Vietmap.NetCore.Legacy.Common;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerImportVMDsService : BackgroundService
    {
        public static int MapVersionCode { get; set; }

        private readonly ILogger<WorkerImportVMDsService> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IVMPoiRawRepository _vmPoiRawRepository;
        private readonly IGoogleRawRepo _googleRawRepo;
        private readonly IVinfastStationRepo _vinfastStationRepo;

        private readonly List<VMMappingConfig> _vmMappings;

        public WorkerImportVMDsService(ILogger<WorkerImportVMDsService> logger
            , IOptions<List<VMMappingConfig>> vmMappings
            , IServiceProvider serviceProvider
            , IVMPoiRawRepository vmPoiRawRepository
            , IVinfastStationRepo vinfastStationRepo
            , IGoogleRawRepo googleRawRepo)
        {
            _logger = logger;

            _vmMappings = vmMappings.Value;

            _serviceProvider = serviceProvider;
            _vmPoiRawRepository = vmPoiRawRepository;
            _vinfastStationRepo = vinfastStationRepo;   
            _googleRawRepo = googleRawRepo;

        }

        private List<Country> countries = new List<Country>();
        private List<City> cities = new List<City>();
        private List<District> districts = new List<District>();
        private List<Ward> wards = new List<Ward>();

        ConcurrentBag<ImportStatus> importStatuses = new ConcurrentBag<ImportStatus>();

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await loadAdminData();

                Models.ImportSummary importSummary = new Models.ImportSummary()
                {
                    Namne = "Poi",
                };

                ////5. import parent pois
                List<Task> tasks = new List<Task>();
                int skip = 0;
                int pageSize = 100;
                while (!stoppingToken.IsCancellationRequested)
                {
                    var rawPois = await _vmPoiRawRepository.GetPois(true, skip, pageSize);
                    if (!rawPois.Any())
                    {
                        break;
                    }
                    List<RawPOI> upsertPois = new List<RawPOI>();
                    List<RawPOI> deletePois = new List<RawPOI>();

                    foreach (var rawPoi in rawPois)
                    {
                        upsertPois.Add(rawPoi);
                    }

                    if (upsertPois.Count > 0)
                    {
                        tasks.Add(importPois(upsertPois));
                    }

                    if (deletePois.Count() > 0)
                    {
                        tasks.Add(removePois(deletePois));
                    }

                    if (tasks.Count > 5)
                    {
                        await Task.WhenAll(tasks);
                        tasks = new List<Task>();
                    }

                    _logger.LogInformation($"imported parent pois skip {skip} size {pageSize}");
                    skip++;
                }
                if(tasks.Count > 0)
                {
                    await Task.WhenAll(tasks);
                }
                _logger.LogInformation("imported parent pois Done");

                //6. import pois
                skip = 0;
                while (!stoppingToken.IsCancellationRequested)
                {
                    var rawPois = await _vmPoiRawRepository.GetPois(false, skip, pageSize);
                    if (!rawPois.Any())
                    {
                        break;
                    }

                    List<RawPOI> upsertPois = new List<RawPOI>();
                    List<RawPOI> deletePois = new List<RawPOI>();

                    foreach (var rawPoi in rawPois)
                    {
                        upsertPois.Add(rawPoi);
                    }

                    if (upsertPois.Count > 0)
                    {
                        tasks.Add(importPois(upsertPois));
                    }

                    if (deletePois.Count() > 0) {
                        tasks.Add(removePois(deletePois));
                    }

                    if (tasks.Count > 5)
                    {
                        await Task.WhenAll(tasks);
                        tasks = new List<Task>();
                    }

                    _logger.LogInformation($"import pois skip {skip} size {pageSize}");
                    skip++;
                }
                if (tasks.Count > 0)
                {
                    await Task.WhenAll(tasks);
                }
                _logger.LogInformation("import pois Done");

                importSummary.Total = importStatuses.Count();
                importSummary.TotalAddedNew = importStatuses.Where(x => x.Status == PoiImportStatus.Success.ToString()).Count();
                importSummary.TotalIgnore = importStatuses.Where(x => x.Status == PoiImportStatus.Updated.ToString()).Count();
                importSummary.TotalError = importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Count();


                _logger.LogInformation($"importPois::summary::{JsonConvert.SerializeObject(importSummary)}");
                _logger.LogInformation($"importPois::errors::{JsonConvert.SerializeObject(importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Select(x => new { id = x.Id, error = x.Msg.ToString() }).ToList())}");

                await _vmPoiRawRepository.InsertImportSummary(new Repository.VMPOIRaw.Entities.ImportSummary
                {
                    Namne = importSummary.Namne,
                    Total = importSummary.Total,
                    TotalAddedNew = importSummary.TotalAddedNew,
                    TotalError  = importSummary.TotalError,
                    TotalIgnore = importSummary.TotalIgnore,
                    TotalUpdated  = importSummary.TotalUpdated,
                    LastModified = DateTime.Now
                });

                _logger.LogInformation("Done");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                Environment.Exit(1);
            }

        }
        

        /// <summary>
        /// 1. 
        /// </summary>
        /// <param name="rawPois"></param>
        /// <returns></returns>
        private async Task importPois(IEnumerable<RawPOI> rawPois)
        {
            using var scope = _serviceProvider.CreateScope();
            IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();
            IWorkingHourRepository workingHourRepository = scope.ServiceProvider.GetRequiredService<IWorkingHourRepository>();
            IBrandRepository brandRepository = scope.ServiceProvider.GetRequiredService<IBrandRepository>();

            //load mapping google raw
            var ggRawMappings = await _googleRawRepo.GetMappingTrackings(rawPois.Select(x => x.VMId));

            foreach (var rawPoi in rawPois)
            {
                string vmlId = string.Empty; 
                try
                {
                    //1. get parent poi if existed
                    MappingVM? parentMappingPoi = null;
                    if (rawPoi.VMParentId > 0)
                    {
                        parentMappingPoi = await poiRepository.GetMappingVM(rawPoi.VMParentId);
                    }

                    //2. get mapping category, if not return error
                    var mappingCat = _vmMappings.Where(x => x.VMCatCodes.Contains(rawPoi.CatIds)).FirstOrDefault();
                    if (mappingCat == null)
                    {
                        //importStatuses.Add(new ImportStatus()
                        //{
                        //    Id = rawPoi.Id,
                        //    Status = nameof(PoiImportStatus.Error),
                        //    Msg = nameof(PoiImportErrorMsg.NotFoundCategory)
                        //});
                        //continue;
                        mappingCat = _vmMappings.FirstOrDefault(x => x.VMLCatId == Guid.Parse( "8d612eec-7fc8-46ee-a300-7ec95fe74da7"));
                    }

                    //3. get mapping brand
                    var dbBrand = mappingCat?.BrandMappings?.Where(x => x.VMCodes.Contains(rawPoi.ChainName ?? " "))?.FirstOrDefault();

                    //4. get mapping branch
                    var dbBranch = dbBrand == null ? null : await brandRepository.GetBranch(rawPoi.BranchName, dbBrand.VMLId);

                    //5. get working hour
                    string workingTime = string.IsNullOrWhiteSpace(rawPoi.WorkingTime) || rawPoi.WorkingTime == " " ? "T2-CN:0000-2400" : rawPoi.WorkingTime;
                    var dbWh = await workingHourRepository.Get(workingTime);

                    //6. if rawPoi already import, ignore it
                    var mappingPoi = await poiRepository.GetMappingVM(rawPoi.VMId);
                    string partyId = $"vml.{ConvertHelper.ComputeSha256Hash(rawPoi.VMId.ToString())}";
                    if (mappingPoi != null)
                    {
                        //toto: delete poi and all relation parties
                        vmlId = partyId;
                        await poiRepository.DeletePOi(partyId);
                    } else
                    {
                        await poiRepository.DeletePOi(partyId);
                    }    

                    //7. generate new party

                    //7.1 generate party with hash vml id
                    vmlId = partyId;
                    Guid partyTypeId = new Guid("d9c03a5c-51d8-42de-bb77-7cff5ff5fc0b");
                    var party = new Party()
                    {
                        Id = partyId,
                        PartyTypeId = partyTypeId,
                    };

                    party.PartyCategories = new List<PartyCategory>()
                        {
                            new PartyCategory()
                            {
                                BranchId = dbBranch?.Id,
                                CategoryId = mappingCat.VMLCatId,
                                BrandId = dbBrand?.VMLId,
                                PartyId = partyId,
                            }
                        };

                    //7.1 add partyCategory conveninece with special poi
                    if (mappingCat.VMLCatCode == "ChargingStation")
                    {
                        party.PartyCategories.Add(new PartyCategory
                        {
                            PartyId = partyId,
                            CategoryId = Guid.Parse("5314506e-8e1f-4471-b674-8378e801cda5"),
                        });
                    }
                    else if (mappingCat.VMLCatCode == "GasStation")
                    {
                        party.PartyCategories.Add(new PartyCategory
                        {
                            PartyId = partyId,
                            CategoryId = Guid.Parse("09357e15-5db1-4b05-b897-8bd2cd7801f0"),
                        });
                    }
                    else if (mappingCat.VMLCatCode == "Parking")
                    {
                        party.PartyCategories.Add(new PartyCategory
                        {
                            PartyId = partyId,
                            CategoryId = Guid.Parse("22f84f8c-d69c-4eea-bec0-dfe087c5b574"),
                        });
                    }
                    else
                    {

                    }


                    //7.2 update party working time
                    if (dbWh != null)
                    {
                        party.PartyWorkingHour = new PartyWorkingHour
                        {
                            Id = Guid.NewGuid(),
                            PartyId = partyId,
                            WorkingHourId = dbWh.Id
                        };
                    }

                    //7.3 if party has parent, add PartyAffiliation
                    if (parentMappingPoi != null)
                    {
                        party.PartyAffiliations = new List<PartyAffiliation>(){
                            new PartyAffiliation
                            {
                                Id = Guid.NewGuid(),
                                PartyId = partyId,
                                ParentPartyId = parentMappingPoi.PartyId,
                            }
                        };
                    }


                    //7.4 add party contact
                    if ((rawPoi.Phones?.Any() ?? false)
                        || (rawPoi.Websites?.Any() ?? false)
                        || (rawPoi.Emails?.Any() ?? false))
                    {
                        party.Contact = new Contact
                        {
                            TelNum1 = rawPoi.Phones?.FirstOrDefault() ?? string.Empty,
                            TelNum2 = string.Empty,
                            Website = rawPoi.Websites?.FirstOrDefault() ?? string.Empty,
                            Email = rawPoi.Emails?.FirstOrDefault() ?? string.Empty,
                            PartyId = partyId
                        };
                    }

                    //7.5 generate party EvesePower and Specials from specials field
                    if (!string.IsNullOrEmpty(rawPoi.Specials))
                    {
                        GenerateRelationFromSpecials(rawPoi, mappingCat.VMLCatCode, party);
                    }

                    //7.6 import admin data
                    string address = rawPoi.Address ?? string.Empty;
                    string fullAmdin = string.Empty;
                    if(rawPoi.admin != null)
                    {
                        var country = countries.FirstOrDefault(x => x.Name == rawPoi.admin.country);
                        var city = cities.FirstOrDefault(x => x.Name == rawPoi.admin.city);
                        var district = districts.FirstOrDefault(x => x.Name == rawPoi.admin.district);
                        var ward = wards.FirstOrDefault(x => x.Name == rawPoi.admin.ward);

                        if(ward != null)
                        {
                            fullAmdin += $", {ward.Name}";
                        }
                        if (district != null)
                        {
                            fullAmdin += $", {district.Name}";
                        }
                        if (city != null)
                        {
                            fullAmdin += $", {city.Name}";
                        }

                        if (country != null 
                            && city != null 
                            && district != null)
                        {
                            party.Admin = new Admin
                            {
                                Id = Guid.NewGuid(),
                                CountryId = country.Id,
                                CityId = city.Id,
                                DistrictId = district.Id,
                                WardId = ward?.Id
                            };
                        }
                    }

                    if (!string.IsNullOrEmpty(fullAmdin))
                    {
                        if (string.IsNullOrEmpty(address))
                        {
                            fullAmdin = fullAmdin.Substring(2, fullAmdin.Length - 2);
                        }

                        address = $"{address} {fullAmdin}";
                    }

                    //7.7 load ggMappingGoogle
                    var _mappingGgTracking = ggRawMappings?.FirstOrDefault(x => x.VmId == rawPoi.VMId);
                    if (_mappingGgTracking == null)
                    {
                        await _googleRawRepo.UpsertMappingTracking(new PlaceMappingTracking
                        {
                            VmId = rawPoi.VMId,
                            VmAddress = rawPoi.Address ?? string.Empty,
                            VmAlterName = rawPoi.AltName,
                            VmCatId = rawPoi.CatIds,
                            VmFullName = rawPoi.FullName ?? string.Empty,
                            VmLatitude = rawPoi.Lat,
                            VmLongitude = rawPoi.Lng,
                            VmName = rawPoi.Name,
                            VmParentId = rawPoi.VMParentId,
                            VmShortName = rawPoi.ShortName,
                            IsReRunGGSearch = true,
                            SearchText = address
                        });
                    } 
                    else
                    {
                        _mappingGgTracking.VmlId = partyId;
                        
                        await _googleRawRepo.UpsertMappingTracking(_mappingGgTracking);
                    }

                    //7.8 add mapping vf station
                    if(mappingCat.VMLCatCode == "ChargingStation" && rawPoi.ChainName.Equals("VinFast", StringComparison.OrdinalIgnoreCase))
                    {
                        var vfMapping = await _vinfastStationRepo.GetMapping(partyId);
                        if (vfMapping == null)
                        {
                            vfMapping = new VinfastStationMapping
                            {
                               VMName = rawPoi.Name,
                               VMAddress = rawPoi.Address ?? string.Empty,
                               VMLat = rawPoi.Lat,
                               VMLId = partyId,
                               VMLng = rawPoi.Lng,  
                               LocationId = string.Empty,
                               stationAddress = string.Empty,
                               stationName = string.Empty
                            };

                            await _vinfastStationRepo.UpsertVfMappingData(vfMapping);
                        }
                    }

                    //insert new poi
                    string _ggPlaceId = _mappingGgTracking?.GgIsValid == true ? _mappingGgTracking?.GgPlaceId ?? string.Empty : string.Empty;
                    if (!string.IsNullOrEmpty(_ggPlaceId) 
                        && _ggPlaceId.StartsWith("venues.")
                        && _mappingGgTracking?.VenueMapping != null
                        && !string.IsNullOrEmpty(_mappingGgTracking.VenueMapping.googlePlaceId))
                    {
                        _ggPlaceId = $"googlePlaces.{_mappingGgTracking.VenueMapping.googlePlaceId}";
                    }

                    var newPoi = new POI
                    {
                        Id = partyId,
                        Name = rawPoi.FullName ?? string.Empty,
                        Address = address,
                        Label = $"{rawPoi.FullName}, {rawPoi.Address}",
                        Lat = rawPoi.Lat,
                        Lng = rawPoi.Lng,
                        ParentId = parentMappingPoi?.PartyId ?? string.Empty,
                        GooglePlaceId = _ggPlaceId,
                        IsDelete = rawPoi.Status.ToUpper() == ImportPOIStatus.DELETE.ToString()
                    };

                    party.PartyMappings = new List<PartyMapping>();
                    var mappingVm = new MappingVM
                    {
                        Address = rawPoi.Address ?? string.Empty,
                        VmId = rawPoi.VMId,
                        ParentId = rawPoi.VMParentId,
                        Lat = rawPoi.Lat,
                        Lng = rawPoi.Lng,
                        Name = rawPoi.FullName ?? string.Empty,
                        PartyId = partyId,
                        Type = rawPoi.CatIds.ToString(),
                    };
                    party.PartyMappings.Add(new PartyMapping
                    {
                        PartyId = partyId,
                        SourceId = partyId,
                        Source = "VML"
                    });


                    MappingGoogle mappingGg = new MappingGoogle();
                    List<MappingWaze> mappingWazes = new List<MappingWaze>();
                    if (_mappingGgTracking != null 
                        && !string.IsNullOrWhiteSpace(_mappingGgTracking.GgPlaceId)
                        && _mappingGgTracking.GgIsValid)
                    {
                        if(!string.IsNullOrEmpty(_mappingGgTracking.SearchMethod))
                        {
                        
                            party.PartyMappings.Add(new PartyMapping
                            {
                                PartyId = partyId,
                                SourceId = _mappingGgTracking.GgPlaceId ?? string.Empty,
                                Source = "VM"
                            });

                            //mappingWazes.Add(mappingWaze);

                            if(_mappingGgTracking.VenueMapping != null && !string.IsNullOrEmpty(_mappingGgTracking.VenueMapping.googlePlaceId))
                            {
                                party.PartyMappings.Add(new PartyMapping
                                {
                                    PartyId = partyId,
                                    SourceId = $"googlePlaces.{_mappingGgTracking.VenueMapping.googlePlaceId}",
                                    Source = "Waze"
                                });
                            }
                        }
                        else
                        {
                            
                            party.PartyMappings.Add(new PartyMapping
                            {
                                PartyId = partyId,
                                SourceId = _mappingGgTracking.GgPlaceId ?? string.Empty,
                                Source = "Google"
                            });
                        }

                    }

                    await poiRepository.CreateNewPOI(newPoi, party, mappingVm, mappingGg, mappingWazes);
                    await GenerateAbilityPOisFromSpecials(poiRepository, newPoi, rawPoi, party);

                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawPoi.Id,
                        Status = nameof(PoiImportStatus.Success)
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogError($"ImportFail::vm id {rawPoi.VMId}::name {rawPoi.Name}::party id {vmlId}");
                    importStatuses.Add(new ImportStatus()
                    {
                        Id = rawPoi.Id,
                        Status = nameof(PoiImportStatus.Error),
                        Msg = nameof(PoiImportErrorMsg.UnHandleError)
                    });

                    _logger.LogError(ex, ex.Message);
                }
            }
           
            foreach (var rawPoi in rawPois)
            {
                rawPoi.ImportStatus = importStatuses.FirstOrDefault(x => x.Id == rawPoi.Id) ?? new ImportStatus();
                await _vmPoiRawRepository.UpdatePoi(rawPoi);
            }
        }

        private async Task removePois(IEnumerable<RawPOI> rawPois)
        {
            using var scope = _serviceProvider.CreateScope();
            IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();

            List<string> removedIds = new List<string>();
            foreach (var rawPoi in rawPois) {
                string partyId = $"vml.{ConvertHelper.ComputeSha256Hash(rawPoi.VMId.ToString())}";
                removedIds.Add(partyId);
            }
            await poiRepository.SetStatusDeletePois(removedIds);
        }

        private async Task GenerateAbilityPOisFromSpecials(IPoiRepository poiRepository, POI poi, RawPOI rawPoi, Party parentParty)
        {
            using var scope = _serviceProvider.CreateScope();
            IPoiRepository _poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();
            
            if(rawPoi.CatIds == "3001")
            {
                rawPoi.Specials = "Ăn Uống,Vệ Sinh";
            }

            var specials = rawPoi.Specials?.Split(',')?.ToList();
            if (specials?.Count > 0)
            {
                foreach (var special in specials)
                {
                    string partyId = $"vml.{Guid.NewGuid().ToString("N")}";

                    Guid partyTypeId = new Guid("d9c03a5c-51d8-42de-bb77-7cff5ff5fc0b");
                    var _party = new Party()
                    {
                        Id = partyId,
                        PartyTypeId = partyTypeId,
                    };

                    _party.PartyCategories = new List<PartyCategory>();
                    var newPoi = new POI
                    {
                        Id = partyId,
                        Name = string.Empty,
                        Address = rawPoi.Address ?? string.Empty,
                        GooglePlaceId = string.Empty,
                        Label = string.Empty,
                        Lat = rawPoi.Lat,
                        Lng = rawPoi.Lng,
                        ParentId = parentParty.Id,
                    };

                    switch (special)
                    {
                        case "Ăn Uống":
                            {
                                _party.PartyCategories.Add(new PartyCategory
                                {
                                    PartyId = partyId,
                                    CategoryId = Guid.Parse("c1736e3f-0fc0-4b15-afd4-31b3bdc1edf9"),
                                });
                                newPoi.Name = "Điểm ăn uống";
                            }
                            break;
                        case "Vệ Sinh":
                        case "Nhà Vệ Sinh":
                            {
                                _party.PartyCategories.Add(new PartyCategory
                                {
                                    PartyId = partyId,
                                    CategoryId = Guid.Parse("d0d11857-f175-49b4-ac07-3058d53184a6"),
                                });
                                newPoi.Name = "Nhà vệ sinh";
                            }
                            break;
                        case "Đỗ Xe":
                            {
                                _party.PartyCategories.Add(new PartyCategory
                                {
                                    PartyId = partyId,
                                    CategoryId = Guid.Parse("22f84f8c-d69c-4eea-bec0-dfe087c5b574"),
                                });
                                newPoi.Name = "Bãi đỗ xe";
                            }
                            break;
                    }


                    if (_party.PartyCategories.Count == 0)
                    {
                        continue;
                    }

                    _party.PartyAffiliations = new List<PartyAffiliation>(){
                            new PartyAffiliation
                            {
                                Id = Guid.NewGuid(),
                                PartyId = partyId,
                                ParentPartyId = parentParty.Id,
                            }
                        };


                    await poiRepository.CreateNewPOI(newPoi, _party, null, null, null);
                }
            }

            

        }

        private async Task<IEnumerable<PartyCategory>> GetConviniences(long vmId, string partyId)
        {
            var results = new List<PartyCategory>();

            var childPois = await _vmPoiRawRepository.GetChildPois(vmId);
            if (childPois == null)
            {
                return results;
            }

            foreach (var poi in childPois)
            {
                var cat = _vmMappings.Where(x => x.VMCatCodes.Contains(poi.CatIds)).FirstOrDefault();
                if (cat == null)
                {
                    continue;
                }

                if (cat.VMLCatCode == "ChargingStation")
                {
                    results.Add(new PartyCategory
                    {
                        PartyId = partyId,
                        CategoryId = Guid.Parse("5314506e-8e1f-4471-b674-8378e801cda5"),
                    });
                }
                else if (cat.VMLCatCode == "GasStation")
                {
                    results.Add(new PartyCategory
                    {
                        PartyId = partyId,
                        CategoryId = Guid.Parse("09357e15-5db1-4b05-b897-8bd2cd7801f0"),
                    });
                }
                else if (cat.VMLCatCode == "Parking")
                {
                    results.Add(new PartyCategory
                    {
                        PartyId = partyId,
                        CategoryId = Guid.Parse("ba0f8d88-15d5-4ec7-a749-e36ea013406f"),
                    });
                }
            }

            return results;
        }

        private void GenerateRelationFromSpecials(RawPOI rawPoi, string vmlCatCode, Party party)
        {
            switch(vmlCatCode)
            {
                case "ChargingStation":
                    {
                        if(!rawPoi.ChainName.Equals("VinFast", StringComparison.OrdinalIgnoreCase))
                        {
                            party.PartyEvsePowers = new List<PartyEvsePower>();
                            var powers = generateEvsePowerFromSpecial(rawPoi.Specials);
                            foreach (var p in powers)
                            {
                                p.Id = Guid.NewGuid();
                                p.PartyId = party.Id;
                                party.PartyEvsePowers.Add(p);
                            }
                        }
                    }
                    break;
                case "GasStation":
                    {
                        var partySpecial = getGasStationPartySpecials(rawPoi.Specials);
                        if(partySpecial != null)
                        {
                            party.PartySpecial = new PartySpecial
                            {
                                Id = Guid.NewGuid(),
                                PartyId = party.Id,
                                Special = partySpecial.Special,
                                GasStation = partySpecial.GasStation,
                            };
                        }   
                    }
                    break;
            }
        }

        private List<PartyEvsePower> generateEvsePowerFromSpecial(string special)
        {
            var results = new List<PartyEvsePower>();
            if (string.IsNullOrWhiteSpace(special)
                || !special.StartsWith("EV="))
            {
                return results;
            }

            var _specialEv = special.Substring("EV=".Length, special.Length - "EV=".Length);
            var _specialEvDatas = _specialEv.Split(',');

            if(_specialEvDatas.Length < 2)
            {
                return results;
            }

            var _types = _specialEvDatas[1].Split('/');
            var _totalEvs = 0;
            int.TryParse(_specialEvDatas[2], out _totalEvs);

            if (_types.Length == 1)
            {
                var _partyEvsePower = new PartyEvsePower
                {
                    Type = getEvsType(_types[0]),
                    TotalEvse = _totalEvs
                };
                results.Add(_partyEvsePower);
            }
            else
            {
                foreach (var _type in _types)
                {
                    var _partyEvsePower = new PartyEvsePower
                    {
                        Type = getEvsType(_type),
                        TotalEvse = 0
                    };
                    results.Add(_partyEvsePower);
                }
            }

            return results;

        }

        private int getEvsType(string type)
        {
            string pattern = @"^(AC|DC)(\d+)";
            Match match = Regex.Match(type, pattern);

            if (match.Success)
            {
                if (int.TryParse(match.Groups[2].Value, out int power))
                {
                    return power * 1000;
                }
            }

            _logger.LogError($"Cannot convert power type {type}");
            return 0;
        }
    
        private PartySpecial? getGasStationPartySpecials(string special)
        {
            List<PartySpecial> partySpecials = new List<PartySpecial>();
            var _specials = special.Split(',');
            string _foundGasSpecial = string.Empty;
            foreach (var _special in _specials)
            {
                if (_special.Contains("Xăng")
                    || _special.Contains("Do"))
                {
                    _foundGasSpecial += $"{_special},";
                }
            }

            if(_foundGasSpecial.Length > 0)
            {
                _foundGasSpecial = _foundGasSpecial.Substring(0, _foundGasSpecial.Length -1);
            }

            return _foundGasSpecial.Length == 0? null : new PartySpecial
            {
                Special = _foundGasSpecial,
                GasStation = 0
            };
        }
    
        private async Task loadAdminData()
        {
            using var scope = _serviceProvider.CreateScope();
            IAdminRepository adminRepository = scope.ServiceProvider.GetRequiredService<IAdminRepository>();

            countries = (await adminRepository.GetCountries()).ToList();
            cities = (await adminRepository.GetCities()).ToList();
            districts = (await adminRepository.GetDistricts()).ToList();
            wards = (await adminRepository.GetWards()).ToList();

        }
    }
}
