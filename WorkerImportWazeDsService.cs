using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Models;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.SearchRawDb.Entity;
using Platform.IOTHub.Repository.SearchRawDb.Repository;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Repository.VMPOIRaw.Models;
using Platform.IOTHub.Utility;
using System.Collections.Concurrent;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerImportWazeDsService : BackgroundService
    {
        private readonly ILogger<WorkerImportWazeDsService> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IGoogleRawRepo _googleRawRepo;
        private readonly HttpClient _peakClient;

        public WorkerImportWazeDsService(ILogger<WorkerImportWazeDsService> logger
            , IServiceProvider serviceProvider
            , IGoogleRawRepo googleRawRepo
            , IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _googleRawRepo = googleRawRepo;

            _peakClient = httpClientFactory.CreateClient("PeakApi");
        }

        private readonly ILocationRawRepository _locationRawRepository;
        ConcurrentBag<ImportStatus> importStatuses = new ConcurrentBag<ImportStatus>();

        /// <summary>
        /// 1. load overclicks pois
        /// 2. foreach each poi
        /// 2.1 check poi exit mapping
        /// 2.2 update poi if exit
        /// 2.3 add new poi
        /// 2.3.1 get google place id if vennues, call api to revert
        /// 2.3.2 is existed in party_mapping 
        /// 2.3.2.1 add new record in mapping_waze
        /// 2.3.3 is not existed in party_mapping
        /// 2.3.3.1 generate new poi and new party
        /// 2.3.3.2 has crawlerGoogle
        /// 2.3.3.2.1 add party_images, party_rating
        /// 2.3.3.3 add new record to WazeMappingTracking (Google)
        /// 2.3.3.2 add new poi, mapping_waze, party_mapping
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Models.ImportSummary importSummary = new Models.ImportSummary()
            {
                Namne = "WazePoi",
            };

            using var scope = _serviceProvider.CreateScope();
            IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();
            ILocationRawRepository locationRawRepository = scope.ServiceProvider.GetRequiredService<ILocationRawRepository>();

            int skip = 0;
            int pageSize = 500;
            while (!stoppingToken.IsCancellationRequested)
            {
                //1. load overclicks pois
                var overClicksPois = await locationRawRepository.QueryLocationsOverClick(100, skip, pageSize);
                if(!overClicksPois.Any())
                {
                    break;
                }

                //2. foreach each poi
                foreach(var overClickPoi in overClicksPois)
                {
                    try
                    {
                        //2.1 check poi exit mapping
                        var _mappingWazePoi = await poiRepository.GetMappingWaze(overClickPoi.PlaceId);

                        //2.2 is existed poi
                        if (_mappingWazePoi != null)
                        {
                            //2.2.1 update poi
                            var _imp = new ImportStatus
                            {
                                Id = overClickPoi.PlaceId,
                                Msg = "",
                                Status = PoiImportStatus.Updated.ToString()
                            };

                            var _existedTrackingWaze = (await _googleRawRepo.GetWazeMappingTrackingsByPlaceIds(new string[] { overClickPoi.PlaceId }))?.FirstOrDefault();
                            var vmMapping = await poiRepository.GetMappingVMByPartyId(_existedTrackingWaze?.VmlId ?? string.Empty);
                            // only delete poi only from waze search
                            if(_existedTrackingWaze != null && vmMapping == null)
                            {
                                await poiRepository.DeletePOi(_existedTrackingWaze.VmlId, overClickPoi.PlaceId);
                            }
                            
                            //await updatePoiRaw(poiRepository, locationRawRepository, overClickPoi, _mappingWazePoi);
                            importStatuses.Add(_imp);
                            _logger.LogInformation(JsonConvert.SerializeObject(_imp));
                        }

                        //2.3 add new poi
                        //2.3.1 get google place id if vennues, call api to revert
                        //if (overClickPoi.PlaceId.StartsWith("venues.") && string.IsNullOrEmpty(overClickPoi.AlterPlaceId))
                        //{
                        //    var _vennueId = overClickPoi.PlaceId.Replace("venues.", "");
                        //    var ggRsp = await getGGPlaceId(_vennueId);
                        //    if (ggRsp != null)
                        //    {
                        //        overClickPoi.AlterPlaceId = $"googlePlaces.{ggRsp.googlePlaceId}";
                        //    }
                        //}

                        //2.3.2 is existed in party_mapping 
                        var partyMapping = overClickPoi.PlaceId.StartsWith("venues.") ?
                            await poiRepository.GetPartyMapping(overClickPoi.AlterPlaceId)
                            : await poiRepository.GetPartyMapping(overClickPoi.PlaceId);

                        //2.3.2.1 add new record in mapping_waze
                        var mappingTrackingWaze = (await _googleRawRepo.GetWazeMappingTrackingsByPlaceIds(new string[] { overClickPoi.PlaceId }))?.FirstOrDefault();

                        if (partyMapping == null && !string.IsNullOrWhiteSpace(mappingTrackingWaze?.GgPlaceId))
                        {
                            partyMapping = await poiRepository.GetPartyMapping(mappingTrackingWaze?.GgPlaceId ?? string.Empty);
                        }

                        if(partyMapping == null)
                        {
                            partyMapping = (await poiRepository.GetPoiPartyMapping(mappingTrackingWaze?.VmlId ?? string.Empty))?.FirstOrDefault();
                        }

                        if (partyMapping != null)
                        {
                            await poiRepository.AddPartyWaze(partyMapping.PartyId, overClickPoi.PlaceId);
                            if (!string.IsNullOrEmpty(overClickPoi.AlterPlaceId))
                            {
                                await locationRawRepository.UpdateAlterPlaceId(overClickPoi.PlaceId, overClickPoi.AlterPlaceId);
                                await poiRepository.AddPartyWaze(partyMapping.PartyId, overClickPoi.AlterPlaceId);
                            }

                            if(mappingTrackingWaze != null)
                            {
                                mappingTrackingWaze.VmlId = partyMapping.PartyId;
                                await _googleRawRepo.UpsertWazeMappingTracking(mappingTrackingWaze);
                            }

                            var _imp = new ImportStatus
                            {
                                Id = overClickPoi.PlaceId,
                                Status = PoiImportStatus.Success.ToString(),
                                Msg = "AddNewPartyWaze"
                            };
                            importStatuses.Add(_imp);
                            _logger.LogInformation(JsonConvert.SerializeObject(_imp));
                            continue;
                        }

                        //2.3.3 is not existed in party_mapping

                        
                        string partyId = string.Empty;
                        if (mappingTrackingWaze != null)
                        {
                            if (string.IsNullOrEmpty(mappingTrackingWaze.VmlId))
                            {
                                mappingTrackingWaze.VmlId = $"vml.{Guid.NewGuid().ToString("N")}";
                            }
                            partyId = mappingTrackingWaze.VmlId;
                            await poiRepository.DeletePOi(partyId, overClickPoi.PlaceId);
                            //mappingTrackingWaze.VmlId = $"vml.{Guid.NewGuid().ToString("N")}";
                            //partyId = mappingTrackingWaze.VmlId;
                        }
                        else
                        {
                            mappingTrackingWaze = new WazeMappingTracking
                            {
                                Createddate = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                                GgAddrress = string.Empty,
                                GgCategoryName = string.Empty,
                                GgDistance = 0,
                                GgIsValid = false,
                                GgPlaceId = string.Empty,
                                GgLat = 0,
                                GgLng = 0,
                                GgName = string.Empty,

                                WazeAddress = overClickPoi.Address,
                                WazeId = overClickPoi.PlaceId,
                                WazeAlterId = overClickPoi.AlterPlaceId ?? string.Empty,
                                WazeLatitude = overClickPoi.Latitude,
                                WazeLongitude = overClickPoi.Longitude,
                                WazeName = overClickPoi.Name ?? string.Empty,
                                VmlId = $"vml.{Guid.NewGuid().ToString("N")}"
                            };

                            partyId = mappingTrackingWaze.VmlId;
                        }
                        await _googleRawRepo.UpsertWazeMappingTracking(mappingTrackingWaze);

                        //2.3.3.1 generate new poi and new party
                        Guid partyTypeId = new Guid("d9c03a5c-51d8-42de-bb77-7cff5ff5fc0b");
                        var party = new Party()
                        {
                            Id = partyId,
                            PartyTypeId = partyTypeId,
                        };
                        var newPoi = new POI
                        {
                            Id = partyId,
                            Name = overClickPoi.Name ?? string.Empty,
                            Address = overClickPoi.Address,
                            Label = string.Empty,
                            Lat = overClickPoi.Latitude,
                            Lng = overClickPoi.Longitude,
                            ParentId = string.Empty,
                            GooglePlaceId = string.Empty,
                        };

                        //2.3.3.2 has crawlerGoogle
                        var crawlerGg = mappingTrackingWaze.GgIsValid && !string.IsNullOrEmpty(mappingTrackingWaze.GgPlaceId) 
                            ? (await _googleRawRepo.GetCrawlerGoogles(new string[] { mappingTrackingWaze.GgPlaceId.Replace("googlePlaces.","") })).FirstOrDefault() : null;
                        if (crawlerGg != null)
                        {
                            //2.3.3.2.1 add party_images, party_rating
                            newPoi.GooglePlaceId = mappingTrackingWaze.GgPlaceId;

                            //2.3.3.3.3 add new poi, mapping_waze, party_mapping
                            party.PartyMappings = new List<PartyMapping>();

                            MappingGoogle mappingGoogle = new MappingGoogle()
                            {
                                Address = crawlerGg.address ?? string.Empty,
                                GooglePlaceId = $"googlePlaces.{crawlerGg.placeId}",
                                Lat = crawlerGg.location.lat,
                                Lng = crawlerGg.location.lng,
                                Name = crawlerGg.title ?? string.Empty,
                                PartyId = partyId
                            };
                            party.PartyMappings.Add(new PartyMapping
                            {
                                PartyId = partyId,
                                Source = "Google",
                                SourceId = $"googlePlaces.{crawlerGg.placeId}"
                            });

                            List<MappingWaze> mappingWazes = new List<MappingWaze>();
                            mappingWazes.Add(new MappingWaze
                            {
                                Address = overClickPoi.Address,
                                Lat = overClickPoi.Latitude,
                                Lng = overClickPoi.Longitude,
                                Name = overClickPoi.Name ?? string.Empty,
                                PartyId = partyId,
                                WazeId = overClickPoi.PlaceId
                            });
                            party.PartyMappings.Add(new PartyMapping
                            {
                                PartyId = partyId,
                                SourceId = overClickPoi.PlaceId,
                                Source = "Waze"
                            });

                            if (!string.IsNullOrWhiteSpace(overClickPoi.AlterPlaceId))
                            {
                                mappingWazes.Add(new MappingWaze
                                {
                                    Address = overClickPoi.Address,
                                    Lat = overClickPoi.Latitude,
                                    Lng = overClickPoi.Longitude,
                                    Name = overClickPoi.Name ?? string.Empty,
                                    PartyId = partyId,
                                    WazeId = overClickPoi.AlterPlaceId
                                });

                                party.PartyMappings.Add(new PartyMapping
                                {
                                    PartyId = partyId,
                                    SourceId = overClickPoi.AlterPlaceId,
                                    Source = "Waze"
                                });
                            }

                            await poiRepository.CreateNewPOI(newPoi, party, null, mappingGoogle, mappingWazes);

                            importStatuses.Add(new ImportStatus
                            {
                                Status = PoiImportStatus.Success.ToString(),
                                Msg = "AddNewPOIWithCrawlerGoogle"
                            });
                            _logger.LogInformation(JsonConvert.SerializeObject(importStatuses.Last()));
                        }
                        //2.3.3.3 add new mapping
                        else
                        {
                            //2.3.3.2.1 add party_images, party_rating
                            newPoi.GooglePlaceId = $"";

                            //2.3.3.3.3 add new poi, mapping_waze, party_mapping
                            party.PartyMappings = new List<PartyMapping>();

                            List<MappingWaze> mappingWazes = new List<MappingWaze>();
                            mappingWazes.Add(new MappingWaze
                            {
                                Address = overClickPoi.Address,
                                Lat = overClickPoi.Latitude,
                                Lng = overClickPoi.Longitude,
                                Name = overClickPoi.Name ?? string.Empty,
                                PartyId = partyId,
                                WazeId = overClickPoi.PlaceId
                            });
                            party.PartyMappings.Add(new PartyMapping
                            {
                                PartyId = partyId,
                                SourceId = overClickPoi.PlaceId,
                                Source = "Waze"
                            });

                            if (!string.IsNullOrWhiteSpace(overClickPoi.AlterPlaceId))
                            {
                                mappingWazes.Add(new MappingWaze
                                {
                                    Address = overClickPoi.Address,
                                    Lat = overClickPoi.Latitude,
                                    Lng = overClickPoi.Longitude,
                                    Name = overClickPoi.Name ?? string.Empty,
                                    PartyId = partyId,
                                    WazeId = overClickPoi.AlterPlaceId
                                });

                                party.PartyMappings.Add(new PartyMapping
                                {
                                    PartyId = partyId,
                                    SourceId = overClickPoi.AlterPlaceId,
                                    Source = "Waze"
                                });
                            }

                            await poiRepository.CreateNewPOI(newPoi, party, null, null, mappingWazes);

                            importStatuses.Add(new ImportStatus
                            {
                                Status = PoiImportStatus.Success.ToString(),
                                Msg = "AddNewPOI"
                            });
                        }

                        _logger.LogInformation($"import new {partyId}");
                    }catch(Exception ex)
                    {
                        importStatuses.Add(new ImportStatus { Id = overClickPoi.PlaceId, Status = PoiImportStatus.Error.ToString(), Msg = $"unhandle:{ex.Message}" });
                       
                    }
                }

                skip++;
            }


            importSummary.Total = importStatuses.Count();
            importSummary.TotalAddedNew = importStatuses.Where(x => x.Status == PoiImportStatus.Success.ToString()).Count();
            importSummary.TotalIgnore = importStatuses.Where(x => x.Status == PoiImportStatus.Updated.ToString()).Count();
            importSummary.TotalError = importStatuses.Where(x => x.Status == PoiImportStatus.Error.ToString()).Count();

            await locationRawRepository.InsertImportSummary(new Repository.SearchRawDb.Entity.ImportSummary
            {
                createddate = DateTimeOffset.Now.ToUnixTimeSeconds(),
                Namne = importSummary.Namne,
                Total = importSummary.Total,    
                TotalAddedNew = importSummary.TotalAddedNew,
                TotalIgnore = importSummary.TotalIgnore,
                TotalError = importSummary.TotalError,
                TotalUpdated = importSummary.TotalUpdated,
            });

            _logger.LogInformation("Done");
            Environment.Exit(0);

        }

        private async Task updatePoiRaw(IPoiRepository poiRepository, ILocationRawRepository locationRawRepository, LocationRaw overClickPoi, MappingWaze mpWaze)
        {
            try
            {
                //2.3 add new poi
                //2.3.1 get google place id if vennues, call api to revert
                if (overClickPoi.PlaceId.StartsWith("venues.") && string.IsNullOrEmpty(overClickPoi.AlterPlaceId))
                {
                    var _vennueId = overClickPoi.PlaceId.Replace("venues.", "");
                    var ggRsp = await getGGPlaceId(_vennueId);
                    if (ggRsp != null)
                    {
                        overClickPoi.AlterPlaceId = $"googlePlaces.{ggRsp.googlePlaceId}";
                    }
                }

                var existedPoi = await poiRepository.GetAsync(mpWaze.PartyId);
                existedPoi.Address = overClickPoi.Address;
                existedPoi.Name = overClickPoi.Name;

                var updatedParty = new UpdatePartyData
                {
                    Id = mpWaze.PartyId,
                };

                //2.3.3.2 has crawlerGoogle
                var crawlerGg = !string.IsNullOrWhiteSpace(overClickPoi.AlterPlaceId)
                    ? (await _googleRawRepo.GetCrawlerGoogles(new string[] { overClickPoi.AlterPlaceId.Replace("googlePlaces.", "") })).FirstOrDefault()
                    : (await _googleRawRepo.GetCrawlerGoogles(new string[] { overClickPoi.PlaceId.Replace("googlePlaces.", "") })).FirstOrDefault();
                if (crawlerGg != null)
                {
                    //2.3.3.2.1 add party_images, party_rating
                    existedPoi.GooglePlaceId = $"googlePlaces.{crawlerGg.placeId}";

                    updatedParty.PartyImages = new List<PartyImage>();
                    if (!string.IsNullOrEmpty(crawlerGg.imageUrl))
                    {
                        updatedParty.PartyImages.Add(new PartyImage
                        {
                            Name = "Main",
                            ImageUrl = crawlerGg.imageUrl ?? string.Empty,
                        });
                    }

                    if (crawlerGg.imageUrls.Count > 0)
                    {
                        for (int i = 0; i < crawlerGg.imageUrls.Count; i++)
                        {
                            updatedParty.PartyImages.Add(new PartyImage
                            {
                                Name = $"{i}",
                                ImageUrl = crawlerGg.imageUrls[i],
                            });
                        }
                    }

                    updatedParty.PartyRating = crawlerGg?.reviewsCount > 0 ? new PartyRating
                    {
                        AverageRating = crawlerGg?.totalScore.Value ?? 0,
                        TotalReviews = crawlerGg?.reviewsCount.Value ?? 0,
                    } : null;

                    //2.3.3.2.2 add new record to WazeMappingTracking (Google)
                    await _googleRawRepo.UpsertWazeMappingTracking(new WazeMappingTracking
                    {
                        Createddate = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                        GgAddrress = crawlerGg.address ?? string.Empty,
                        GgCategoryName = crawlerGg.categoryName ?? string.Empty,
                        GgDistance = DistanceCalculator.CalculateDistance(overClickPoi.Latitude, overClickPoi.Longitude, crawlerGg.location.lat, crawlerGg.location.lng) * 1000,
                        GgIsValid = true,
                        GgPlaceId = $"googlePlaces.{crawlerGg.placeId}",
                        GgLat = crawlerGg.location.lat,
                        GgLng = crawlerGg.location.lng,
                        GgName = crawlerGg.title ?? string.Empty,

                        WazeAddress = overClickPoi.Address,
                        WazeId = overClickPoi.PlaceId,
                        WazeAlterId = overClickPoi.AlterPlaceId,
                        WazeLatitude = overClickPoi.Latitude,
                        WazeLongitude = overClickPoi.Longitude,
                        WazeName = overClickPoi.Name ?? string.Empty,

                    });

                    //2.3.3.3.3 add new poi, mapping_waze, party_mapping
                    updatedParty.PartyMappings = new List<PartyMapping>();

                    MappingGoogle mappingGoogle = new MappingGoogle()
                    {
                        Address = crawlerGg.address ?? string.Empty,
                        GooglePlaceId = $"googlePlaces.{crawlerGg.placeId}",
                        Lat = crawlerGg.location.lat,
                        Lng = crawlerGg.location.lng,
                        Name = crawlerGg.title ?? string.Empty,
                        PartyId = mpWaze.PartyId
                    };
                    updatedParty.PartyMappings.Add(new PartyMapping
                    {
                        PartyId = mpWaze.PartyId,
                        Source = "Google",
                        SourceId = $"googlePlaces.{crawlerGg.placeId}"
                    });

                    List<MappingWaze> mappingWazes = new List<MappingWaze>();
                    mappingWazes.Add(new MappingWaze
                    {
                        Address = overClickPoi.Address,
                        Lat = overClickPoi.Latitude,
                        Lng = overClickPoi.Longitude,
                        Name = overClickPoi.Name ?? string.Empty,
                        PartyId = mpWaze.PartyId,
                        WazeId = overClickPoi.PlaceId
                    });
                    updatedParty.PartyMappings.Add(new PartyMapping
                    {
                        PartyId = mpWaze.PartyId,
                        SourceId = overClickPoi.PlaceId,
                        Source = "Waze"
                    });

                    if (!string.IsNullOrWhiteSpace(overClickPoi.AlterPlaceId))
                    {
                        mappingWazes.Add(new MappingWaze
                        {
                            Address = overClickPoi.Address,
                            Lat = overClickPoi.Latitude,
                            Lng = overClickPoi.Longitude,
                            Name = overClickPoi.Name ?? string.Empty,
                            PartyId = overClickPoi.PlaceId,
                            WazeId = overClickPoi.AlterPlaceId
                        });

                        updatedParty.PartyMappings.Add(new PartyMapping
                        {
                            PartyId = overClickPoi.PlaceId,
                            SourceId = overClickPoi.AlterPlaceId,
                            Source = "Waze"
                        });
                    }

                    await poiRepository.UpdatePartyData(updatedParty);

                    importStatuses.Add(new ImportStatus
                    {
                        Status = PoiImportStatus.Success.ToString(),
                        Msg = "AddNewPOIWithCrawlerGoogle"
                    });
                    _logger.LogInformation(JsonConvert.SerializeObject(importStatuses.Last()));
                }
                //2.3.3.3 add new mapping
                else
                {
                    //2.3.3.2.1 add party_images, party_rating
                    //updatedParty.GooglePlaceId = $"";

                    //2.3.3.2.2 add new record to WazeMappingTracking (Google)
                    await _googleRawRepo.UpsertWazeMappingTracking(new WazeMappingTracking
                    {
                        Createddate = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                        GgAddrress = string.Empty,
                        GgCategoryName = string.Empty,
                        GgDistance = 0,
                        GgIsValid = false,
                        GgPlaceId = string.Empty,
                        GgLat = 0,
                        GgLng = 0,
                        GgName = string.Empty,

                        WazeAddress = overClickPoi.Address,
                        WazeId = overClickPoi.PlaceId,
                        WazeAlterId = overClickPoi.AlterPlaceId ?? string.Empty,
                        WazeLatitude = overClickPoi.Latitude,
                        WazeLongitude = overClickPoi.Longitude,
                        WazeName = overClickPoi.Name ?? string.Empty,

                    });

                    //2.3.3.3.3 add new poi, mapping_waze, party_mapping
                    updatedParty.PartyMappings = new List<PartyMapping>();


                    List<MappingWaze> mappingWazes = new List<MappingWaze>();
                    mappingWazes.Add(new MappingWaze
                    {
                        Address = overClickPoi.Address,
                        Lat = overClickPoi.Latitude,
                        Lng = overClickPoi.Longitude,
                        Name = overClickPoi.Name ?? string.Empty,
                        PartyId = updatedParty.Id,
                        WazeId = overClickPoi.PlaceId
                    });
                    updatedParty.PartyMappings.Add(new PartyMapping
                    {
                        PartyId = updatedParty.Id,
                        SourceId = overClickPoi.PlaceId,
                        Source = "Waze"
                    });

                    if (!string.IsNullOrWhiteSpace(overClickPoi.AlterPlaceId))
                    {
                        mappingWazes.Add(new MappingWaze
                        {
                            Address = overClickPoi.Address,
                            Lat = overClickPoi.Latitude,
                            Lng = overClickPoi.Longitude,
                            Name = overClickPoi.Name ?? string.Empty,
                            PartyId = updatedParty.Id,
                            WazeId = overClickPoi.AlterPlaceId
                        });

                        updatedParty.PartyMappings.Add(new PartyMapping
                        {
                            PartyId = updatedParty.Id,
                            SourceId = overClickPoi.AlterPlaceId,
                            Source = "Waze"
                        });
                    }

                    await poiRepository.UpdatePartyData(updatedParty);

                }
            }
            catch (Exception ex)
            {
                importStatuses.Add(new ImportStatus { Id = overClickPoi.PlaceId, Status = PoiImportStatus.Error.ToString(), Msg = $"unhandle:{ex.Message}" });
                _logger.LogInformation(JsonConvert.SerializeObject(importStatuses.Last()));
            }
        }


        private async Task<Models.PeakApi.VenueResponse?> getGGPlaceId(string vennuesId)
        {
            try
            {
                var response = await _peakClient.GetAsync($"/api/wz/v_info/venue?v_id={vennuesId}");
                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var data = JsonConvert.DeserializeObject<Models.PeakApi.VenueResponse>(content);

                    return data;
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            return null;
        }

    }
}
