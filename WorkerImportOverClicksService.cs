using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.SearchRawDb.Repository;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Repository.VMPOIRaw.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerImportOverClicksService : BackgroundService
    {
        private readonly ILogger<WorkerImportOverClicksService> _logger;
        private readonly IGoogleRawRepo _googleRawRepo;
        private readonly IServiceProvider _serviceProvider;
        private readonly HttpClient _peakClient;

        public WorkerImportOverClicksService(ILogger<WorkerImportOverClicksService> logger
            , IGoogleRawRepo googleRawRepo
            , IServiceProvider serviceProvider
            , IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _googleRawRepo = googleRawRepo;
            _serviceProvider = serviceProvider;

            _peakClient = httpClientFactory.CreateClient("PeakApi");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = _serviceProvider.CreateScope();
            IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();
            ILocationRawRepository locationRawRepository = scope.ServiceProvider.GetRequiredService<ILocationRawRepository>();

            int skip = 0;
            int pageSize = 500;
            List<ImportStatus> importStatuses = new List<ImportStatus>();
            while (!stoppingToken.IsCancellationRequested)
            {
                //1. load overclicks pois
                var overClicksPois = await locationRawRepository.QueryLocationsOverWeeklyClick(10, skip, pageSize);
                if (!overClicksPois.Any())
                {
                    break;
                }

                //2. upsert waze mappings data
                foreach (var overClickPoi in overClicksPois) {

                    _logger.LogInformation($"clicks:{overClickPoi.PlaceId}");
                    try
                    {
                        //1 check existed tracking mapping waze
                        var _trackingMappingWaze = (await _googleRawRepo.GetWazeMappingTrackingsByPlaceIds(new List<string>() { overClickPoi.PlaceId })).FirstOrDefault();
                        string partyId = !string.IsNullOrWhiteSpace(_trackingMappingWaze?.VmlId) ? _trackingMappingWaze.VmlId : $"vml.{Guid.NewGuid().ToString("N")}";

                        if (_trackingMappingWaze == null)
                        {
                            _trackingMappingWaze = new WazeMappingTracking
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
                                VmlId = partyId,
                                IsReRunGGSearch = true,

                                Lastmodified = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                            };
                        }
                        else
                        {
                            _trackingMappingWaze.IsReRunGGSearch = true;
                            _trackingMappingWaze.Lastmodified = DateTimeOffset.Now.ToUnixTimeSeconds();
                        }

                        if (_trackingMappingWaze.WazeId.StartsWith("venues.")
                            && string.IsNullOrWhiteSpace(_trackingMappingWaze.GgPlaceId))
                        {
                            var wazeRsp = await GetGGPlaceId(_trackingMappingWaze.WazeId);
                            if (wazeRsp != null)
                            {
                                _trackingMappingWaze.WazeAlterId = wazeRsp.googlePlaceId;
                                _logger.LogInformation($"clicks:{overClickPoi.PlaceId}::WazeAlterId::{_trackingMappingWaze.WazeAlterId}");
                            }
                        }

                        //2 existed party mapping
                        if (!string.IsNullOrWhiteSpace(_trackingMappingWaze.VmlId))
                        {
                            //2.2.1 update 
                            await _googleRawRepo.UpsertWazeMappingTracking(_trackingMappingWaze);

                            _logger.LogInformation($"clicks:{overClickPoi.PlaceId}::UpdateCrawler");

                        }
                        //2.3 new party mapping 
                        else
                        {

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

                            _logger.LogInformation($"clicks:{overClickPoi.PlaceId}::AddNew");
                        }
                    }
                    catch (Exception ex) {
                        _logger.LogError($"{overClickPoi.PlaceId} import error:{ex.Message}");
                    }
                }

                skip++;
            }

            _logger.LogInformation("Done");
            Environment.Exit(0);
        }


        private async Task<Models.PeakApi.VenueResponse?> GetGGPlaceId(string vennuesId)
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
