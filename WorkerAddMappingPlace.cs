using Amazon.Runtime.Internal.Util;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Platform.IOTHub.ImportPOI.Service.Helper;
using Platform.IOTHub.ImportPOI.Service.Models;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Repository.VMPOIRaw.Models;
using Platform.IOTHub.Utility;
using ZstdSharp.Unsafe;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerAddMappingPlace : BackgroundService
    {
        private readonly ILogger<WorkerImportVMDsService> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IVMPoiRawRepository _vmPoiRawRepository;
        private readonly IGoogleRawRepo _googleRawRepo;

        private readonly List<VMMappingConfig> _vmMappings;

        private readonly HttpClient _client;
        private readonly HttpClient _peakClient;
        private readonly IConfiguration _configuration;

        public WorkerAddMappingPlace(ILogger<WorkerImportVMDsService> logger
            , IOptions<List<VMMappingConfig>> vmMappings
            , IServiceProvider serviceProvider
            , IVMPoiRawRepository vmPoiRawRepository
            , IGoogleRawRepo googleRawRepo
            , IHttpClientFactory httpClientFactory
            , IConfiguration configuration)
        {
            _logger = logger;

            _vmMappings = vmMappings.Value;

            _serviceProvider = serviceProvider;
            _vmPoiRawRepository = vmPoiRawRepository;
            _googleRawRepo = googleRawRepo;

            _client = httpClientFactory.CreateClient("WazeApi");
            _peakClient = httpClientFactory.CreateClient("PeakApi");

            _configuration = configuration;

        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = _serviceProvider.CreateScope();

            List<Task> tasks = new List<Task>();
            int skip = 0;
            int pageSize = 100;

            while (!stoppingToken.IsCancellationRequested)
            {
                var rawPois = await _vmPoiRawRepository.GetPois(null, skip, pageSize);
                if (!rawPois.Any())
                {
                    break;
                }
                tasks.Add(AddMappingPlaceData(rawPois));
                _logger.LogInformation($"add place skip {skip} size {pageSize}");
                if(tasks.Count > 2)
                {
                    await Task.WhenAll(tasks);
                    tasks = new List<Task>();
                }
                skip++;
            }

            if(tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }

            _logger.LogInformation("Done");
            Environment.Exit(0);
        }

        private async Task AddMappingPlaceData(IEnumerable<RawPOI> rawPois)
        {
            try
            {
                foreach(var poi in rawPois)
                {
                    var mappingTracking = await _googleRawRepo.GetMappingTracking(poi.VMId);
                    if(mappingTracking != null
                        && mappingTracking.GgIsValid)
                    {
                        poi.GGPlaceId = mappingTracking.GgPlaceId;
                        await _vmPoiRawRepository.UpdatePoi(poi);
                        continue;
                    }

                    //if (mappingTracking != null
                    //    && !mappingTracking.GgIsValid
                    //    && !string.IsNullOrEmpty(mappingTracking.GgPlaceId))
                    //{
                    //    continue;
                    //}

                    if(mappingTracking == null)
                    {
                        mappingTracking = new PlaceMappingTracking
                        {
                            VmId = poi.VMId,
                            VmParentId = poi.VMParentId,
                            VmAddress = poi.Address,
                            VmName = poi.Name,
                            VmFullName = poi.FullName,
                            VmShortName = poi.ShortName,
                            VmAlterName = poi.AltName,
                            VmLatitude = poi.Lat,
                            VmLongitude = poi.Lng,
                            VmCatId = poi.CatIds
                        };
                    }

                    poi.GGPlaceId = string.Empty;
                    
                    //// 1. search name & address
                    //var rsp = await AutocompleteAsync($"{poi.Name} {poi.Address}", poi.Lat, poi.Lng);
                    //if(rsp != null)
                    //{
                    //    poi.MappingPlaces = rsp.Select(x => new AutocompletePlace
                    //    {
                    //        Address = x.Address,
                    //        Coordinate = new Repository.VMPOIRaw.Entities.BaseCoordinateDto
                    //        {
                    //            Latitude = x.Coordinate.Latitude,
                    //            Longitude = x.Coordinate.Longitude,
                    //        },
                    //        Description = x.Description,
                    //        Distance = x.Distance,
                    //        Name = x.Name,
                    //        PlaceId = x.PlaceId,    
                    //    }).ToList();

                    //    var _mplace = poi.MappingPlaces.Where(x => x.Distance < 200).OrderBy(x => x.Distance)?.FirstOrDefault();
                    //    if(_mplace != null)
                    //    {
                    //        poi.GGPlaceId = _mplace.PlaceId;

                    //        mappingTracking.GgAddrress = _mplace.Address;
                    //        mappingTracking.GgDistance = _mplace.Distance;
                    //        mappingTracking.GgLat = _mplace.Coordinate.Latitude;
                    //        mappingTracking.GgLng = _mplace.Coordinate.Longitude;
                    //        mappingTracking.GgPlaceId = _mplace.PlaceId;
                    //        mappingTracking.GgName = _mplace.Name;
                    //        mappingTracking.SearchMethod = SearchMappingPlaceMethod.NameAndAddress.ToString();
                    //        mappingTracking.SearchText = $"{poi.Name} {poi.Address}";
                    //    }
                    //}

                    ////2.1 Vinfast - name & vfStation name
                    ////2.2 not vinfast - full name
                    //if (string.IsNullOrWhiteSpace(poi.GGPlaceId))
                    //{
                    //    bool isVfchargStation = poi.ChainName == "VinFast" && poi.CatIds == "10013-1";

                    //    if (isVfchargStation && !string.IsNullOrEmpty(poi.vinfastChargerStation?.locationId))
                    //    {
                    //        var rsp2 = await AutocompleteAsync($"{poi.Name} {poi.vinfastChargerStation.stationName}", poi.vinfastChargerStation.latitude, poi.vinfastChargerStation.longitude);

                    //        if (rsp2 != null)
                    //        {
                    //            poi.MappingPlaces = rsp2.Select(x => new AutocompletePlace
                    //            {
                    //                Address = x.Address,
                    //                Coordinate = new Repository.VMPOIRaw.Entities.BaseCoordinateDto
                    //                {
                    //                    Latitude = x.Coordinate.Latitude,
                    //                    Longitude = x.Coordinate.Longitude,
                    //                },
                    //                Description = x.Description,
                    //                Distance = x.Distance,
                    //                Name = x.Name,
                    //                PlaceId = x.PlaceId,
                    //            }).ToList();

                    //            var _mplace = poi.MappingPlaces.Where(x => x.Distance < 200).OrderBy(x => x.Distance)?.FirstOrDefault();
                    //            if (_mplace != null)
                    //            {
                    //                poi.GGPlaceId = _mplace.PlaceId;

                    //                mappingTracking.GgAddrress = _mplace.Address;
                    //                mappingTracking.GgDistance = _mplace.Distance;
                    //                mappingTracking.GgLat = _mplace.Coordinate.Latitude;
                    //                mappingTracking.GgLng = _mplace.Coordinate.Longitude;
                    //                mappingTracking.GgPlaceId = _mplace.PlaceId;
                    //                mappingTracking.GgName = _mplace.Name;
                    //                mappingTracking.SearchMethod = SearchMappingPlaceMethod.NameAndVFStationName.ToString();
                    //                mappingTracking.SearchText = $"{poi.Name} {poi.vinfastChargerStation.stationName}";
                    //            } 
                    //            else
                    //            {
                    //                mappingTracking.SearchText = $"{poi.vinfastChargerStation.stationName}";
                    //                mappingTracking.SearchMethod = SearchMappingPlaceMethod.NameAndAdmin.ToString();
                    //                await _googleRawRepo.UpsertMappingTracking(mappingTracking);
                    //                continue;
                    //            }
                    //        }


                    //    }
                    //    else
                    //    {
                    //        var rsp2 = await AutocompleteAsync($"{poi.FullName}", poi.Lat, poi.Lng);

                    //        if (rsp2 != null)
                    //        {
                    //            poi.MappingPlaces = rsp2.Select(x => new AutocompletePlace
                    //            {
                    //                Address = x.Address,
                    //                Coordinate = new Repository.VMPOIRaw.Entities.BaseCoordinateDto
                    //                {
                    //                    Latitude = x.Coordinate.Latitude,
                    //                    Longitude = x.Coordinate.Longitude,
                    //                },
                    //                Description = x.Description,
                    //                Distance = x.Distance,
                    //                Name = x.Name,
                    //                PlaceId = x.PlaceId,
                    //            }).ToList();

                    //            var _mplace = poi.MappingPlaces.Where(x => x.Distance < 200).OrderBy(x => x.Distance)?.FirstOrDefault();
                    //            if (_mplace != null)
                    //            {
                    //                poi.GGPlaceId = _mplace.PlaceId;

                    //                mappingTracking.GgAddrress = _mplace.Address;
                    //                mappingTracking.GgDistance = _mplace.Distance;
                    //                mappingTracking.GgLat = _mplace.Coordinate.Latitude;
                    //                mappingTracking.GgLng = _mplace.Coordinate.Longitude;
                    //                mappingTracking.GgPlaceId = _mplace.PlaceId;
                    //                mappingTracking.GgName = _mplace.Name;
                    //                mappingTracking.SearchMethod = SearchMappingPlaceMethod.FullName.ToString();
                    //                mappingTracking.SearchText = $"{poi.FullName}";
                    //            }
                    //        }
                    //    }
                    //}

                    ////3. name & cat name
                    //if (string.IsNullOrWhiteSpace(poi.GGPlaceId))
                    //{
                    //    var rsp2 = await AutocompleteAsync($"{poi.Name} {poi.CatName}", poi.Lat, poi.Lng);

                    //    if (rsp2 != null)
                    //    {
                    //        poi.MappingPlaces = rsp2.Select(x => new AutocompletePlace
                    //        {
                    //            Address = x.Address,
                    //            Coordinate = new Repository.VMPOIRaw.Entities.BaseCoordinateDto
                    //            {
                    //                Latitude = x.Coordinate.Latitude,
                    //                Longitude = x.Coordinate.Longitude,
                    //            },
                    //            Description = x.Description,
                    //            Distance = x.Distance,
                    //            Name = x.Name,
                    //            PlaceId = x.PlaceId,
                    //        }).ToList();

                    //        var _mplace = poi.MappingPlaces.Where(x => x.Distance < 200).OrderBy(x => x.Distance)?.FirstOrDefault();
                    //        if (_mplace != null)
                    //        {
                    //            poi.GGPlaceId = _mplace.PlaceId;

                    //            mappingTracking.GgAddrress = _mplace.Address;
                    //            mappingTracking.GgDistance = _mplace.Distance;
                    //            mappingTracking.GgLat = _mplace.Coordinate.Latitude;
                    //            mappingTracking.GgLng = _mplace.Coordinate.Longitude;
                    //            mappingTracking.GgPlaceId = _mplace.PlaceId;
                    //            mappingTracking.GgName = _mplace.Name;
                    //            mappingTracking.SearchMethod = SearchMappingPlaceMethod.NameAndCatName.ToString();
                    //            mappingTracking.SearchMethod = $"{poi.Name} {poi.CatName}";
                    //        }
                    //    }
                    //}

                    //4. name & admin info
                    string _searchText = $"{poi.FullName}";
                    if (poi.admin != null)
                    {
                        //if (!string.IsNullOrWhiteSpace(poi.admin.ward))
                        //{
                        //    _searchText += $",{poi.admin.ward}";
                        //}
                        if (!string.IsNullOrWhiteSpace(poi.admin.district))
                        {
                            _searchText += $",{poi.admin.district}";
                        }
                        //if (!string.IsNullOrWhiteSpace(poi.admin.city))
                        //{
                        //    _searchText += $",{poi.admin.city}";
                        //}
                    }

                    //if (string.IsNullOrWhiteSpace(poi.GGPlaceId) && !string.IsNullOrWhiteSpace(_searchText))
                    //{
                    //    var rsp2 = await AutocompleteAsync($"{_searchText}", poi.Lat, poi.Lng);

                    //    if (rsp2 != null)
                    //    {
                    //        poi.MappingPlaces = rsp2.Select(x => new AutocompletePlace
                    //        {
                    //            Address = x.Address,
                    //            Coordinate = new Repository.VMPOIRaw.Entities.BaseCoordinateDto
                    //            {
                    //                Latitude = x.Coordinate.Latitude,
                    //                Longitude = x.Coordinate.Longitude,
                    //            },
                    //            Description = x.Description,
                    //            Distance = x.Distance,
                    //            Name = x.Name,
                    //            PlaceId = x.PlaceId,
                    //        }).ToList();

                    //        var _mplace = poi.MappingPlaces.Where(x => x.Distance < 400).OrderBy(x => x.Distance)?.FirstOrDefault();
                    //        if (_mplace != null)
                    //        {
                    //            poi.GGPlaceId = _mplace.PlaceId;

                    //            mappingTracking.GgAddrress = _mplace.Address;
                    //            mappingTracking.GgDistance = _mplace.Distance;
                    //            mappingTracking.GgLat = _mplace.Coordinate.Latitude;
                    //            mappingTracking.GgLng = _mplace.Coordinate.Longitude;
                    //            mappingTracking.GgPlaceId = _mplace.PlaceId;
                    //            mappingTracking.GgName = _mplace.Name;
                    //            mappingTracking.SearchMethod = SearchMappingPlaceMethod.NameAndAdmin.ToString();
                    //            mappingTracking.SearchText = $"{_searchText}";
                    //        }
                    //    }
                    //}

                    //if (poi.GGPlaceId.StartsWith("venues."))
                    //{
                    //    var rsp3 = await GetGGPlaceId(poi.GGPlaceId.Replace("venues.", ""));
                    //    if (rsp3 != null)
                    //    {
                    //        poi.VenueMapping = new VenueMapping
                    //        {
                    //            id = rsp3.id,
                    //            distance = DistanceCalculator.CalculateDistance(poi.Lat, poi.Lng, rsp3.latLng?.lat ?? 0, rsp3.latLng?.lng ?? 0),
                    //            googlePlaceId = rsp3.googlePlaceId,
                    //            latLng = new VenueMappingLatLng
                    //            {
                    //                lat = rsp3.latLng?.lat ?? 0,
                    //                lng = rsp3.latLng?.lng ?? 0,
                    //            }
                    //        };

                    //        if(!string.IsNullOrWhiteSpace(rsp3.googlePlaceId))
                    //        {
                    //            poi.GGPlaceId = $"googlePlaces.{rsp3.googlePlaceId}";
                    //            mappingTracking.GgPlaceId = $"googlePlaces.{rsp3.googlePlaceId}";
                    //        }

                    //    }
                    //}    

                    //await _vmPoiRawRepository.UpdatePoi(poi);
                    
                    mappingTracking.GgIsValid = false;
                    mappingTracking.SearchText = _searchText;
                    if(poi.MappingPlaces?.Count > 0)
                    {
                        var nearliestPlace = poi.MappingPlaces.OrderBy(x => x.Distance).FirstOrDefault();
                        mappingTracking.GgAddrress = nearliestPlace.Address;
                        mappingTracking.GgDistance = nearliestPlace.Distance;
                        mappingTracking.GgLat = nearliestPlace.Coordinate.Latitude;
                        mappingTracking.GgLng = nearliestPlace.Coordinate.Longitude;
                        mappingTracking.GgPlaceId = nearliestPlace.PlaceId;
                        mappingTracking.GgName = nearliestPlace.Name;
                    }

                    await _googleRawRepo.UpsertMappingTracking(mappingTracking);
                }
                
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                Environment.Exit(1);
            }
        }

        private async Task<Models.PeakApi.VenueResponse?> GetGGPlaceId(string vennuesId)
        {
            try
            {
                var response = await _peakClient.GetAsync($"/api/wz/v_info/venue?v_id={vennuesId}");
                if(response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var data = JsonConvert.DeserializeObject<Models.PeakApi.VenueResponse>(content);

                    return data;
                }    

            }catch(Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            return null;
        }

        private async Task<IEnumerable<AutocompletePlaceDto>> AutocompleteAsync(string streetAddress, double lat, double lng)
        {
            try
            {
                var responsePlatform = await _client.GetAsync($"/autocomplete/q?c=wd&sll={lat},{lng}&s={_configuration["Waze:Key"]}&q={streetAddress}&lang=vi&e=ROW&exp=14,15,16,18");

                var resultPlatform = await responsePlatform.Content.ReadAsStringAsync();

                var entries = JsonConvert.DeserializeObject<List<object>>(resultPlatform);

                var entries2 = JsonConvert.DeserializeObject<List<object>>(JsonConvert.SerializeObject(entries[1]));

                List<AutocompletePlaceDto> response = new List<AutocompletePlaceDto>();

                foreach (var item in entries2)
                {
                    var entries3 = JsonConvert.DeserializeObject<List<object>>(JsonConvert.SerializeObject(item));

                    var data = new AutocompletePlaceDto();
                    data.Name = entries3[0].ToString();

                    AutocompleteWazeResponse wazeResp = JsonConvert.DeserializeObject<AutocompleteWazeResponse>(JsonConvert.SerializeObject(entries3[3]));
                    data.Address = !string.IsNullOrEmpty(wazeResp.o?.d) ? wazeResp.o?.d : data.Name;

                    data.Coordinate = new Models.BaseCoordinateDto
                    {
                        Latitude = wazeResp.y,
                        Longitude = wazeResp.x
                    };

                    data.PlaceId = wazeResp.v;
                    data.Description = data.Name;
                    data.RateSimmilar = TextHelper.CalculateSimilarity(streetAddress, data.Address);
                    data.Distance = DistanceCalculator.CalculateDistance(lat, lng, wazeResp.y, wazeResp.x) * 1000;
                    if (!string.IsNullOrEmpty(data.PlaceId))
                    {
                        response.Add(data);
                    }
                }

                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);

            }
            return null;
        }
    }
}
