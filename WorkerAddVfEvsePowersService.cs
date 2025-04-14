using Elastic.Apm.Api.Kubernetes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using Platform.IOTHub.ImportPOI.Service.Models;
using Platform.IOTHub.ImportPOI.Service.Models.PeakApi;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Models;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Utility;
using System.Text;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerAddVfEvsePowersService : BackgroundService
    {
        private readonly ILogger<WorkerAddVfEvsePowersService> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IVinfastStationRepo _vfStationRepo;

        private readonly List<VMMappingConfig> _vmMappings;

        private readonly HttpClient _client;
        private readonly IConfiguration _configuration;

        public WorkerAddVfEvsePowersService(ILogger<WorkerAddVfEvsePowersService> logger
            , IOptions<List<VMMappingConfig>> vmMappings
            , IServiceProvider serviceProvider
            , IVinfastStationRepo vpnStationRepo
            , IHttpClientFactory httpClientFactory
            , IConfiguration configuration)
        {
            _logger = logger;

            _vmMappings = vmMappings.Value;

            _serviceProvider = serviceProvider;
            _vfStationRepo = vpnStationRepo;

            _client = httpClientFactory.CreateClient("PeakApi");

            _configuration = configuration;

        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();

                List<Task> tasks = new List<Task>();
                int skip = 0;
                int pageSize = 500;
                while (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation($"query from {skip * pageSize} with {pageSize} items");
                    //1. query pois evese powers
                    var vfMappings = await _vfStationRepo.QueryMappingVfStations(skip, pageSize);
                    if (vfMappings == null || !vfMappings.Any())
                    {
                        break;
                    }

                    foreach (var vfMapping in vfMappings)
                    {
                        //2. call api get list powers
                        var vfPowerRaw = await _vfStationRepo.GetVfStation(vfMapping.LocationId);

                        var updateData = new UpdatePartyEvseData()
                        {
                            Id = vfMapping.VMLId,
                            PartyEvsePowers = new List<PartyEvsePower>(),
                            PartyImages = new List<PartyImage>()
                        };

                        if (vfPowerRaw?.evsePowers?.Any() ?? false)
                        {
                            foreach (var evsePower in vfPowerRaw.evsePowers)
                            {
                                updateData.PartyEvsePowers.Add(new PartyEvsePower
                                {
                                    Id = Guid.NewGuid(),
                                    PartyId = vfMapping.VMLId,
                                    Type = evsePower.type,
                                    TotalEvse = evsePower.totalEvse,
                                });
                            }
                        }

                        if (vfPowerRaw?.images?.Any() ?? false)
                        {
                            for (int i = 0; i < vfPowerRaw.images.Count; i++)
                            {
                                updateData.PartyImages.Add(new PartyImage
                                {
                                    Id = Guid.NewGuid(),
                                    ImageUrl = vfPowerRaw.images[i].url,
                                    PartyId = vfMapping.VMLId,
                                    Name = i.ToString(),
                                });
                            }
                        }

                        await poiRepository.UpdatePartyEvsePowerData(updateData);
                    }

                    skip++;

                    _logger.LogInformation("Done");

                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                Environment.Exit(1);
            }
            Environment.Exit(0);

        }

        private async Task<VinfastChargerStation?> crawlVfStation(VinfastStationMapping vfMapping)
        {
            VinfastChargerStation result = null;
            int retry = 3;
            try
            {
                while (retry > 0)
                {
                    var request = new Models.PeakApi.SearchStationRequest
                    {
                        excludeFavorite = true,
                        latitude = vfMapping.VMLat,
                        longitude = vfMapping.VMLng,
                    };

                    var response = await _client.PostAsync("/api/vf/search-stations", new StringContent(JsonConvert.SerializeObject(request), Encoding.UTF8, "application/json"));
                    if (response.IsSuccessStatusCode)
                    {
                        var _content = await response.Content.ReadAsStringAsync();
                        if (string.IsNullOrEmpty(_content))
                        {
                            retry--;
                            continue;
                        }

                        var rsp = JsonConvert.DeserializeObject<VfStationResponse>(_content, new JsonSerializerSettings
                        {
                            Converters = new List<JsonConverter> { new BsonDocumentConverter() }
                        });
                        if (rsp == null || rsp.data.Count == 0)
                        {
                            retry--;
                            continue;
                        }
                        await _vfStationRepo.UpsertCrawlerData(rsp.data);

                        result = BsonSerializer.Deserialize<VinfastChargerStation>(rsp.data[0]);
                    }

                    if (string.IsNullOrEmpty(result?.locationId))
                    {
                        retry--;
                        continue;
                    }

                    break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            return result;
        }
    }
}
