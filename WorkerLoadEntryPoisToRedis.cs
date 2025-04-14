using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.IOTHub.ImportPOI.Service.Cache;
using Platform.IOTHub.Repository.POI.Repositories;
using Redis.OM;
using Redis.OM.Contracts;
using Redis.OM.Searching;
using StackExchange.Redis;
using Vietmap.NetCore.Legacy.Common;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerLoadEntryPoisToRedis : BackgroundService
    {
        private readonly ILogger<WorkerLoadEntryPoisToRedis> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConfiguration _configuration;


        public WorkerLoadEntryPoisToRedis(ILogger<WorkerLoadEntryPoisToRedis> logger
            , IServiceProvider serviceProvider
            , IConfiguration configuration)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = GetConfigurationOptions();
            ConnectionMultiplexer.Connect(config);
            var provider = new RedisConnectionProvider(ConnectionMultiplexer.Connect(config));
            IRedisConnection _redis = provider.Connection;

             _redis.DropIndexAndAssociatedRecords(typeof(EntryPoiSearchCache));

            var indexInf = _redis.GetIndexInfo(typeof(EntryPoiSearchCache));
            if (indexInf == null)
            {
                _redis.CreateIndex(typeof(EntryPoiSearchCache));
            }
            IRedisCollection<EntryPoiSearchCache> _autoCompleteCol = provider.RedisCollection<EntryPoiSearchCache>();

            using var scope = _serviceProvider.CreateScope();
            IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();

            int size = 500;
            int index = 0;
            while (true)
            {
                //1. load entry pois
                //1.1 query parent entry pois
                var fullEntryPois = await poiRepository.QueryFullEntryPois(index, size);
                if (fullEntryPois == null
                    || fullEntryPois.Count() == 0)
                {
                    break;
                }

                //1.2 import parent entry pois
                foreach (var fullEntryPoi in fullEntryPois)
                {
                    var parentPoi = fullEntryPoi.Parent;
                    await _autoCompleteCol.InsertAsync(new EntryPoiSearchCache
                    {
                        Id = parentPoi.Id,
                        ParentId = string.Empty,
                        GGplaceId = parentPoi.GooglePlaceId ?? string.Empty,
                        GGParentPlaceId = string.Empty,
                        Address = parentPoi.Address,
                        Click = 0,
                        Geom = new Redis.OM.Modeling.GeoLoc
                        {
                            Latitude = parentPoi.Lat,
                            Longitude = parentPoi.Lng
                        },
                        Name = parentPoi.Name,
                        SearchText = $"{parentPoi.Name}"
                    });


                    foreach(var enPo in fullEntryPoi.EntryPOIs)
                    {
                        var names = enPo.Name.Split('-');
                        await _autoCompleteCol.InsertAsync(new EntryPoiSearchCache
                        {
                            Id = $"{enPo.Id}.en_",
                            ParentId = parentPoi?.Id ?? string.Empty,
                            GGplaceId = enPo.GooglePlaceId ?? string.Empty,
                            GGParentPlaceId = parentPoi?.GooglePlaceId ?? string.Empty,
                            Address = enPo.Address,
                            Click = 0,
                            Geom = new Redis.OM.Modeling.GeoLoc
                            {
                                Latitude = enPo.Lat,
                                Longitude = enPo.Lng,
                            },
                            Name = enPo.Name,
                            SearchText = $"{parentPoi?.Name ?? string.Empty}, {enPo.Name}"
                        });

                        var otherGGPlaces = await poiRepository.GetPoiPartyMapping(parentPoi?.Id ?? string.Empty);
                        var parentGGPlaceIds = otherGGPlaces.Where(x => x.Source != "VML").Where(x => x.SourceId != parentPoi.GooglePlaceId).Select(x => x.SourceId).Distinct().ToList();
                        foreach (var parentGGPlaceId in parentGGPlaceIds)
                        {
                            await _autoCompleteCol.InsertAsync(new EntryPoiSearchCache
                            {
                                Id = $"{enPo.Id}.en_{parentGGPlaceId.Replace("googlePlaces.","").Replace("venues.","")}",
                                ParentId = parentPoi?.Id ?? string.Empty,
                                GGplaceId = enPo.GooglePlaceId ?? string.Empty,
                                GGParentPlaceId = parentGGPlaceId,
                                Address = enPo.Address,
                                Click = 0,
                                Geom = new Redis.OM.Modeling.GeoLoc
                                {
                                    Latitude = enPo.Lat,
                                    Longitude = enPo.Lng,
                                },
                                Name = enPo.Name,
                                SearchText = ""
                            });
                        }
                    }
                }

                index++;
                _logger.LogInformation($"ImportEntryPois::index::{index}::size::{fullEntryPois.Count()}");
            }

            _logger.LogInformation("Done");
            Environment.Exit(0);
        }

        private ConfigurationOptions GetConfigurationOptions()
        {
            var configurationOptions = new ConfigurationOptions
            {
                SyncTimeout = 60000, // 60 seconds
                ConnectTimeout = 60000, // 60 seconds
                Password = _configuration["PoiRedis:Password"]
            };

            var hosts = (_configuration["PoiRedis:Hosts"] ?? string.Empty).Split(new[] { ';', ',', '|' }, StringSplitOptions.RemoveEmptyEntries);
            var ports = (_configuration["PoiRedis:Ports"] ?? string.Empty).Split(new[] { ';', ',', '|' }, StringSplitOptions.RemoveEmptyEntries);
            for (var i = 0; i < hosts.Count(); i++)
            {
                configurationOptions.EndPoints.Add(hosts[i], Convert.ToInt32(ports[i]));
            }

            return configurationOptions;
        }

    }
}
