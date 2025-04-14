using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.IOTHub.Repository.POI.Repositories;
using Redis.OM.Contracts;
using Redis.OM;
using StackExchange.Redis;
using Microsoft.Extensions.Configuration;
using Redis.OM.Searching;
using Platform.IOTHub.ImportPOI.Service.Cache;
using Platform.IOTHub.ImportPOI.Service.Helper;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerLoadRedisService: BackgroundService
    {
        private readonly ILogger<WorkerLoadRedisService> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConfiguration _configuration;


        public WorkerLoadRedisService(ILogger<WorkerLoadRedisService> logger
            , IServiceProvider serviceProvider
            , IConfiguration configuration)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _configuration = configuration;
        }

        private List<PoiCategoryCache> DefaultConveniences = new List<PoiCategoryCache>
        {
            new PoiCategoryCache()
            {
                Id = Guid.Parse("09357e15-5db1-4b05-b897-8bd2cd7801f0"),
                Code = "A_GasStation",
                ImageUrl = "https://api.vietmap.live/share/images/nearby/v2/a_gas.svg",
                MarkerUrl = "https://api.vietmap.live/share/images/nearby/v2/marker/gas.png",
                MarkerSelectedUrl = string.Empty,
                Name = "Trạm xăng",
                InActive = true
            },
            new PoiCategoryCache()
            {
                Id = Guid.Parse("5314506e-8e1f-4471-b674-8378e801cda5"),
                Code = "A_ChargingStation",
                ImageUrl = "https://api.vietmap.live/share/images/nearby/v2/a_charging.svg",
                MarkerUrl = "https://api.vietmap.live/share/images/nearby/v2/marker/charging.png",
                MarkerSelectedUrl = string.Empty,
                Name = "Trạm sạc",
                InActive = true
            },
            new PoiCategoryCache()
            {
                Id = Guid.Parse("22f84f8c-d69c-4eea-bec0-dfe087c5b574"),
                Code = "A_Parking",
                ImageUrl = "https://api.vietmap.live/share/images/nearby/v2/a_parking.svg",
                MarkerUrl = "https://api.vietmap.live/share/images/nearby/v2/marker/parking.png",
                MarkerSelectedUrl = string.Empty,
                Name = "Bãi đỗ xe",
                InActive = true
            },
            new PoiCategoryCache()
            {
                Id = Guid.Parse("c1736e3f-0fc0-4b15-afd4-31b3bdc1edf9"),
                Code = "A_FoodAndBeverages",
                ImageUrl = "https://api.vietmap.live/share/images/nearby/v2/a_cafe.svg",
                MarkerUrl = string.Empty,
                MarkerSelectedUrl = string.Empty,
                Name = "Ăn uống",
                InActive = true
            },
            new PoiCategoryCache()
            {
                Id = Guid.Parse("d0d11857-f175-49b4-ac07-3058d53184a6"),
                Code = "A_WC",
                ImageUrl = "https://api.vietmap.live/share/images/nearby/v2/a_restroom.svg",
                MarkerUrl = string.Empty,
                MarkerSelectedUrl = string.Empty,
                Name = "Nhà vệ sinh",
                InActive = true
            }
        };
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = GetConfigurationOptions();
            ConnectionMultiplexer.Connect(config);
            var provider = new RedisConnectionProvider(ConnectionMultiplexer.Connect(config));
            IRedisConnection _redis = provider.Connection;

            _redis.DropIndexAndAssociatedRecords(typeof(PoiSearchCache));

            _redis.CreateIndex(typeof(PoiSearchCache));
            IRedisCollection<PoiSearchCache> _autoCompleteCol = provider.RedisCollection<PoiSearchCache>();


            using var scope = _serviceProvider.CreateScope();
            IPoiRepository poiRepository = scope.ServiceProvider.GetRequiredService<IPoiRepository>();

            int skip = 0;
            int pageSize = 200;
            while (!stoppingToken.IsCancellationRequested)
            { 
                var fullPois = await poiRepository.GetPOIs(new Guid[] { Guid.Parse("d9c03a5c-51d8-42de-bb77-7cff5ff5fc0b") }, new Guid[] {}, skip, pageSize);
                if(fullPois == null || fullPois.Count() == 0)
                {
                    break;
                }

                foreach(var pois in fullPois)
                {
                    var poiSearch = new PoiSearchCache
                    {
                        Id = pois.Id,
                        ParentId = pois.ParentId,
                        Address = pois.Address,
                        BrandCodes = pois.Brands?.Where(x => x != null)?.Select(x => x.Code)?.ToList() ?? new List<string>(),
                        CategoryCodes = pois.Categories?.Where(x => x != null)?.Select(x => x.Code)?.ToList() ?? new List<string>(),
                        ConvenienceCodes = pois.Conveniences?.Where(x => x != null)?.Select(x => x.Code)?.ToList() ?? new List<string>(),
                        Brands = pois.Brands?.Where(x => x != null)?.Select(x => new PoiCategoryCache
                        {
                            Id = x.Id,
                            Code = x.Code,
                            ImageUrl = x.ImageUrl ?? string.Empty,
                            MarkerUrl = x.MarkerUrl ?? string.Empty,
                            MarkerSelectedUrl = x.MarkerSelectedUrl ?? string.Empty,
                            Name = x.Name
                        })?.ToList() ?? new List<PoiCategoryCache>(),
                        Categories = pois.Categories?.Where(x => x != null)?.Select(x => new PoiCategoryCache
                        {
                            Id = x.Id,
                            Code = x.Code,
                            ImageUrl = x.ImageUrl ?? string.Empty,
                            MarkerUrl = x.MarkerUrl ?? string.Empty,
                            MarkerSelectedUrl = x.MarkerSelectedUrl ?? string.Empty,
                            Name = x.Name,
                            InActive = true
                        })?.ToList() ?? new List<PoiCategoryCache>(),
                        Conveniences = DefaultConveniences.Select(x => new PoiCategoryCache
                        {
                            Code = x.Code,
                            Id = x.Id,
                            ImageUrl= x.ImageUrl ?? string.Empty,
                            InActive = x.InActive,
                            MarkerSelectedUrl = x.MarkerSelectedUrl,
                            MarkerUrl = x.MarkerUrl,
                            Name = x.Name
                        }).ToList(),
                        Click = 0,
                        Geom = new Redis.OM.Modeling.GeoLoc
                        {
                            Latitude = pois.Lat,
                            Longitude = pois.Lng
                        },
                        GGplaceId = pois.GGplaceId,
                        Name = pois.Name,
                        OpeningHours = pois.OpenningHours?.FirstOrDefault() ?? string.Empty,
                        EvsePowers = pois.PartyEvsePowers?.Where(p => p != null && p.Type > 0)?.Select(p => new EvsePowerCache
                        {
                            Type = p.Type,
                            TotalEvse = p.TotalEvse
                        })?.OrderBy(x => x.Type)?.ToList() ?? new List<EvsePowerCache>(),
                        Rating = pois.PartyRating == null ? null : new RatingCache
                        {
                            AverageRating = pois.PartyRating.AverageRating,
                            TotalReviews = pois.PartyRating.TotalReviews,
                            Source = pois.PartyRating.Source,
                            SourceUrl = pois.PartyRating.SourceUrl
                        }
                    };

                    if (poiSearch.CategoryCodes.Contains("A_GasStation"))
                    {
                        poiSearch.Conveniences.RemoveAll(x => x.Code == "A_GasStation");
                    } else if (poiSearch.CategoryCodes.Contains("A_ChargingStation"))
                    {
                        poiSearch.Conveniences.RemoveAll(x => x.Code == "A_ChargingStation");
                        poiSearch.Conveniences.RemoveAll(x => x.Code == "A_GasStation");
                    }
                    else if (poiSearch.CategoryCodes.Contains("A_Parking"))
                    {
                        poiSearch.Conveniences.RemoveAll(x => x.Code == "A_Parking");
                        poiSearch.Conveniences.RemoveAll(x => x.Code == "A_GasStation");
                    }
                    else if (poiSearch.CategoryCodes.Contains("A_FoodAndBeverages"))
                    {
                        poiSearch.Conveniences.RemoveAll(x => x.Code == "A_FoodAndBeverages");
                    }
                    else
                    {

                    }

                    var _activeConveniences = pois.Conveniences?.Select(x => x.Code)?.ToList() ?? new List<string>();
                    if (_activeConveniences.Count > 0)
                    {
                        foreach(var c in poiSearch.Conveniences)
                        {
                            c.InActive = !_activeConveniences.Contains(c.Code);
                            if(!c.InActive && !poiSearch.CategoryCodes.Contains(c.Code))
                            {
                                poiSearch.CategoryCodes.Add(c.Code);
                            }
                        }
                    }

                    //if (!string.IsNullOrEmpty(pois.Ward))
                    //{
                    //    poiSearch.Address += $", {pois.Ward}";
                    //}
                    //if (!string.IsNullOrEmpty(pois.District))
                    //{
                    //    poiSearch.Address += $", {pois.District}";
                    //}
                    //if (!string.IsNullOrEmpty(pois.City))
                    //{
                    //    poiSearch.Address += $", {pois.City}";
                    //}

                    await _autoCompleteCol.InsertAsync(poiSearch);
                }
                _logger.LogInformation($"loaded redis skip {skip} size {pageSize}");
                skip ++;
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
