using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Platform.IOTHub.Repository.POI.Entities;
using Platform.IOTHub.Repository.POI.Models;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;

namespace Platform.IOTHub.ImportPOI.Service
{
    public class WorkerAddggData : BackgroundService
    {
        private readonly ILogger<WorkerAddggData> _logger;
        private readonly IServiceProvider _serviceProvider;


        private readonly IGoogleRawRepo _ggRawRepository;

        private readonly IConfiguration _configuration;

        public WorkerAddggData(ILogger<WorkerAddggData> logger
            , IServiceProvider serviceProvider
            , IGoogleRawRepo ggRawRepository
            , IConfiguration configuration)
        {
            _logger = logger;

            _serviceProvider = serviceProvider;
            _ggRawRepository = ggRawRepository;


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
                int pageSize = 100;
                while (!stoppingToken.IsCancellationRequested)
                {
                    var pois = await poiRepository.GetPOIs(new Guid[] { Guid.Parse("d9c03a5c-51d8-42de-bb77-7cff5ff5fc0b") }, new Guid[] { }, skip, pageSize); 
                    if (!pois.Any())
                    {
                        break;
                    }

                    var placeIds = pois.Where(x => !string.IsNullOrEmpty(x.GGplaceId))
                        .Select(x => x.GGplaceId.Replace("googlePlaces.","")).Distinct().ToArray();
                    if(placeIds.Length > 0)
                    {
                        var rawDatas = await _ggRawRepository.GetCrawlerGoogles(placeIds);
                        foreach(var rawData in rawDatas)
                        {
                            var poi = pois.FirstOrDefault(p => p.GGplaceId == $"googlePlaces.{rawData.placeId}");
                            if(poi != null)
                            {
                                var updatePoi = new UpdatePartyData
                                {
                                    Id = poi.Id,
                                    PartyImages = new List<Repository.POI.Entities.PartyImage>(),

                                };

                                if (!string.IsNullOrEmpty(rawData.imageUrl))
                                {
                                    updatePoi.PartyImages.Add(new PartyImage
                                    {
                                        Name = "Main",
                                        ImageUrl = rawData.imageUrl,
                                    });
                                }

                                if (rawData.imageUrls.Count > 0)
                                {
                                    for (int i = 0; i < rawData.imageUrls.Count; i++)
                                    {
                                        updatePoi.PartyImages.Add(new PartyImage
                                        {
                                            Name = $"{i}",
                                            ImageUrl = rawData.imageUrls[i],
                                        });
                                    }
                                }

                                updatePoi.PartyRating = rawData.reviewsCount > 0 ? new PartyRating
                                {
                                    AverageRating = rawData.totalScore ?? 0,
                                    TotalReviews = rawData.reviewsCount ?? 0,
                                } : null;

                                await poiRepository.UpdatePartyData(updatePoi);

                                //if (rawData.additionalInfo != null && rawData.additionalInfo?.Amenities != null)
                                //{
                                //    //todo: add sub 
                                //}
                            }
                        }
                    }
                    
                    

                    _logger.LogInformation($"add gg data skip {skip} size {pageSize}");
                    skip++;
                }
                _logger.LogInformation("Done");
                Environment.Exit(0);
            }catch(Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                Environment.Exit(1);
            }
        }



        //private async Task AddRatingData(IEnumerable<RawPOI> rawPois)
        //{
        //    try
        //    {
                
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, ex.Message);
        //    }
        //}

        //private async Task<WazeLoginReponse?> login()
        //{
        //    var response = await _client.GetAsync($"api/wz/rt-login?hex_data={_configuration["PeakApi:HexData"]}");
        //    if(response.IsSuccessStatusCode)
        //    {
        //        var content = await response.Content.ReadAsStringAsync();
        //        return string.IsNullOrWhiteSpace(content) ? null : JsonConvert.DeserializeObject<WazeLoginReponse>(content);
        //    }
        //    return null;
        //}

        //private async Task<PlaceDetailResposne?> getRating(string serverId, int sequence, string uid, List<string> placeIds)
        //{
        //    var request = new
        //    {
        //        place_ids = placeIds
        //    };

        //    var response = await _client.PostAsync("/api/wz/place/details/batch"
        //        , new StringContent(JsonConvert.SerializeObject(request, Formatting.None), Encoding.UTF8, "application/json"));

        //    if(response.IsSuccessStatusCode)
        //    {
        //        var content = await response.Content.ReadAsStringAsync();
        //        var rspData = JsonConvert.DeserializeObject<PlaceDetailResposne>(content);

        //        return rspData?.data == null ? null : rspData;
        //    }

        //    return null;
        //}
    }
}
