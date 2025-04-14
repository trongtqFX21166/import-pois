using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using Platform.IOTHub.ImportPOI.Service.Models.PeakApi;
using Platform.IOTHub.Repository.POI.Repositories;
using Platform.IOTHub.Repository.VMPOIRaw;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Platform.IOTHub.Utility;
using System.Text;

namespace Platform.IOTHub.ImportPOI.Service
{
    /// <summary>
    /// Background service that reads crawler_vfstation data and updates it from VinFast API
    /// </summary>
    public class WorkerSyncVinfastStationService : BackgroundService
    {
        private readonly ILogger<WorkerSyncVinfastStationService> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IVinfastStationRepo _vfStationRepo;
        private readonly HttpClient _client;
        private readonly IConfiguration _configuration;

        public WorkerSyncVinfastStationService(
            ILogger<WorkerSyncVinfastStationService> logger,
            IServiceProvider serviceProvider,
            IVinfastStationRepo vfStationRepo,
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _vfStationRepo = vfStationRepo;
            _client = httpClientFactory.CreateClient("PeakApi");
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                _logger.LogInformation("Starting VinFast station sync service");

                // Step 1: Get all crawler_vfstation entries that need updating
                // We'll use a timestamp-based approach to only update stations that haven't been updated recently
                int hoursThreshold = _configuration.GetValue<int>("VinfastSync:HoursThreshold", 6); // Default 6 hours
                var cutoffTime = DateTimeOffset.UtcNow.AddHours(-hoursThreshold).ToUnixTimeSeconds();

                int skip = 0;
                int pageSize = 100;
                int totalProcessed = 0;
                int totalUpdated = 0;
                int totalFailed = 0;

                while (!stoppingToken.IsCancellationRequested)
                {
                    // Get stations to update - either never updated or updated before our cutoff time
                    var stations = await _vfStationRepo.GetStationsToUpdate(cutoffTime, skip, pageSize);
                    if (stations == null || !stations.Any())
                    {
                        _logger.LogInformation("No more stations to update");
                        break;
                    }

                    _logger.LogInformation($"Processing batch of {stations.Count()} stations (skip: {skip}, pageSize: {pageSize})");

                    foreach (var station in stations)
                    {
                        totalProcessed++;
                        try
                        {
                            // Step 2: Call VinFast API to get updated station information
                            var updatedStation = await FetchUpdatedStationData(station);

                            if (updatedStation != null)
                            {
                                // Step 3: Update the local database with the fresh information
                                station.LastSyncTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                station.LastSyncStatus = "success";

                                // Update station properties with fresh data
                                station.stationName = updatedStation.stationName;
                                station.stationAddress = updatedStation.stationAddress;
                                station.latitude = updatedStation.latitude;
                                station.longitude = updatedStation.longitude;

                                // Update charging powers information
                                if (updatedStation.evsePowers != null && updatedStation.evsePowers.Any())
                                {
                                    station.evsePowers = updatedStation.evsePowers;
                                }

                                // Update images if available
                                if (updatedStation.images != null && updatedStation.images.Any())
                                {
                                    station.images = updatedStation.images;
                                }

                                // Save the updated station data back to the database
                                await _vfStationRepo.UpdateVfStation(station);
                                totalUpdated++;

                                _logger.LogInformation($"Successfully updated VinFast station: {station.locationId} - {station.stationName}");
                            }
                            else
                            {
                                station.LastSyncTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                station.LastSyncStatus = "failed";
                                await _vfStationRepo.UpdateVfStation(station);
                                totalFailed++;

                                _logger.LogWarning($"Failed to get updated data for VinFast station: {station.locationId}");
                            }
                        }
                        catch (Exception ex)
                        {
                            totalFailed++;
                            _logger.LogError(ex, $"Error updating VinFast station {station.locationId}: {ex.Message}");

                            try
                            {
                                station.LastSyncTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                station.LastSyncStatus = $"error: {ex.Message}";
                                await _vfStationRepo.UpdateVfStation(station);
                            }
                            catch (Exception updateEx)
                            {
                                _logger.LogError(updateEx, $"Failed to update status for station {station.locationId}");
                            }
                        }

                        // Throttle requests to avoid overwhelming the API
                        await Task.Delay(500, stoppingToken);
                    }

                    skip += pageSize;

                    // Log progress after each batch
                    _logger.LogInformation($"Progress: Processed {totalProcessed} stations, Updated {totalUpdated}, Failed {totalFailed}");
                }

                _logger.LogInformation($"VinFast station sync complete. Total processed: {totalProcessed}, Updated: {totalUpdated}, Failed: {totalFailed}");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Fatal error in VinFast station sync service: {ex.Message}");
                Environment.Exit(1);
            }
        }

        private async Task<VinfastChargerStation?> FetchUpdatedStationData(VinfastChargerStation station)
        {
            int retryCount = _configuration.GetValue<int>("VinfastSync:ApiRetries", 3);
            VinfastChargerStation? result = null;

            while (retryCount > 0)
            {
                try
                {
                    if (string.IsNullOrEmpty(station.locationId))
                    {
                        _logger.LogWarning($"Station has empty locationId, cannot fetch updated data");
                        return null;
                    }

                    // Create location info request with the locationId
                    var request = new LocationInfoRequest
                    {
                        locationIds = new List<string> { station.locationId }
                    };

                    // Call VinFast API via Peak API proxy
                    var response = await _client.PostAsync(
                        "/api/vf/location-info",
                        new StringContent(JsonConvert.SerializeObject(request), Encoding.UTF8, "application/json")
                    );

                    if (response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        if (string.IsNullOrEmpty(content))
                        {
                            _logger.LogWarning($"Empty response from location-info API for locationId: {station.locationId}");
                            retryCount--;
                            continue;
                        }

                        var locationResponse = JsonConvert.DeserializeObject<LocationInfoResponse>(content);

                        if (locationResponse == null ||
                            locationResponse.code != 200000 ||
                            locationResponse.data == null ||
                            !locationResponse.data.Any())
                        {
                            _logger.LogWarning($"No data returned from location-info API for locationId: {station.locationId}, response code: {locationResponse?.code}");
                            retryCount--;
                            continue;
                        }

                        // Get the station data
                        var stationData = locationResponse.data.FirstOrDefault();
                        if (stationData == null)
                        {
                            _logger.LogWarning($"No station data in the response for locationId: {station.locationId}");
                            retryCount--;
                            continue;
                        }

                        // Convert the API response to BsonDocument to store in crawler_vfstation
                        //var bsonDoc = stationData.ToBsonDocument();
                        //await _vfStationRepo.UpsertCrawlerData(new List<MongoDB.Bson.BsonDocument> { bsonDoc });

                        // Map the API response to our VinfastChargerStation model
                        result = new VinfastChargerStation
                        {
                            locationId = stationData.locationId,
                            stationName = stationData.stationName,
                            stationAddress = stationData.stationAddress,
                            latitude = stationData.latitude,
                            longitude = stationData.longitude,
                            totalEvse = stationData.totalEvse,
                            numberOfAvailableEvse = stationData.numberOfAvailableEvse,
                            evsePowers = stationData.evsePowers?.Select(e => new evsePower
                            {
                                type = e.type,
                                totalEvse = e.totalEvse,
                                numberOfAvailableEvse = e.numberOfAvailableEvse
                            }).ToList() ?? new List<evsePower>(),
                            images = stationData.images?.Select(i => new VfChargerImage
                            {
                                url = i.url,
                                thumbnail = i.thumbnail
                            }).ToList() ?? new List<VfChargerImage>(),
                        };

                        _logger.LogInformation($"Successfully fetched updated data for station: {station.locationId} - {result.stationName}");
                        break;
                    }
                    else
                    {
                        _logger.LogWarning($"API request failed with status code: {response.StatusCode} for locationId: {station.locationId}");
                        retryCount--;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error fetching VinFast station data for {station.locationId}: {ex.Message}");
                    retryCount--;
                }
            }

            return result;
        }
    }
}