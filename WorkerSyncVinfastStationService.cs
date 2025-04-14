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
using System.Collections.Concurrent;
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
                int batchSize = 10; // Process 10 stations in parallel
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

                    // Process stations in batches of 10
                    for (int i = 0; i < stations.Count(); i += batchSize)
                    {
                        var batchStations = stations.Skip(i).Take(batchSize).ToList();
                        totalProcessed += batchStations.Count;

                        try
                        {
                            // Step 2: Fetch data for all stations in the batch with a single API call
                            var locationIds = batchStations.Select(s => s.locationId).Where(id => !string.IsNullOrEmpty(id)).ToList();
                            if (!locationIds.Any())
                            {
                                _logger.LogWarning("Batch contains only stations with empty locationIds, skipping");
                                totalFailed += batchStations.Count;
                                continue;
                            }

                            var updatedStationsData = await FetchBatchStationData(locationIds);

                            // Process each station with its updated data
                            foreach (var station in batchStations)
                            {
                                try
                                {
                                    // Find the matching updated station data
                                    var updatedStation = updatedStationsData?.FirstOrDefault(s => s.locationId == station.locationId);

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
                                        station.totalEvse = updatedStation.totalEvse;
                                        station.numberOfAvailableEvse = updatedStation.numberOfAvailableEvse;

                                        // Update charging powers information
                                        if (updatedStation.evsePowers != null && updatedStation.evsePowers.Any())
                                        {
                                            station.evsePowers = updatedStation.evsePowers.Select(e => new evsePower
                                            {
                                                type = e.type,
                                                totalEvse = e.totalEvse,
                                                numberOfAvailableEvse = e.numberOfAvailableEvse
                                            }).ToList();
                                        }

                                        // Update images if available
                                        if (updatedStation.images != null && updatedStation.images.Any())
                                        {
                                            station.images = updatedStation.images.Select(i => new VfChargerImage
                                            {
                                                url = i.url,
                                                thumbnail = i.thumbnail
                                            }).ToList();
                                        }

                                        // Save the updated station data back to the database
                                        await _vfStationRepo.UpdateVfStation(station);
                                        totalUpdated++;

                                        _logger.LogInformation($"Successfully updated VinFast station: {station.locationId} - {station.stationName}");
                                    }
                                    else
                                    {
                                        station.LastSyncTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                        station.LastSyncStatus = "failed - no data returned";
                                        await _vfStationRepo.UpdateVfStation(station);
                                        totalFailed++;

                                        _logger.LogWarning($"No data returned for VinFast station: {station.locationId}");
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
                            }
                        }
                        catch (Exception batchEx)
                        {
                            totalFailed += batchStations.Count;
                            _logger.LogError(batchEx, $"Error processing batch of {batchStations.Count} stations: {batchEx.Message}");

                            // Update each station with the error
                            foreach (var station in batchStations)
                            {
                                try
                                {
                                    station.LastSyncTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                                    station.LastSyncStatus = $"batch error: {batchEx.Message}";
                                    await _vfStationRepo.UpdateVfStation(station);
                                }
                                catch (Exception updateEx)
                                {
                                    _logger.LogError(updateEx, $"Failed to update status for station {station.locationId}");
                                }
                            }
                        }

                        // Log progress after each batch
                        _logger.LogInformation($"Progress: Processed {totalProcessed} stations, Updated {totalUpdated}, Failed {totalFailed}");

                        // Throttle between batches to avoid overwhelming API
                        await Task.Delay(500, stoppingToken);
                    }

                    skip += pageSize;
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

        private async Task<List<StationData>> FetchBatchStationData(List<string> locationIds)
        {
            int retryCount = _configuration.GetValue<int>("VinfastSync:ApiRetries", 3);

            while (retryCount > 0)
            {
                try
                {
                    // Create location info request with multiple locationIds
                    var request = new LocationInfoRequest
                    {
                        locationIds = locationIds
                    };

                    _logger.LogInformation($"Fetching data for {locationIds.Count} stations in a single API call");

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
                            _logger.LogWarning($"Empty response from location-info API for batch request");
                            retryCount--;
                            continue;
                        }

                        var locationResponse = JsonConvert.DeserializeObject<LocationInfoResponse>(content);

                        if (locationResponse == null ||
                            locationResponse.code != 200000 ||
                            locationResponse.data == null)
                        {
                            _logger.LogWarning($"Invalid response from location-info API, response code: {locationResponse?.code}");
                            retryCount--;
                            continue;
                        }

                        if (!locationResponse.data.Any())
                        {
                            _logger.LogWarning($"No data returned from location-info API for any of the requested stations");
                            return new List<StationData>();
                        }

                        _logger.LogInformation($"Successfully fetched data for {locationResponse.data.Count} stations out of {locationIds.Count} requested");
                        return locationResponse.data;
                    }
                    else
                    {
                        _logger.LogWarning($"API request failed with status code: {response.StatusCode} for batch request");
                        retryCount--;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error fetching VinFast station data for batch of {locationIds.Count} stations: {ex.Message}");
                    retryCount--;
                }
            }

            return new List<StationData>();
        }
    }

    public class LocationInfoRequest
    {
        public List<string> locationIds { get; set; }
    }

    public class LocationInfoResponse
    {
        public int code { get; set; }
        public string message { get; set; }
        public List<StationData> data { get; set; }
    }

    public class StationData
    {
        public string locationId { get; set; }
        public string stationName { get; set; }
        public string stationAddress { get; set; }
        public double latitude { get; set; }
        public double longitude { get; set; }
        public int totalEvse { get; set; }
        public int numberOfAvailableEvse { get; set; }
        public List<EvsePower> evsePowers { get; set; }
        public List<StationImage> images { get; set; }
    }

    public class EvsePower
    {
        public int type { get; set; }
        public int totalEvse { get; set; }
        public int numberOfAvailableEvse { get; set; }
    }

    public class StationImage
    {
        public string url { get; set; }
        public string thumbnail { get; set; }
    }
}