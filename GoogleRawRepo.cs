using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Platform.IOTHub.Repository.VMPOIRaw.Entities;
using Vietmap.NetCore.MongoDb;

namespace Platform.IOTHub.Repository.VMPOIRaw
{
    public class GoogleRawRepo : IGoogleRawRepo
    {
        private readonly ILogger<GoogleRawRepo> _logger;
        private readonly IMongoDbHelper _dbHelper;

        private readonly IMongoCollection<RawGoogle> _rawgoogleCol;
        private readonly IMongoCollection<RawThirdPartyData> _thirdpartydataCol;
        private readonly IMongoCollection<PlaceMappingTracking> _mappingTrackingdataCol;
        private readonly IMongoCollection<WazeMappingTracking> _wazeMappingTrackingCol;

        public GoogleRawRepo(ILogger<GoogleRawRepo> logger, IEnumerable<IMongoDbHelper> dbHelpers)
        {
            _logger = logger;
            _dbHelper = dbHelpers.FirstOrDefault(x => x.DatabaseName == RegisterGgDb.DatabaseName);

            _rawgoogleCol = _dbHelper.GetCollection<RawGoogle>("CrawlerGoogle");
            _thirdpartydataCol = _dbHelper.GetCollection<RawThirdPartyData>("raw_third_party");
            _mappingTrackingdataCol = _dbHelper.GetCollection<PlaceMappingTracking>("mapping_place_tracking");
            _wazeMappingTrackingCol = _dbHelper.GetCollection<WazeMappingTracking>("waze_mapping_place_tracking");
        }

        public async Task<IEnumerable<RawGoogle>> GetCrawlerGoogles(string[] placeIds)
        {
            return await _rawgoogleCol.Find(x => placeIds.Contains(x.placeId)).ToListAsync();
        }

        public async Task<IEnumerable<RawGoogle>> GetCrawlerGooglesBySearchText(string[] searchTexts)
        {
            return await _rawgoogleCol.Find(x => searchTexts.Contains(x.searchString)).ToListAsync();
        }

        public async Task<List<string>> UpsertCrawGoogleData(IEnumerable<BsonDocument> rawGGPlaces)
        {
            const string collectionName = "CrawlerGoogle";
            var placeIds = new List<string>();
            var bulkOperations = new List<WriteModel<BsonDocument>>();

            foreach (var newDocument in rawGGPlaces)
            {
                try
                {
                    if (!newDocument.Contains("placeId") || newDocument["placeId"].IsBsonNull)
                    {
                        _logger.LogWarning($"Document missing placeId: {newDocument.ToJson()}");
                        continue;
                    }

                    var placeId = newDocument["placeId"].AsString;
                    placeIds.Add(placeId);

                    var filter = Builders<BsonDocument>.Filter.Eq("placeId", placeId);
                    var existingDoc = await _dbHelper.GetCollection<BsonDocument>(collectionName)
                        .Find(filter)
                        .FirstOrDefaultAsync();

                    if (existingDoc == null)
                    {
                        // For new documents, just insert as is
                        bulkOperations.Add(new InsertOneModel<BsonDocument>(newDocument));
                    }
                    else
                    {
                        // For existing documents, merge reviews and update other fields
                        var mergedDoc = await MergeDocuments(existingDoc, newDocument);
                        bulkOperations.Add(new ReplaceOneModel<BsonDocument>(filter, mergedDoc));
                    }

                    // Process in batches of 1000 to avoid memory issues
                    if (bulkOperations.Count >= 1000)
                    {
                        await ProcessBulkOperations(bulkOperations, collectionName);
                        bulkOperations.Clear();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing document for bulk upsert: {Error}", ex.Message);
                }
            }

            // Process any remaining operations
            if (bulkOperations.Any())
            {
                await ProcessBulkOperations(bulkOperations, collectionName);
            }

            return placeIds;
        }

        private async Task<BsonDocument> MergeDocuments(BsonDocument existingDoc, BsonDocument newDoc)
        {
            try
            {
                // Create a copy of the new document as our base
                var mergedDoc = newDoc.DeepClone().AsBsonDocument;

                // If there are no existing reviews, just return the new document
                if (!existingDoc.Contains("reviews") || existingDoc["reviews"].IsBsonNull)
                {
                    return mergedDoc;
                }

                // If the new document doesn't have reviews, use the existing ones
                if (!newDoc.Contains("reviews") || newDoc["reviews"].IsBsonNull)
                {
                    mergedDoc["reviews"] = existingDoc["reviews"];
                    return mergedDoc;
                }

                // Merge reviews from both documents
                var existingReviews = existingDoc["reviews"].AsBsonArray;
                var newReviews = newDoc["reviews"].AsBsonArray;
                var mergedReviews = new BsonArray();
                var seenReviewIds = new HashSet<string>();

                // Add existing reviews to the merged set
                foreach (var review in existingReviews)
                {
                    if (review.AsBsonDocument.Contains("reviewId"))
                    {
                        var reviewId = review["reviewId"].AsString;
                        seenReviewIds.Add(reviewId);
                        mergedReviews.Add(review);
                    }
                }

                // Add new reviews that don't exist in the old document
                foreach (var review in newReviews)
                {
                    if (review.AsBsonDocument.Contains("reviewId"))
                    {
                        var reviewId = review["reviewId"].AsString;
                        if (!seenReviewIds.Contains(reviewId))
                        {
                            mergedReviews.Add(review);
                        }
                    }
                }

                // Update the reviews in the merged document
                mergedDoc["reviews"] = mergedReviews;

                // Update review-related metadata
                if (mergedReviews.Count > 0)
                {
                    // Update reviewsCount
                    mergedDoc["reviewsCount"] = mergedReviews.Count;

                    // Recalculate reviewsDistribution
                    var distribution = new BsonDocument
            {
                { "oneStar", CountReviewsByStars(mergedReviews, 1) },
                { "twoStar", CountReviewsByStars(mergedReviews, 2) },
                { "threeStar", CountReviewsByStars(mergedReviews, 3) },
                { "fourStar", CountReviewsByStars(mergedReviews, 4) },
                { "fiveStar", CountReviewsByStars(mergedReviews, 5) }
            };
                    mergedDoc["reviewsDistribution"] = distribution;

                    // Recalculate totalScore
                    double totalStars = 0;
                    foreach (var review in mergedReviews)
                    {
                        if (review.AsBsonDocument.Contains("stars"))
                        {
                            var starsValue = review["stars"];
                            // Handle both integer and double star ratings
                            totalStars += starsValue.IsInt32 ? starsValue.AsInt32 : starsValue.AsDouble;
                        }
                    }
                    mergedDoc["totalScore"] = Math.Round(totalStars / mergedReviews.Count, 1);
                }

                return mergedDoc;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error merging documents for placeId: {PlaceId}", newDoc["placeId"]);
                throw;
            }
        }

        private int CountReviewsByStars(BsonArray reviews, int stars)
        {
            return reviews.Count(r =>
            {
                if (!r.AsBsonDocument.Contains("stars")) return false;
                var starsValue = r["stars"];
                return starsValue.IsInt32 ?
                    starsValue.AsInt32 == stars :
                    Math.Round(starsValue.AsDouble) == stars;
            });
        }

        private async Task ProcessBulkOperations(List<WriteModel<BsonDocument>> operations, string collectionName)
        {
            try
            {
                var bulkWriteOptions = new BulkWriteOptions
                {
                    IsOrdered = false  // Continue processing remaining operations even if some fail
                };

                var result = await _dbHelper.GetCollection<BsonDocument>(collectionName)
                    .BulkWriteAsync(operations, bulkWriteOptions);

                _logger.LogInformation(
                    "Bulk write completed. Inserted: {InsertedCount}, Modified: {ModifiedCount}, Deleted: {DeletedCount}",
                    result.InsertedCount,
                    result.ModifiedCount,
                    result.DeletedCount);
            }
            catch (MongoBulkWriteException mbwex)
            {
                _logger.LogError(mbwex,
                    "Bulk write partially failed. Successful operations: {SuccessCount}, Failed operations: {FailedCount}",
                    mbwex.Data?.Keys?.Count ?? 0,
                    mbwex.WriteErrors?.Count ?? 0);

                foreach (var error in mbwex.WriteErrors ?? new List<BulkWriteError>())
                {
                    _logger.LogError("Write error at index {Index}: {Message}", error.Index, error.Message);
                }

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error during bulk write operation");
                throw;
            }
        }
        public async Task UpSertRawThirdPartyData(RawThirdPartyData poi)
        {
            var rawData = await _thirdpartydataCol.Find(x => x.Id == poi.Id).FirstOrDefaultAsync();
            if (rawData == null)
            {
                await _thirdpartydataCol.InsertOneAsync(poi);
            }
            else
            {
                if (poi.Rating != null)
                {
                    rawData.Rating = poi.Rating;
                }

                if (poi.vinfastChargerStation != null)
                {
                    rawData.vinfastChargerStation = poi.vinfastChargerStation;
                }

                if (poi.MappingPlaces != null)
                {
                    rawData.MappingPlaces = poi.MappingPlaces;
                }

                if (poi.VenueMapping != null)
                {
                    rawData.VenueMapping = poi.VenueMapping;
                }

                await _thirdpartydataCol.ReplaceOneAsync(x => x.Id == poi.Id, rawData, new ReplaceOptions { IsUpsert = true });
            }
        }

        public async Task<PlaceMappingTracking> GetMappingTracking(long vmId)
        {
            return await _mappingTrackingdataCol.Find(x => x.VmId == vmId).FirstOrDefaultAsync();
        }

        public async Task UpsertMappingTracking(PlaceMappingTracking tracking)
        {
            var maptracking = await _mappingTrackingdataCol.Find(x => x.VmId == tracking.VmId).FirstOrDefaultAsync();
            if(maptracking == null)
            {
                tracking.Createddate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _mappingTrackingdataCol.InsertOneAsync(tracking);
            }
            else
            {
                tracking.Lastmodified = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _mappingTrackingdataCol.ReplaceOneAsync(x => x.Id == tracking.Id, tracking);
            }
        }

        public async Task<int> GetTotalInvalidMapping()
        {
            var count = await _mappingTrackingdataCol.Find(x  => !x.IsIgnoreMapping || x.IsReRunGGSearch).CountDocumentsAsync();
            return (int)count;
        }

        public async Task<IEnumerable<PlaceMappingTracking>> GetInvalidMappings(int skip, int pageSize)
        {
            var currenttimeUnix = DateTimeOffset.UtcNow.AddHours(-48).ToUnixTimeSeconds();
            return await _mappingTrackingdataCol
                .Find(x => !x.IsIgnoreMapping || x.IsReRunGGSearch)
                .Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<PlaceMappingTracking>> GetMappingTrackings(IEnumerable<string> searchTexts)
        {
            return await _mappingTrackingdataCol.Find(x => searchTexts.Contains(x.SearchText)).ToListAsync();
        }


        public async Task<int> GetTotalReGGMapping()
        {
            var count = await _mappingTrackingdataCol.Find(x => x.IsReRunGGSearch).CountDocumentsAsync();
            return (int)count;
        }

        public async Task<IEnumerable<PlaceMappingTracking>> GetReGGMappings(int skip, int pageSize)
        {
            return await _mappingTrackingdataCol
                .Find(x => x.IsReRunGGSearch)
                .Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<PlaceMappingTracking>> GetMappingTrackings(IEnumerable<long> vmIds)
        {
            return await _mappingTrackingdataCol.Find(x => vmIds.Contains(x.VmId)).ToListAsync();
        }

        public async Task UpsertWazeMappingTracking(WazeMappingTracking tracking)
        {
            var maptracking = await _wazeMappingTrackingCol.Find(x => x.WazeId == tracking.WazeId).FirstOrDefaultAsync();
            if (maptracking == null)
            {
                tracking.Createddate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _wazeMappingTrackingCol.InsertOneAsync(tracking);
            }
            else
            {
                tracking.Id = maptracking.Id;
                tracking.Lastmodified = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _wazeMappingTrackingCol.ReplaceOneAsync(x => x.WazeId == tracking.WazeId, tracking);
            }
        }

        public async Task<int> GetTotalInvalidWazeMapping()
        {
            var count = await _wazeMappingTrackingCol.Find(x => !x.IsIgnoreMapping && x.IsReRunGGSearch).CountDocumentsAsync();
            return (int)count;
        }

        public async Task<IEnumerable<WazeMappingTracking>> GetInvalidWazeMappings(int skip, int pageSize)
        {
            return await _wazeMappingTrackingCol
                .Find(x => !x.IsIgnoreMapping && x.IsReRunGGSearch)
                .Skip(skip * pageSize).Limit(pageSize).ToListAsync();
        }

        public async Task<IEnumerable<WazeMappingTracking>> GetWazeMappingTrackings(IEnumerable<string> searchTexts)
        {
            return await _wazeMappingTrackingCol.Find(x => searchTexts.Contains(x.WazeAddress)).ToListAsync();
        }

        public async Task<IEnumerable<WazeMappingTracking>> GetWazeMappingTrackingsByPlaceIds(IEnumerable<string> placeIds)
        {
            return await _wazeMappingTrackingCol.Find(x => placeIds.Contains(x.WazeId)).ToListAsync();
        }
    }
}
