
function updateCateName() {
	
	const bulkInsert = db.mapping_place_tracking.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.mapping_place_tracking.find({});

	// Process each document
	documents.forEach(doc => {
		var ggplaceId = doc.GgPlaceId;
		if (ggplaceId) {
			ggplaceId = ggplaceId.replace("googlePlaces.", "");

			const ggRaw = db.CrawlerGoogle.findOne({
				placeId: ggplaceId
			});

			if (ggRaw) {
				doc.GgCategoryName = ggRaw.categoryName;
				// Upsert into mapping_place_tracking Document
				bulkInsert.find({ _id: doc._id }).upsert().replaceOne(doc);
			}
		}
		
	});

	// Execute the bulk operation
	bulkInsert.execute();

	return true;
}

updateCateName();