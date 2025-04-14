function updateEntryPointPOIs() {
    try {
        // Function to generate random number between 1 and 999999
        function getRandomNumber() {
            return Math.floor(Math.random() * 999999) + 1;
        }

        // Switch to VmlDb and get POIs
        vmlDb = db.getSiblingDB('VmlDb');
        var pois = vmlDb.ReplaceMapPoiDB.find({ layer: "Entry_point" }).toArray();
        var poiVmIds = pois.map(function (poi) {
            return NumberInt(poi.vmId);
        });

        print("Found " + pois.length + " POIs to process");

        // Delete existing POIs
        var deleteResult = db.entry_pois.deleteMany({
            VMParentId: { $in: poiVmIds }
        });

        print("Deleted " + deleteResult.deletedCount + " documents from entry_pois");

        // Transform POIs
        var transformedPois = pois.map(function (poi) {
            return {
                VMId: NumberInt(parseInt(poi.vmId) + 100000000 + poi.randomIndex),
                VMParentId: NumberInt(poi.vmId),
                Lat: poi.coordinates[0],
                Lng: poi.coordinates[1],
                Name: poi.name,
                Address: ""
            };
        });

        // Insert transformed POIs
        if (transformedPois.length > 0) {
            var insertResult = db.entry_pois.insertMany(transformedPois);
            print("Inserted " + insertResult.insertedCount + " new POIs into entry_pois");
        }

        print("\nUpdate completed successfully!");
        print("Summary:");
        print("- Total POIs processed: " + pois.length);
        print("- Documents deleted: " + deleteResult.deletedCount);
        print("- New documents inserted: " + transformedPois.length);

    } catch (error) {
        print("Error occurred: " + error.message);
    }
}

// Run the function
updateEntryPointPOIs();