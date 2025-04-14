// updateEntryPOINearBy.js
function prepareUpdateDocument(doc) {
    if (!doc) return null;
    const updateDoc = { ...doc };
    delete updateDoc._id;
    return updateDoc;
}

function createUpdateLog(stats) {
    return {
        timestamp: new Date(),
        operation: "EntryPoint_Nearby Update",
        statistics: {
            new: stats.new,
            updated: stats.update,
            deleted: stats.delete,
            errors: stats.errors,
            missingParentPois: stats.missingParentPoi.length
        },
        details: {
            missingParentPois: stats.missingParentPoi,
            summary: `Processed ${stats.new} new entries, ${stats.update} updates, ${stats.delete} deletes with ${stats.errors} errors`
        },
        processingTime: new Date() - stats.startTime
    };
}

function createStatusHistory(entryId, type, status, details) {
    return {
        entryId: entryId,
        type: type,
        status: status,
        details: details,
        timestamp: new Date(),
        version: new Date().toISOString().split('T')[0] // YYYY-MM-DD format
    };
}

function processEntryPoiUpdates() {
    const stats = {
        startTime: new Date(),
        new: 0,
        update: 0,
        delete: 0,
        errors: 0,
        processedIds: [],
        missingParentPoi: [],
        errorDetails: []
    };

    // Create collection for status history if it doesn't exist
    if (!db.entry_poi_status_history) {
        db.createCollection("entry_poi_status_history");
        db.entry_poi_status_history.createIndex({ entryId: 1, timestamp: -1 });
    }

    const diffRecords = db.EntryPoint_Nearby_Diff.find({}).toArray();
    print(`Found ${diffRecords.length} records to process`);

    diffRecords.forEach(diffRecord => {
        try {
            const operationResult = {
                id: diffRecord.Id,
                type: diffRecord.Type,
                status: 'success'
            };

            switch (diffRecord.Type.toLowerCase()) {
                case 'new':
                    const newEntry = db.EntryPoint_Nearby_Raw.findOne({ Id: parseInt(diffRecord.Id) });
                    if (!newEntry) {
                        throw new Error('Raw data not found');
                    }

                    const newParentExists = db.POI_Nearby.findOne({ Id: newEntry.ParentId });
                    if (!newParentExists) {
                        stats.missingParentPoi.push({
                            entryId: newEntry.Id,
                            parentId: newEntry.ParentId
                        });
                        throw new Error('Parent POI not found');
                    }

                    const preparedNew = prepareUpdateDocument(newEntry);
                    preparedNew.status = 'new';
                    preparedNew.createdDate = new Date();
                    db.EntryPoint_Nearby.insertOne(preparedNew);
                    stats.new++;

                    // Record status history
                    db.entry_poi_status_history.insertOne(createStatusHistory(
                        newEntry.Id,
                        'new',
                        'success',
                        { createdDate: preparedNew.createdDate }
                    ));
                    break;

                case 'update':
                    const updateEntry = db.EntryPoint_Nearby_Raw.findOne({ Id: parseInt(diffRecord.Id) });
                    if (!updateEntry) {
                        throw new Error('Raw data not found');
                    }

                    const updateParentExists = db.POI_Nearby.findOne({ Id: updateEntry.ParentId });
                    if (!updateParentExists) {
                        stats.missingParentPoi.push({
                            entryId: updateEntry.Id,
                            parentId: updateEntry.ParentId
                        });
                        throw new Error('Parent POI not found');
                    }

                    const preparedUpdate = prepareUpdateDocument(updateEntry);
                    db.EntryPoint_Nearby.updateOne(
                        { Id: updateEntry.Id },
                        {
                            $set: {
                                ...preparedUpdate,
                                status: 'update',
                                lastUpdateDate: new Date()
                            }
                        }
                    );
                    stats.update++;

                    // Record status history
                    db.entry_poi_status_history.insertOne(createStatusHistory(
                        updateEntry.Id,
                        'update',
                        'success',
                        { lastUpdateDate: new Date() }
                    ));
                    break;

                case 'delete':
                    const deleteResult = db.EntryPoint_Nearby.updateOne(
                        { Id: parseInt(diffRecord.Id) },
                        {
                            $set: {
                                status: 'delete',
                                deletedDate: new Date()
                            }
                        }
                    );
                    if (deleteResult.modifiedCount === 1) {
                        stats.delete++;

                        // Record status history
                        db.entry_poi_status_history.insertOne(createStatusHistory(
                            parseInt(diffRecord.Id),
                            'delete',
                            'success',
                            { deletedDate: new Date() }
                        ));
                    } else {
                        throw new Error('Entry not found for deletion');
                    }
                    break;
            }

            stats.processedIds.push(diffRecord.Id);
            operationResult.status = 'success';

        } catch (error) {
            stats.errors++;
            stats.errorDetails.push({
                id: diffRecord.Id,
                type: diffRecord.Type,
                error: error.message
            });

            // Record error in status history
            db.entry_poi_status_history.insertOne(createStatusHistory(
                parseInt(diffRecord.Id),
                diffRecord.Type.toLowerCase(),
                'error',
                { error: error.message }
            ));
        }
    });

    const updateLog = createUpdateLog(stats);
    db.update_logs.insertOne(updateLog);
    return updateLog;
}

// Main execution
try {
    print("Starting entry POI update process...");
    const updateLog = processEntryPoiUpdates();
    printjson(updateLog);
} catch (error) {
    const errorLog = {
        timestamp: new Date(),
        operation: "EntryPoint_Nearby Update",
        status: "error",
        error: error.message,
        stack: error.stack
    };
    db.update_logs.insertOne(errorLog);
    printjson(errorLog);
    throw error;
}