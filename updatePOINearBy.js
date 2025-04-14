// updatePOINearBy.js
function prepareUpdateDocument(doc) {
    if (!doc) return null;
    const updateDoc = { ...doc };
    delete updateDoc._id;
    return updateDoc;
}

function createUpdateLog(stats, details) {
    return {
        timestamp: new Date(),
        operation: "POI_Nearby Update",
        statistics: stats,
        details: details,
        processingTime: new Date() - stats.startTime
    };
}

function createStatusHistory(poiId, type, status, details) {
    return {
        poiId: poiId,
        type: type,
        status: status,
        details: details,
        timestamp: new Date(),
        version: new Date().toISOString().split('T')[0] // YYYY-MM-DD format
    };
}

function processDiffUpdates() {
    const stats = {
        startTime: new Date(),
        updates: 0,
        deletes: 0,
        inserts: 0,
        errors: 0,
        details: []
    };

    // Create collection for status history if it doesn't exist
    if (!db.poi_status_history) {
        db.createCollection("poi_status_history");
        db.poi_status_history.createIndex({ poiId: 1, timestamp: -1 });
    }

    db.POI_Nearby_Diff.find().forEach(diff => {
        try {
            const poiId = parseInt(diff.Id);
            const operation = {
                id: poiId,
                type: diff.Type,
                status: 'success'
            };

            switch (diff.Type.toLowerCase()) {
                case 'update':
                    const updatedDoc = db.POI_Nearby_Raw.findOne({ Id: poiId });
                    if (updatedDoc) {
                        const prepared = prepareUpdateDocument(updatedDoc);
                        const updateResult = db.POI_Nearby.updateOne(
                            { Id: poiId },
                            {
                                $set: {
                                    ...prepared,
                                    status: 'update',
                                    lastUpdateDate: new Date()
                                }
                            },
                            { upsert: true }
                        );
                        stats.updates++;
                        operation.changes = diff.Names || 'Full update';

                        // Record status history
                        db.poi_status_history.insertOne(createStatusHistory(
                            poiId,
                            'update',
                            'success',
                            { changes: diff.Names || 'Full update' }
                        ));
                    } else {
                        operation.status = 'error';
                        operation.error = 'Raw document not found';
                    }
                    break;

                case 'delete':
                    const deleteResult = db.POI_Nearby.updateOne(
                        { Id: poiId },
                        {
                            $set: {
                                status: 'delete',
                                deletedDate: new Date()
                            }
                        }
                    );
                    stats.deletes += deleteResult.modifiedCount;
                    operation.result = deleteResult;

                    // Record status history
                    db.poi_status_history.insertOne(createStatusHistory(
                        poiId,
                        'delete',
                        'success',
                        { deletedDate: new Date() }
                    ));
                    break;

                case 'new':
                    const newDoc = db.POI_Nearby_Raw.findOne({ Id: poiId });
                    if (newDoc) {
                        const prepared = prepareUpdateDocument(newDoc);
                        prepared.status = 'new';
                        prepared.createdDate = new Date();
                        db.POI_Nearby.insertOne(prepared);
                        stats.inserts++;

                        // Record status history
                        db.poi_status_history.insertOne(createStatusHistory(
                            poiId,
                            'new',
                            'success',
                            { createdDate: prepared.createdDate }
                        ));
                    } else {
                        operation.status = 'error';
                        operation.error = 'Raw document not found';
                    }
                    break;
            }
            stats.details.push(operation);
        } catch (error) {
            stats.errors++;
            stats.details.push({
                id: diff.Id,
                type: diff.Type,
                status: 'error',
                error: error.message
            });

            // Record error in status history
            db.poi_status_history.insertOne(createStatusHistory(
                parseInt(diff.Id),
                diff.Type.toLowerCase(),
                'error',
                { error: error.message }
            ));
        }
    });

    const updateLog = createUpdateLog(stats, {
        totalProcessed: stats.updates + stats.deletes + stats.inserts,
        summary: `Processed ${stats.updates} updates, ${stats.inserts} inserts, ${stats.deletes} deletes with ${stats.errors} errors`
    });

    db.update_logs.insertOne(updateLog);
    return updateLog;
}

// Main execution
try {
    print("Starting POI update process...");
    const updateLog = processDiffUpdates();
    printjson(updateLog);
} catch (error) {
    const errorLog = {
        timestamp: new Date(),
        operation: "POI_Nearby Update",
        status: "error",
        error: error.message,
        stack: error.stack
    };
    db.update_logs.insertOne(errorLog);
    printjson(errorLog);
    throw error;
}