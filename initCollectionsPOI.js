function loadCategory() {
    // Convert category codes to string in existing data
    db.categories.find().forEach(function (doc) {
        db.categories.updateOne(
            { _id: doc._id },
            { $set: { Code: doc.Code.toString() } }
        );
    });

    const bulkInsert = db.catgory.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    // Process each document
    documents.forEach(doc => {
        if (!doc.CatIds) return;

        const cat = db.categories.findOne({
            Code: doc.CatIds
        });

        if (cat) {
            const element = {
                CatIds: cat.Code,
                CatName: cat.Sub === "" ? cat.Main : cat.Sub,
                CreatedAt: new Date(),
                UpdatedAt: new Date()
            };

            bulkInsert.find({ CatIds: element.CatIds }).upsert().replaceOne(element);
        }
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadCategory', result });
    return true;
}

function loadBrands() {
    const bulkInsert = db.brands.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    documents.forEach(doc => {
        if (!doc.CatIds) return;

        const cat = db.catgory.findOne({
            CatIds: doc.CatIds
        });

        if (cat && doc.Names?.vi?.BrandName) {
            const element = {
                ChainName: doc.Names.vi.BrandName,
                CatIds: cat.CatIds,
                CatName: cat.CatName,
                CreatedAt: new Date(),
                UpdatedAt: new Date()
            };

            bulkInsert.find({
                ChainName: element.ChainName,
                CatIds: element.CatIds
            }).upsert().replaceOne(element);
        }
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadBrands', result });
    return true;
}

function loadBranch() {
    const bulkInsert = db.branchs.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    documents.forEach(doc => {
        if (!doc.CatIds || !doc.Names?.vi?.Branch) return;

        const cat = db.catgory.findOne({
            CatIds: doc.CatIds
        });

        if (cat) {
            const brand = db.brands.findOne({
                ChainName: doc.Names.vi.BrandName
            });

            if (!brand) return;

            const element = {
                BranchName: doc.Names.vi.Branch,
                CatIds: cat.CatIds,
                CatName: cat.CatName,
                BrandId: brand._id,
                ChainName: brand.ChainName,
                CreatedAt: new Date(),
                UpdatedAt: new Date()
            };

            bulkInsert.find({
                BranchName: element.BranchName,
                ChainName: element.ChainName
            }).upsert().replaceOne(element);
        }
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadBranch', result });
    return true;
}

function loadWorkingHours() {
    const bulkInsert = db.workhours.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    documents.forEach(doc => {
        if (!doc.WorkingTime) return;

        const element = {
            WorkingTime: doc.WorkingTime,
            CreatedAt: new Date(),
            UpdatedAt: new Date()
        };

        bulkInsert.find({ WorkingTime: element.WorkingTime }).upsert().replaceOne(element);
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadWorkingHours', result });
    return true;
}

function loadPoi() {
    const bulkInsert = db.pois.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    documents.forEach(doc => {

        const element = {
            VMId: doc.Id,
            VMParentId: doc.ParentId,
            Lat: doc.Geometry.Y,
            Lng: doc.Geometry.X,
            CatIds: doc.CatIds,
            Phones: doc.Phones || [],
            Emails: doc.Emails || [],
            Websites: doc.Websites || [],
            WorkingTime: doc.WorkingTime,
            CreatedAt: new Date(doc.CreatedDt),
            UpdatedAt: new Date(doc.ModifiedDt),
            Status: doc.status
        };

        if (doc.Names?.vi) {
            element.Name = doc.Names.vi.Name;
            element.ShortName = doc.Names.vi.ShortName;
            element.AltName = doc.Names.vi.AltName;
            element.FullName = doc.Names.vi.FullName;
            element.BranchName = doc.Names.vi.Branch;
            element.ChainName = doc.Names.vi.BrandName;
        }

        if (doc.Specials?.vi) {
            element.Specials = doc.Specials.vi;
        }

        if (doc.Addresses?.vi) {
            element.Address = doc.Addresses.vi;
        }

        const cat = db.catgory.findOne({
            CatIds: doc.CatIds
        });

        if (cat) {
            element.CatName = cat.CatName;
        }

        bulkInsert.find({ VMId: doc.Id }).upsert().replaceOne(element);
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadPoi', result });
    return true;
}

function loadEntryPoi() {
    try {
        // Initialize bulk operation
        const bulkInsert = db.entry_pois.initializeUnorderedBulkOp();
        let processedCount = 0;
        let errorCount = 0;

        // Fetch all documents
        const documents = db.EntryPoint_Nearby.find({});

        // Process each document
        documents.forEach(doc => {
            try {
                // Validate required fields
                if (!doc.Id || !doc.Geometry?.X || !doc.Geometry?.Y) {
                    print(`Warning: Skipping document ${doc._id} due to missing required fields`);
                    errorCount++;
                    return;
                }

                // Process name - take only the part before hyphen if it exists
                let processedName = '';
                if (doc.Names?.vi) {
                    processedName = doc.Names.vi.split('-')[0].trim();
                }

                // Construct street name safely
                const prefix = doc.StreetName?.vi?.[0]?.Prefix || '';
                const name = doc.StreetName?.vi?.[0]?.Name || '';
                const fullAddress = [prefix, name].filter(Boolean).join(' ');

                const element = {
                    VMId: doc.Id,
                    VMParentId: doc.ParentId || null,
                    Lat: doc.Geometry.Y,
                    Lng: doc.Geometry.X,
                    Name: processedName,
                    Address: fullAddress,
                    CreatedAt: doc.CreatedDt ? new Date(doc.CreatedDt) : new Date(),
                    UpdatedAt: doc.ModifiedDt ? new Date(doc.ModifiedDt) : new Date(),
                    Status: doc.status
                };

                // Add to bulk operation
                bulkInsert.find({ VMId: doc.Id }).upsert().replaceOne(element);
                processedCount++;
            } catch (docError) {
                print(`Error processing document ${doc._id}: ${docError.message}`);
                errorCount++;
            }
        });

        // Execute bulk operation if there are items to process
        let result = null;
        if (processedCount > 0) {
            result = bulkInsert.execute();
        }

        // Generate detailed operation report
        const report = {
            operation: 'loadEntryPoi',
            processedCount,
            errorCount,
            success: errorCount === 0,
            timestamp: new Date(),
            result
        };

        printjson(report);
        return report;

    } catch (error) {
        const errorReport = {
            operation: 'loadEntryPoi',
            success: false,
            error: error.message,
            timestamp: new Date()
        };
        printjson(errorReport);
        return errorReport;
    }
}

// Create indexes after loading data
function createIndexes() {
    db.pois.createIndex({ Location: "2dsphere" });
    db.pois.createIndex({ VMId: 1 }, { unique: true });
    db.entry_pois.createIndex({ Location: "2dsphere" });
    db.entry_pois.createIndex({ VMId: 1 }, { unique: true });
    printjson({ operation: 'createIndexes', status: 'completed' });
}

// Main execution
try {
    loadCategory();
    loadBrands();
    loadBranch();
    loadWorkingHours();
    loadPoi();
    loadEntryPoi();
    createIndexes();
    printjson({ status: 'success', message: 'All operations completed successfully' });
} catch (error) {
    printjson({ status: 'error', message: error.message });
    throw error;
}