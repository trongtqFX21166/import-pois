function loadCity() {
    const bulkInsert = db.city.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    documents.forEach(doc => {
        if (!doc.City?.vi?.Prefix || !doc.City?.vi?.Name) return;

        const cityName = `${doc.City.vi.Prefix} ${doc.City.vi.Name}`;

        const element = {
            Name: cityName,
            NameVi: doc.City.vi.Name,
            PrefixVi: doc.City.vi.Prefix,
            NameEn: doc.City.en.Name,
            PrefixEn: doc.City.en.Prefix,
            CreatedAt: new Date(),
            UpdatedAt: new Date()
        };

        bulkInsert.find({ Name: cityName }).upsert().replaceOne(element);
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadCity', result });
    return true;
}

function loadDistrict() {
    const bulkInsert = db.district.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    documents.forEach(doc => {
        if (!doc.City?.vi?.Prefix || !doc.City?.vi?.Name ||
            !doc.District?.vi?.Prefix || !doc.District?.vi?.Name) return;

        const cityName = `${doc.City.vi.Prefix} ${doc.City.vi.Name}`;
        const districtName = `${doc.District.vi.Prefix} ${doc.District.vi.Name}`;

        const city = db.city.findOne({
            Name: cityName
        });

        if (city) {
            const element = {
                Name: districtName,
                City: cityName,
                NameVi: doc.District.vi.Name,
                PrefixVi: doc.District.vi.Prefix,
                NameEn: doc.District.en.Name,
                PrefixEn: doc.District.en.Prefix,
                CreatedAt: new Date(),
                UpdatedAt: new Date()
            };

            bulkInsert.find({
                Name: districtName,
                City: cityName
            }).upsert().replaceOne(element);
        }
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadDistrict', result });
    return true;
}

function loadWard() {
    const bulkInsert = db.ward.initializeUnorderedBulkOp();
    const documents = db.POI_Nearby.find({});

    documents.forEach(doc => {
        if (!doc.City?.vi?.Prefix || !doc.City?.vi?.Name ||
            !doc.District?.vi?.Prefix || !doc.District?.vi?.Name) return;

        const cityName = `${doc.City.vi.Prefix} ${doc.City.vi.Name}`;
        const districtName = `${doc.District.vi.Prefix} ${doc.District.vi.Name}`;

        let wardName = "";
        if (doc.Ward) {
            wardName = `${doc.Ward.vi.Prefix || ''} ${doc.Ward.vi.Name}`.trim();
        }

        const district = db.district.findOne({
            Name: districtName,
            City: cityName
        });

        if (district && wardName) {
            const element = {
                Name: wardName,
                District: district.Name,
                City: district.City,
                NameVi: doc.Ward.vi.Name,
                PrefixVi: doc.Ward.vi.Prefix,
                NameEn: doc.Ward.en.Name,
                PrefixEn: doc.Ward.en.Prefix,
                CreatedAt: new Date(),
                UpdatedAt: new Date()
            };

            bulkInsert.find({
                Name: wardName,
                District: district.Name,
                City: district.City
            }).upsert().replaceOne(element);
        }
    });

    const result = bulkInsert.execute();
    printjson({ operation: 'loadWard', result });
    return true;
}

function addAdminData() {
    const documents = db.pois.find({});
    let updateCount = 0;

    documents.forEach(doc => {
        const admin = db.POI_Nearby.findOne({ Id: doc.VMId });
        if (!admin) return;

        const cityName = `${admin.City.vi.Prefix} ${admin.City.vi.Name}`;
        const districtName = `${admin.District.vi.Prefix} ${admin.District.vi.Name}`;
        let wardName = "";

        if (admin.Ward) {
            wardName = `${admin.Ward.vi.Prefix || ''} ${admin.Ward.vi.Name}`.trim();
        }

        db.pois.updateOne(
            { _id: doc._id },
            {
                $set: {
                    admin: {
                        city: cityName,
                        district: districtName,
                        ward: wardName,
                        UpdatedAt: new Date()
                    }
                }
            }
        );
        updateCount++;
    });

    printjson({ operation: 'addAdminData', updatedRecords: updateCount });
    return true;
}

// Create indexes for admin collections
function createAdminIndexes() {
    db.city.createIndex({ Name: 1 }, { unique: true });
    db.district.createIndex({ Name: 1, City: 1 }, { unique: true });
    db.ward.createIndex({ Name: 1, District: 1, City: 1 }, { unique: true });
    printjson({ operation: 'createAdminIndexes', status: 'completed' });
}

// Main execution
try {
    loadCity();
    loadDistrict();
    loadWard();
    addAdminData();
    createAdminIndexes();
    printjson({ status: 'success', message: 'All admin operations completed successfully' });
} catch (error) {
    printjson({ status: 'error', message: error.message });
    throw error;
}