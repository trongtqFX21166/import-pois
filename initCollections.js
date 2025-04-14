use VMPOIRawDB;


function queryData() {
    // Get all Documents in 'gas' Collection
	const documents = db.gas.find({}, {"features": 1, "_id": 0}).toArray();

    // Process each document
    documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {
                console.log(feature.attributes);
            });
        }
    });

	return true;
}


function loadCatgory() {
	const bulkInsert = db.catgory.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.gas.find({}, {"features": 1, "_id":0});

	// Process each document
    documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {
				const element = {
					CatIds: feature.attributes.CatIds,
					CatName: 'Trạm xăng'
				};
				// Upsert into education Document
				bulkInsert.find(element).upsert().replaceOne(element);
            });
        }
    });	

	bulkInsert.execute();
	return true;
}


function loadBrands() {
	const bulkInsert = db.brands.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.gas.find({}, {"features": 1});

	// Process each document
	documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {
				const element = {
					ChainName: feature.attributes.ChainName,
					CatIds: feature.attributes.CatIds,
					CatName: "Trạm xăng"
				};
				// Upsert into education Document
				bulkInsert.find(element).upsert().replaceOne(element);
            });
        }
    });	

	bulkInsert.execute();
	return true;
}

function loadBranch() {
	const bulkInsert = db.branchs.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.gas.find({}, {"features": 1});

	// Process each document
	documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {
				const element = {            
					BranchName: feature.attributes.Branch,
					CatIds: feature.attributes.CatIds,
					CatName: "Trạm xăng"
				};
		
				// get education PK
				const brand = db.brands.findOne({
					ChainName : feature.attributes.ChainName
				});
				element.BrandId = brand._id;
				element.ChainName = brand.ChainName

				const cat = db.catgory.findOne({
					CatIds: feature.attributes.CatIds
				});
				element.CatName = cat.CatName
		
				// Upsert into education Document
				bulkInsert.find(element).upsert().replaceOne(element);
            });
        }
    });	

	bulkInsert.execute();
	return true;
}

function loadWorkingHours() {
	const bulkInsert = db.workhours.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.gas.find({}, {"features": 1});

	// Process each document
	documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {
				const element = {            
					WorkingTime: feature.attributes.WorkingTime
				};
			
				// Upsert into education Document
				bulkInsert.find(element).upsert().replaceOne(element);
            });
        }
    });	

	bulkInsert.execute();
	return true;
}


function loadPoi() {
	const bulkInsert = db.pois.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.gas.find({}, {"features": 1});

	// Process each document
	// Process each document
	documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {
				const element = {            
					VMId: feature.attributes.Id,
					VMParentId: feature.attributes.ParentId,
					Name: feature.attributes.Name,
					Address: feature.attributes.Address,
					Lat: feature.attributes.POINT_Y,
					Lng: feature.attributes.POINT_X,
					CatIds: feature.attributes.CatIds,
					TelNum1: feature.attributes.TelNum1,
					TelNum2: feature.attributes.TelNum2,
					Email: feature.attributes.Email,
					Website: feature.attributes.Website,
					BranchName: feature.attributes.Branch,
					WorkingTime: feature.attributes.WorkingTime,
					ChainName: feature.attributes.ChainName
				};

				// get category
				const cat = db.catgory.findOne({
					CatIds: feature.attributes.CatIds
				});
				element.CatName = cat.CatName
		
				// Upsert into education Document
				bulkInsert.find(element).upsert().replaceOne(element);
            });
        }
    });	

	bulkInsert.execute();
	return true;
}

function dropCollections()
{
	db.brands.drop();
	db.branchs.drop();
	db.category.drop();
	db.pois.drop();

}

console.log('drop collections');
dropCollections();

console.log('load category');
loadCatgory();
console.log('load brand');
loadBrands();
console.log('load branch');
loadBranch();
console.log('load works');
loadWorkingHours();

console.log('load poi');
loadPoi();