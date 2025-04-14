function queryData() {
    // Get all Documents in 'charger' Collection
	const documents = db.categories.find({}).toArray();

    // Process each document
    documents.forEach(doc => {
        console.log(doc);
    });

	return true;
}


function loadCatgory() {
	db.categories.find().forEach(function(doc) {
		db.categories.updateOne(
			{ _id: doc._id },
			{ $set: { Code: doc.Code.toString() } }
		);
	});

	const bulkInsert = db.catgory.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.charger.find({}, {"features": 1, "_id":0});

	// Process each document
    documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {

				const cat = db.categories.findOne({
					Code : feature.attributes.CatIds
				});
				
				if (cat) {
					const element = {
						CatIds: cat.Code,
						CatName: cat.Sub === "" ? cat.Main : cat.Sub
					};
					// Upsert into education Document
					bulkInsert.find(element).upsert().replaceOne(element);
				}
            });
        }
    });	

	bulkInsert.execute();
	return true;
}


function loadBrands() {
	const bulkInsert = db.brands.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.charger.find({}, {"features": 1});

	// Process each document
	documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {

            doc.features.forEach(feature => {

				if(feature.attributes.CatIds){
					const cat = db.catgory.findOne({
						CatIds : feature.attributes.CatIds
					});

					if(cat){

						const element = {
							ChainName: feature.attributes.Chainname,
							CatIds: cat.CatIds,
							CatName: cat.CatName
						};
						// Upsert into education Document
						bulkInsert.find(element).upsert().replaceOne(element);
					}
		
	

				}
				
            });
        }
    });	

	bulkInsert.execute();
	return true;
}

function loadBranch() {
	const bulkInsert = db.branchs.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.charger.find({}, {"features": 1});

	// Process each document
	documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {

				if(feature.attributes.CatIds){
					const cat = db.catgory.findOne({
						CatIds : feature.attributes.CatIds
					});
	
					if(cat){
						const element = {            
							BranchName: feature.attributes.Branch,
							CatIds: cat.CatIds,
							CatName: cat.CatName
						};
				
						// get education PK
						const brand = db.brands.findOne({
							ChainName : feature.attributes.Chainname
						});
						element.BrandId = brand._id;
						element.ChainName = brand.ChainName				
						element.CatName = cat.CatName
				
						// Upsert into education Document
						bulkInsert.find(element).upsert().replaceOne(element);
					}
				}
            });
        }
    });	

	bulkInsert.execute();
	return true;
}

function loadWorkingHours() {
	const bulkInsert = db.workhours.initializeUnorderedBulkOp();
	// Get all Documents in 'full' Collection
	const documents = db.charger.find({}, {"features": 1});

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
	const documents = db.charger.find({}, {"features": 1});

	// Process each document
	documents.forEach(doc => {
        if (doc.features && Array.isArray(doc.features)) {
            doc.features.forEach(feature => {
				const element = {            
					VMId: feature.attributes.Id,
					VMParentId: feature.attributes.ParentId,
					Name: feature.attributes.Name,
					Address: feature.attributes.Address,
					Lat: feature.geometry.y,
					Lng: feature.geometry.x,
					CatIds: feature.attributes.CatIds,
					TelNum1: feature.attributes.TelNum1,
					TelNum2: feature.attributes.TelNum2,
					Email: feature.attributes.Email,
					Website: feature.attributes.Website,
					BranchName: feature.attributes.Branch,
					WorkingTime: feature.attributes.WorkingTime,
					ChainName: feature.attributes.Chainname,
					Specials: feature.attributes.Specials
				};

				// get category
				const cat = db.catgory.findOne({
					CatIds: feature.attributes.CatIds
				});
				if(cat){
					element.CatName = cat.CatName
				}
		
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
	db.catgory.drop();
	db.brands.drop();
	db.branchs.drop();	
	db.pois.drop();
	return true;
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