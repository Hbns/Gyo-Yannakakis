use arrow::csv::{self, ReaderBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

mod queries;
use queries::{
    create_cq1, create_cq2, create_cq3, create_cq4, create_cq5, 
    create_cyclic_example_query, create_example_query,
};

mod yannakaki;
use yannakaki::yannakaki;
// takes a filename and returns a Arrow recordbatch
fn process_file(file_path: &str, schema: Arc<Schema>) -> Result<RecordBatch, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut csv = ReaderBuilder::new(schema).has_header(true).build(file)?;
    let batch = csv.next().ok_or("No record batch found")??;

    println!("Successfully read a batch from file: {}", file_path);

    Ok(batch)
}

fn main() -> Result<(), Box<dyn Error>> {
    // Load the data.
    let beers = "./data/beers.csv";
    let breweries = "./data/breweries.csv";
    let categories = "./data/categories.csv";
    let locations = "./data/locations.csv";
    let styles = "./data/styles.csv";
    let data = vec![beers, breweries, categories, locations, styles];
    let keys = vec![
        "Beers".to_string(),
        "Breweries".to_string(),
        "Categories".to_string(),
        "Locations".to_string(),
        "Styles".to_string(),
    ];

    // Create a HashMap to store RecordBatches with keys
    let mut record_batch_map: HashMap<String, RecordBatch> = HashMap::new();

    for (file_path, key) in data.iter().zip(keys.iter()) {
        let schema = match csv::infer_schema_from_files(&[file_path.to_string()], b',', None, true)
        {
            Ok(schema) => schema,
            Err(error) => {
                eprintln!("An error occurred: {:?}", error);
                Schema::empty()
            }
        };

        // Call process_file and store the returned RecordBatch in the HashMap with the relation name as key
        let batch = process_file(file_path, Arc::new(schema))?;
        record_batch_map.insert(key.clone(), batch);
    }
    println!("rbm: {:?}", record_batch_map);
    // make all queries
    let query = create_example_query();
    let cquery = create_cyclic_example_query();
    let cq1 = create_cq1();
    let cq2 = create_cq2();
    let cq3 = create_cq3();
    let cq4 = create_cq4();
    let cq5 = create_cq5();

    /*
    Tests i query is acyclic
    acyclic_test(&query);
    acyclic_test(&cq1);
    acyclic_test(&cq2);
    acyclic_test(&cq3);
    acyclic_test(&cq4);
    acyclic_test(&cq5);
    acyclic_test(&cquery);
     */

    yannakaki(&cq4, &mut record_batch_map);

    Ok(())
}
