use arrow::csv::{ReaderBuilder, self};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

mod queries;
use queries::create_example_query;

mod gyo;
use gyo::acyclic_test;




fn process_file(file_path: &str, schema: Arc<Schema>) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut csv = ReaderBuilder::new(schema)
        .has_header(true)
        .build(file)?;
    let batch = csv.next().ok_or("No record batch found")??;

    // Process the batch
    // Print the schema
    // println!("Schema: {:?}", batch.schema());

    /*
    // Print the data in the RecordBatch
    for i in 0..batch.num_columns() {
        let column = batch.column(i);
        println!("Column {}: {:?}", batch.schema().field(i).name(), column);
    }
    */
    println!("Successfully read a batch from file: {}", file_path);

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Load the data.
    let beers = "./data/beers.csv";
    let breweries = "./data/breweries.csv";
    let categories = "./data/categories.csv";
    let locations = "./data/locations.csv";
    let styles = "./data/styles.csv";
    let data = vec![beers, breweries, categories, locations, styles];

    for file_path in data {
        let schema = match csv::infer_schema_from_files(&[file_path.to_string()], 
        b',', None, true) {
            Ok(schema) => schema,
            Err(error) => {
                eprintln!("An error occurred: {:?}", error);
                Schema::empty()
            }
        };

        process_file(file_path, Arc::new(schema))?;
    }
    // print the example query F1
    let query = create_example_query();
    println!("{:?}", query);
    // Call collect_ears function
    acyclic_test(&query);

    Ok(())
}
