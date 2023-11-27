use arrow::csv::{self, ReaderBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

mod queries;
use queries::{create_cyclic_example_query, create_example_query};

mod gyo;
use gyo::acyclic_test;

mod jointrees;
use jointrees::{semi_join,semi_join2,jt3,common_terms};

use crate::jointrees::full_reducer;

fn process_file(file_path: &str, schema: Arc<Schema>) -> Result<RecordBatch, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut csv = ReaderBuilder::new(schema).has_header(true).build(file)?;
    let batch = csv.next().ok_or("No record batch found")??;

    //Process the batch
    //Print the schema
    //println!("Schema: {:?}", batch.schema());

    /*
    // Print the data in the RecordBatch
    for i in 0..batch.num_columns() {
        let column = batch.column(i);
        println!("Column {}: {:?}", batch.schema().field(i).name(), column);
    }
    */
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
    let mut record_batches: Vec<RecordBatch> = Vec::new();

    for file_path in data {
        let schema = match csv::infer_schema_from_files(&[file_path.to_string()], b',', None, true)
        {
            Ok(schema) => schema,
            Err(error) => {
                eprintln!("An error occurred: {:?}", error);
                Schema::empty()
            }
        };

        // Call process_file and store the returned RecordBatch in the vector
        let batch = process_file(file_path, Arc::new(schema))?;
        record_batches.push(batch);
    }

    //let result = semi_join2(&record_batches[0], &record_batches[1], 1, 0);
    //println!("{:?}", result);
    //println!("{:?}", record_batches);
    // print the example query F1
    let query = create_example_query();
    println!("{:?}", query);
    // Call collect_ears function
    //acyclic_test(&query);

    //let cquery = create_cyclic_example_query();
    //println!("{:?}", cquery);
    //acyclic_test(&cquery);

    //let join_tree = build_join_tree(&query.body_atoms);
    jt3(&query);
    //let join_tree2 = jt3(&query);
    //println!("{:?}", join_tree);
   // println!("{:?}", join_tree2);
    //let reduceddb = full_reducer(&join_tree, &record_batches);
    //println!("Globally consistent: {:?}", reduceddb);

    Ok(())
}
