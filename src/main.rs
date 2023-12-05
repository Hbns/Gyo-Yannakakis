use arrow::csv::{self, ReaderBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

mod queries;
use queries::{create_cyclic_example_query, create_example_query};

mod gyo;
use gyo::{acyclic_test,jt};

mod jointrees;
use jointrees::{semi_join,semi_join2,common_terms, gyo_remove_unique_items, join_tree, reduce};

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
    let keys = vec!["Beers", "Breweries", "Categories", "Locations", "Styles"];
 /*   
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
*/
    // Create a HashMap to store RecordBatches with keys
    let mut record_batch_map: HashMap<&str, RecordBatch> = HashMap::new();

    for (file_path, key) in data.iter().zip(keys.iter()) {
        let schema = match csv::infer_schema_from_files(&[file_path.to_string()], b',', None, true) {
            Ok(schema) => schema,
            Err(error) => {
                eprintln!("An error occurred: {:?}", error);
                Schema::empty()
            }
        };

        // Call process_file and store the returned RecordBatch in the HashMap with the key
        let batch = process_file(file_path, Arc::new(schema))?;
        record_batch_map.insert(key, batch);
    }

    //println!("recb: {:?}", record_batch_map);

  //  let result = semi_join2(&record_batches[0], &record_batches[1], 1, 0);
    //println!("{:?}", result);
    //println!("{:?}", record_batches);
    // print the example query F1
    let query = create_example_query();
    //println!("{:?}", query);
    // Call collect_ears function
    //acyclic_test(&query);

    //let cquery = create_cyclic_example_query();
    //println!("{:?}", cquery);
    //acyclic_test(&cquery);

    //let join_tree = build_join_tree(&query.body_atoms);
    //jt(&query);
    let semi_join_info = join_tree(&query.body_atoms);
    //println!("semi_join_info: {:?}", semi_join_info);

   reduce(&semi_join_info,  &record_batch_map);
    //let mut atoms = query.body_atoms;
    //gyo_remove_unique_items(&mut atoms);
    //let join_tree2 = jt3(&query);
    //println!("{:?}", join_tree);
   // println!("{:?}", join_tree2);
    //let reduceddb = full_reducer(&join_tree, &record_batches);
    //println!("Globally consistent: {:?}", reduceddb);

    Ok(())
}
