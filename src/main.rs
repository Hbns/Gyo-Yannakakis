use arrow::csv::{self, ReaderBuilder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

mod queries;
use queries::{
    create_cq1, create_cq2, create_cq3, create_cq4, create_cq5, create_cyclic_example_query,
    create_example_query,
};

mod gyo;
use gyo::acyclic_test;

mod jointrees;
use jointrees::{join_tree, reduce};

mod yannakaki;
use yannakaki::yannakaki;

mod csvout;
use csvout::write_to_csv;

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

        // Call process_file and store the returned RecordBatch in the HashMap with the key
        let batch = process_file(file_path, Arc::new(schema))?;
        record_batch_map.insert(key.clone(), batch);
    }
    // print the example query F1
    let query = create_example_query();
    //println!("{:?}", query);
    // Call collect_ears function
    //acyclic_test(&query);

    let cquery = create_cyclic_example_query();
    //println!("{:?}", cquery);
    let cq1 = create_cq1();
    let cq2 = create_cq2();
    let cq3 = create_cq3();
    let cq4 = create_cq4();
    let cq5 = create_cq5();
    acyclic_test(&query);
    acyclic_test(&cq1);
    acyclic_test(&cq2);
    acyclic_test(&cq3);
    acyclic_test(&cq4);
    acyclic_test(&cq5);
    acyclic_test(&cquery);
    //yannakaki(&query, &mut record_batch_map);

    /*
        // Prepare your data as vectors of vectors (rows and columns)
        // to be written to csv.
        let data_out: Vec<Vec<&str>> = vec![
            vec!["query_id", "is_acyclic", "bool_answer", "attr_x_answer", "attr_y_answer", "attr_z_answer", "attr_w_answer"],
            vec!["1", "t", "f", "some", "", "somezz", "someww"],
            vec!["2", "t", "t", "", "", "somezz", "someww"],
            // Add more rows as needed
        ];
        write_to_csv(&data_out);
    */
    Ok(())
}
