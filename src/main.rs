use arrow::csv::{ReaderBuilder, self};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

fn process_file(file_path: &str, schema: Arc<Schema>) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut csv = ReaderBuilder::new(schema)
        .has_header(true)
        .build(file)?;
    let batch = csv.next().ok_or("No record batch found")??;

    // Process the batch
    // Print the schema
    println!("Schema: {:?}", batch.schema());

    // Print the data in the RecordBatch
    for i in 0..batch.num_columns() {
        let column = batch.column(i);
        println!("Column {}: {:?}", batch.schema().field(i).name(), column);
    }
    println!("Successfully read a batch from file: {}", file_path);

    Ok(())
}


// Define a struct to represent a term, which can be a constant or a Utf8String.
#[derive(Debug)]
enum Term {
    Utf8String(String),
    Float(f64),
    Integer(i64),
}

impl Term {
    fn clone_term(&self) -> Term {
        match self {
            Term::Utf8String(s) => Term::Utf8String(s.clone()),
            Term::Float(f) => Term::Float(*f),
            Term::Integer(i) => Term::Integer(*i),
        }
    }
}

// Define a struct to represent an atom with a relation name and a tuple of terms.
#[derive(Debug)]
struct Atom {
    name: String,
    terms: Vec<Term>,
}

// Define a struct to represent a conjunctive query.
#[derive(Debug)]
struct ConjunctiveQuery {
    head_atom: Atom,
    body_atoms: Vec<Atom>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let beers = "./data/beers.csv";
    let breweries = "./data/breweries.csv";
    let categories = "./data/categories.csv";
    let locations = "./data/locations.csv";
    let styles = "./data/styles.csv";
    let data = vec![beers, breweries, categories, locations, styles];

    for file_path in data {
        let schema = match csv::infer_schema_from_files(&[file_path.to_string()], b',', None, true) {
            Ok(schema) => schema,
            Err(error) => {
                eprintln!("An error occurred: {:?}", error);
                Schema::empty()
            }
        };

        process_file(file_path, Arc::new(schema))?;
    }
    // F1 example conjunctive query construction
    let vbeer_id = Term::Utf8String("beer_id".to_string());
    let vbrew_id = Term::Utf8String("brew_id".to_string());
    let vbeer = Term::Utf8String("beer".to_string());
    let vabv = Term::Utf8String("abv".to_string());
    let vibu = Term::Utf8String("ibu".to_string());
    let vounces = Term::Utf8String("ounces".to_string());
    let vstyle = Term::Utf8String("style".to_string());
    let vstyle2 = Term::Utf8String("style2".to_string());
    let vstyle_id = Term::Utf8String("style_id".to_string());
    let vcat_id = Term::Utf8String("cat_id".to_string());
    let vstyle4 = Term::Utf8String("style4".to_string());
    let cbelfre = Term::Utf8String("Belgian and French Ale".to_string());
    
    let answer = Atom {
        name: "Answer".to_string(),
        terms: vec![],
    };
    let beers = Atom {
        name: "Beers".to_string(),
        terms: vec![vbeer_id, vbrew_id, vbeer, vabv, vibu, vounces, vstyle, vstyle2],
    };
    let styles = Atom {
        name: "Styles".to_string(),
        terms: vec![vstyle_id, vcat_id.clone_term(), vstyle4],
    };
    let categories = Atom {
        name: "Categories".to_string(),
        terms: vec![vcat_id.clone_term(), cbelfre],
    };
     
    let query = ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![beers, styles, categories],
    };

    // You can now work with your conjunctive query as needed.
    println!("{:?}", query);

    Ok(())
}
