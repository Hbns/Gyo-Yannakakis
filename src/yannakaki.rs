use std::{collections::HashMap, fs::File, io::BufWriter};

use arrow::{
    array::{Array, BooleanArray, Datum, Float64Array, PrimitiveArray, StringArray},
    compute::filter,
    compute::filter_record_batch,
    csv::Writer,
    datatypes::DataType,
    record_batch::RecordBatch,
};

/*
Tree steps:
- semijoin from child to root
- semijoin for root to child
(this removed all dangling tuples)
- perform the query over the reduced database */
use crate::{
    csvout::write_record_batch_to_csv,
    jointrees::{join_tree, make_boolean_array_float64, reduce},
    queries::{Atom, ConjunctiveQuery, Term},
};

// Select rows from a RecordBatch based on an Atom
fn select_rows(record_batch: &RecordBatch, atom: &Atom) -> Vec<usize> {
    let mut selected_rows = Vec::new();

    // Assuming the terms in the Atom correspond to columns in the RecordBatch
    let columns = record_batch.columns();

    // Iterate through rows in the RecordBatch
    for row in 0..record_batch.num_rows() {
        let mut is_match = true;

        // Iterate through terms in the Atom
        for (term, column) in atom.terms.iter().zip(columns) {
            match term {
                Term::Variable(_) => {
                    // Handle variable term if needed
                    is_match = false;
                }
                Term::Constant(value) => {
                    // Get the value from the column at the current row
                    is_match = true;
                    let column_data_type: &DataType = column.data_type();

                    match column_data_type {
                        DataType::Int64 => {
                            let int_column = column
                                .as_any()
                                .downcast_ref::<PrimitiveArray<arrow::datatypes::Int64Type>>()
                                .expect("Failed to downcast to Int64Array");

                            // Compare the value with the constant term
                            if int_column.is_empty()
                                && int_column.value(row) != value.parse::<i64>().unwrap_or(42)
                            {
                                is_match = false;
                                break;
                            }
                        }
                        DataType::Float64 => {
                            let float_column = column
                                .as_any()
                                .downcast_ref::<PrimitiveArray<arrow::datatypes::Float64Type>>()
                                .expect("Failed to downcast to Float64Array");

                            // Compare the value with the constant term
                            if float_column.is_empty()
                                && float_column.value(row) != value.parse::<f64>().unwrap_or(42.0)
                            {
                                is_match = false;
                                break;
                            }
                        }
                        DataType::Utf8 => {
                            let utf8_column = column
                                .as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                                .expect("Failed to downcast to StringArray");

                            // Compare the value with the constant term
                            if utf8_column.is_empty() || &utf8_column.value(row) != value {
                                is_match = false;
                                break;
                            }
                        }
                        // Add more cases for other data types as needed
                        _ => {
                            // Handle other data types
                            println!("Unhandled data type: {:?}", column_data_type);
                        }
                    }
                }
            }
        }

        // If all terms match, add the row index to the result
        if is_match {
            selected_rows.push(row);
        }
    }

    selected_rows
}

// Select rows from a HashMap of RecordBatches based on an Atom
fn select_rows_from_hashmap(
    hashmap: &HashMap<String, RecordBatch>,
    atom: &Atom,
) -> Option<Vec<usize>> {
    hashmap
        .get(atom.name)
        .map(|record_batch| select_rows(record_batch, atom))
}

pub fn yannakaki(query: &ConjunctiveQuery, data: &mut HashMap<String, RecordBatch>) {
    
    println!("cq: {:?}", query);
    let mut semi_join_info = join_tree(&query.body_atoms);
    println!("semi_join_info: {:?}", semi_join_info);
    // semijoin bottem to top
    reduce(semi_join_info.clone(), data);
    semi_join_info.reverse();
    // semijoin top to bottom
    //println!("reversed semi_join_info: {:?}", semi_join_info);
    reduce(semi_join_info, data);
    
    //println!("Query: {:?}", query);
    // do the query
    //empty head -> is there at least one tuple in the database that works for this cq? ture or false
    let a = query.body_atoms[0].clone();
    let r = select_rows_from_hashmap(data, &a);
    // println!("Atom: {:?}, valid rows {:?}", a.name, r);
    let d = data.get(a.name).unwrap();
    // println!("column_length: {:?}", d.num_rows());
    // println!("rb: {:?}", d);

    // Create a boolean array representing the predicate condition
    let column = d.schema().index_of("abv").unwrap_or(42);
    let col_array = d.column(3);

    let value = 0.06;
    let ba = make_boolean_array_float64(d, column, value);

    // Filter the record batch based on the predicate
    let filtered_record_batch = filter_record_batch(d, &ba).unwrap();
    // write to csv
    write_record_batch_to_csv(&filtered_record_batch, "output");

    
}
