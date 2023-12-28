use std::collections::HashMap;

use arrow::{compute::and, compute::filter_record_batch, record_batch::RecordBatch};

use crate::csvout::write_record_batch_to_csv;
use crate::jointrees::{
    join_tree, make_boolean_array_float64, make_boolean_array_string, make_boolean_array_string,
    reduce,
};
use crate::queries::ConjunctiveQuery;

pub fn perform_query(
    query: &ConjunctiveQuery,
    data: &mut HashMap<String, RecordBatch>,
) -> RecordBatch {
    // example for cq4, need to automate this part.
    let atom = query.body_atoms[0].clone();
    let rb = data.get(atom.name).unwrap();

    // Create array(s)
    let column = rb.schema().index_of("style").unwrap_or(42);
    let column2 = rb.schema().index_of("abv").unwrap_or(42);
    let column3 = rb.schema().index_of("ibu").unwrap_or(42);

    // get constants from terms in atom
    let value = "Vienna Lager";
    let ba = make_boolean_array_string(rb, column, value);

    let value2 = 0.05;
    let ba2 = make_boolean_array_float64(rb, column2, value2);

    let value3 = "18";
    let ba3 = make_boolean_array_string(rb, column3, value3);

    // combine the boolean arrays
    let combined_ba = and(&and(&ba, &ba2).unwrap(), &ba3).unwrap();

    // Filter the record batch based on the predicate
    let filtered_record_batch = filter_record_batch(rb, &combined_ba).unwrap();

    filtered_record_batch
}

pub fn yannakaki(query: &ConjunctiveQuery, data: &mut HashMap<String, RecordBatch>) {
    let mut semi_join_info = join_tree(&query.body_atoms);
    // forward phase reducer
    reduce(semi_join_info.clone(), data);
    // backwardward phase reducer
    semi_join_info.reverse();
    reduce(semi_join_info, data);

    // perform query on reduced database
    let result = perform_query(query, data);
    // write to csv
    write_record_batch_to_csv(&result, "output");
}
