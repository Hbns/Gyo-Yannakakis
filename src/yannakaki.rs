use std::{collections::HashMap};

use arrow::{
    compute::{and},
    compute::filter_record_batch,
    record_batch::RecordBatch,
};

use crate::{
    csvout::write_record_batch_to_csv,
    jointrees::{join_tree, make_boolean_array_float64, reduce, make_boolean_array_string, make_boolean_array_int64},
    queries::{ConjunctiveQuery},
};

/*
Tree steps:
- semijoin from child to root
- semijoin for root to child
(this removed all dangling tuples)
- perform the query over the reduced database */
pub fn yannakaki(query: &ConjunctiveQuery, data: &mut HashMap<String, RecordBatch>) {
    
    let mut semi_join_info = join_tree(&query.body_atoms);
    // forward phase reducer
    reduce(semi_join_info.clone(), data);
    // backwardward phase reducer
    semi_join_info.reverse();
    reduce(semi_join_info, data);
    
    // perform query on reduced database
    // example for cq4, need to automate this part.
    //empty head -> is there at least one tuple in the database that works for this cq? ture or false
    let a = query.body_atoms[0].clone();
    let d = data.get(a.name).unwrap();
    
    // Create a boolean array 
    let column = d.schema().index_of("style").unwrap_or(42);
    let column2 = d.schema().index_of("abv").unwrap_or(42);
    let column3 = d.schema().index_of("ibu").unwrap_or(42);
    
    // get infor from terms in atom
    let value = "Vienna Lager";
    let ba = make_boolean_array_string(d, column, value);

    let value2 = 0.05;
    let ba2 = make_boolean_array_float64(d, column2, value2);
    
    let value3 = "18";
    let ba3 = make_boolean_array_string(d, column3, value3);
    
    // combine the boolean arrays
    let cba = and(&and(&ba,&ba2).unwrap(), &ba3).unwrap();

    // Filter the record batch based on the predicate
    let filtered_record_batch = filter_record_batch(d, &cba).unwrap();
    // write to csv
    write_record_batch_to_csv(&filtered_record_batch, "output_cq4");
    
}
