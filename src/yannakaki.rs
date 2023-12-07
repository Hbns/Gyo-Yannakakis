use std::collections::HashMap;

use arrow::record_batch::RecordBatch;

/*
Tree steps:
- semijoin from child to root
- semijoin for root to child
(this removed all dangling tuples)
- perform the query over the reduced database */
use crate::{queries::{ConjunctiveQuery}, jointrees::{join_tree, reduce}};

pub fn yannakaki(query: &ConjunctiveQuery, data: &mut HashMap<String, RecordBatch>){
    println!("cq: {:?}", query);
    let mut semi_join_info = join_tree(&query.body_atoms);
    println!("semi_join_info: {:?}", semi_join_info);
    // semijoin bottem to top
    reduce(semi_join_info.clone(), data);
    semi_join_info.reverse();
    // semijoin top to bottom
    println!("reversed semi_join_info: {:?}", semi_join_info);
    reduce(semi_join_info, data);

    // do the query

}