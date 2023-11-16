// Join trees:

use std::{collections::HashMap, error::Error, sync::Arc};

use arrow::{
    array::{BooleanArray, ArrayRef, Array, Int64Array},
    compute::{and, filter, kernels::cmp::eq},
    record_batch::RecordBatch,
  
};

// Given a hypergraph H = (V,E), a tree T is a join tree of H if
// • the nodes of T are precisely the hyperedges in E and,
// • for each node v in V , the set of nodes of T in which v is an element
// forms a connected subtree of T.
use crate::queries::Atom;

#[derive(Debug)]
pub struct JoinTreeNode {
    relation: String,
    children: Vec<JoinTreeNode>,
}

impl JoinTreeNode {
    fn new(relation: &str) -> JoinTreeNode {
        JoinTreeNode {
            relation: relation.to_string(),
            children: Vec::new(),
        }
    }

    //   fn add_child(&mut self, child: JoinTreeNode) {
    //       self.children.push(child);
    //   }
}

pub fn build_join_tree(body_atoms: &Vec<Atom>) -> JoinTreeNode {
    let mut root = JoinTreeNode::new(&body_atoms[0].name);

    for atom in body_atoms.iter().skip(1) {
        build_join_tree_recursive(&mut root, atom);
    }

    root
}

fn build_join_tree_recursive(parent: &mut JoinTreeNode, atom: &Atom) {
    let mut current_index = 0;
    let mut stack = Vec::new();

    loop {
        if !parent
            .children
            .iter()
            .any(|child| child.relation == atom.name)
        {
            let child_node = JoinTreeNode::new(&atom.name);
            //for term in &atom.terms {
            //     // Add any additional logic for handling terms as needed
            // }
            parent.children.push(child_node);
            break;
        }

        // Find the child with the matching relation
        let child_index = parent
            .children
            .iter()
            .position(|child| child.relation == atom.name);

        match child_index {
            Some(idx) => {
                current_index = idx;
            }
            None => {
                break;
            }
        }
    }

    // Continue building the tree for each child
    for _ in 1..parent.children.len() {
        let child = &mut parent.children[current_index];
        if child.relation == atom.name {
            stack.push(current_index);
        }
    }

    while let Some(idx) = stack.pop() {
        let child = &mut parent.children[idx];
        build_join_tree_recursive(child, atom);
    }
}

// In a semi-join operation, you typically retain only the columns from the 
// first table (the left table) in the result. The purpose of a semi-join is 
// to filter the rows in the left table based on the existence of corresponding
// values in the right table. Therefore, the result will include all columns
// from the left table and none from the right table.

pub fn semi_join(
    batch1: &RecordBatch,
    batch2: &RecordBatch,
    column2_index_batch1: usize,
    column1_index_batch2: usize,
) -> Result<RecordBatch, Box<dyn Error>> {
    let column2_values_batch1 = batch1.column(column2_index_batch1);
    let column1_values_batch2 = batch2.column(column1_index_batch2);

    println!("Column1 Values in Batch1: {:?}", column2_values_batch1);
    println!("Column1 Values in Batch2: {:?}", column1_values_batch2);


    // Create a boolean array based on the equality condition
    let equality_condition = eq(&column2_values_batch1, &column1_values_batch2)?;
    println!("Equality Condition: {:?}", equality_condition);

    // Apply filtering based on the equality condition to batch1
    let filtered_rows: Vec<ArrayRef> = batch1
        .columns()
        .iter()
        .map(|column| filter(column, &equality_condition).unwrap())
        .collect();
    

    // Create a new RecordBatch with the filtered rows for each column
    let result_batch = RecordBatch::try_new(batch1.schema().clone(), filtered_rows)?;
    println!("Filtered Rows: {:?}", result_batch.num_rows());

    Ok(result_batch)
}
pub fn semi_join2(
    record_batch1: &RecordBatch,
    record_batch2: &RecordBatch,
    columnb1_index: usize,
    columnb2_index: usize,
) -> Result<RecordBatch, Box<dyn Error>> {
    // Extract the column values to be compared
    let column_values1 = record_batch1.column(columnb1_index);
    let column_values2 = record_batch2.column(columnb2_index);

    // Initialize a vector to store boolean results
    let mut result_vector: Vec<bool> = Vec::new();

    // Iterate over each row in the first record batch
    for i in 0..record_batch1.num_rows() {
        // Get the value of the current row in the first record batch
        let value_to_compare = Some(column_values1
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(i));

        // Check if the value exists in any row of the second record batch
        let exists_in_second_batch = column_values2
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .any(|value| value == value_to_compare);

        // Add the result to the boolean vector
        result_vector.push(exists_in_second_batch);
    }

    // Create a BooleanArray from the result vector
    let result_boolean_array = BooleanArray::from(result_vector);
    //println!("result_boolean_array: {:?}", result_boolean_array);

    // Apply filtering based on the equality condition to batch1
    let filtered_rows: Vec<ArrayRef> = record_batch1
        .columns()
        .iter()
        .map(|column| filter(column, &result_boolean_array).unwrap())
        .collect();
    //println!("filtered_rows: {:?}", filtered_rows);
    // Use the BooleanArray to filter the original record batch
    //let filtered_record_batch = filter(record_batch1, &result_boolean_arra );
    //let filtered_record_batch = RecordBatch::try_new(batch1.schema().clone(), filtered_rows)?;
     // Create a new RecordBatch with the filtered rows for each column
     let result_batch = RecordBatch::try_new(record_batch1.schema().clone(), filtered_rows)?;
     println!("Filtered Rows: {:?}", result_batch.num_rows());

    Ok(result_batch)
}
pub fn full_reducer(
    join_tree: &JoinTreeNode,
    record_batches: &[RecordBatch],
) -> Result<RecordBatch, Box<dyn Error>> {
    // Static mapping from batch names to indices
   // Static mapping from batch names to indices
   let batch_name_to_index: HashMap<String, usize> = {
    let mut map = HashMap::new();
    map.insert("Beers.".to_string(), 0);
    map.insert("Breweries".to_string(), 1);
    map.insert("Categories".to_string(), 2);
    map.insert("Locations".to_string(), 3);
    map.insert("Styles".to_string(), 4);
    map
};

    // Base case: If the node has no children, return the corresponding record batch
    if join_tree.children.is_empty() {
        let relation_index = *batch_name_to_index
            .get(&join_tree.relation)
            .ok_or(format!("Relation {} not found", join_tree.relation))?;

        return Ok(record_batches[relation_index].clone());
    }

    // Recursive case: Perform semi-join with each child
    let mut result = full_reducer(&join_tree.children[0], record_batches)?;

    for i in 1..join_tree.children.len() {
        let child_batch = full_reducer(&join_tree.children[i], record_batches)?;
        result = semi_join2(&result, &child_batch, 0, 0)?;
    }

    Ok(result)
}