// Join trees:

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int64Array},
    compute::{and, filter, kernels::cmp::eq},
    record_batch::RecordBatch,
};

// Given a hypergraph H = (V,E), a tree T is a join tree of H if
// • the nodes of T are precisely the hyperedges in E and,
// • for each node v in V , the set of nodes of T in which v is an element
// forms a connected subtree of T.
use crate::queries::{Atom, ConjunctiveQuery, Term};

#[derive(Debug, Clone)]
pub struct JoinTreeNode {
    relation: String,
    common_term: Vec<&'static Term>,
    children: Vec<JoinTreeNode>,
}

impl JoinTreeNode {
    fn new(relation: String, common_term: Vec<&'static Term>) -> JoinTreeNode {
        JoinTreeNode {
            relation,
            common_term,
            children: Vec::new(),
        }
    }

    fn add_child(&mut self, child: JoinTreeNode) {
        self.children.push(child);
    }
}

pub fn common_terms(atom1: &Atom, atom2: &Atom) -> Vec<&'static Term> {
    atom1
        .terms
        .iter()
        .cloned()
        .filter(|term1| atom2.terms.iter().any(|term2| term1 == term2))
        .collect()
}

fn build_jtnode(name: String, common: Vec<&'static Term>) -> JoinTreeNode {
    JoinTreeNode::new(name, common)
}

pub fn remove_first_atom(cj: &mut ConjunctiveQuery) {
    if !cj.body_atoms.is_empty() {
        cj.body_atoms.remove(0);
    }
    cj;
}
fn buildjt (nodes: Vec<JoinTreeNode>){
    let mut node = nodes[0].clone();
    let mut node2 = nodes[1].clone();

    node.add_child(node2);
    println!("nodetree: {:?}", node);
  
    
}
fn build_tree(nodes: &mut Vec<JoinTreeNode>) {
    if let Some(mut current_node) = nodes.pop() {
        println!("current_node: {:?}", current_node);
        for child_node in nodes.iter_mut() {
            let common_terms: Vec<_> = current_node
                .common_term
                .iter()
                .filter(|term| child_node.common_term.contains(term))
                .cloned()
                .collect();
println!("child_node: {:?}", child_node);
println!("-common_terms: {:?}", common_terms);

            if !common_terms.is_empty() {
                let child = child_node;
                //current_node.add_child(child);
                //build_tree(&mut current_node.children);
                
            }
        }

        build_tree(nodes);
    }
}

pub fn jt7(atoms: &Vec<Atom>){
    let mut term_set: HashMap<&'static str, HashSet<&Term>> = HashMap::new();
    println!("atoms: {:?}", atoms);
    //BTreeMap keeps order when inserting, hasMap does not but is cheaper (use large data)
    for atom in atoms {
        term_set.insert(atom.name, atom.terms.clone().into_iter().collect());

    }
    println!("term_set: {:?}", term_set);
    // Remove items that are unique to each set
    for set_name in term_set.keys().cloned().collect::<Vec<_>>() {
        let intersection: HashSet<_> = term_set
            .values()
            .filter(|&other_set| other_set != term_set.get(&set_name).unwrap())
            .flat_map(|other_set| other_set.iter())
            .cloned()
            .collect();

        let set = term_set.get_mut(&set_name).unwrap();
        set.retain(|item| intersection.contains(item));
    }
    println!("term_set: {:?}", term_set);

    // Build the join tree
    let mut join_tree_nodes: Vec<JoinTreeNode> = Vec::new();

    for (index, (current_relation, current_terms)) in term_set.iter().enumerate() {
        let mut current_node = JoinTreeNode::
        new(current_relation.to_string(), current_terms.iter().cloned().collect());
        join_tree_nodes.insert(index, current_node);
    } 

    println!("join_tree_nodes: {:?}", join_tree_nodes);
    for node in &join_tree_nodes[0..2]{ // using a slice to not move the vector
        println!("node: {:?}", node); 
    }
    buildjt(join_tree_nodes);
      
    //println!("build_tree: {:?}", join_tree_nodes); 
}



pub fn gyo_remove_unique_items(vectors: &mut Vec<Atom>) {
    // Step 1: Create a HashSet for each vector
    let mut unique_items: Vec<HashSet<&Term>> = vectors.iter().map(|atom| HashSet::new()).collect();

    // Step 2: Create a HashMap to store the mapping between unique items and atom names
    let mut item_to_atom_name: HashMap<&Term, &'static str> = HashMap::new();
    

    // Step 3: Iterate through all vectors to populate and update the HashSet and HashMap
    for (atom_index, atom) in vectors.iter().enumerate() {
        for term in &atom.terms {
            // Clone the term to insert it into the HashSet
            let cloned_term = term.clone();
            unique_items[atom_index].insert(cloned_term);

            // Update the HashMap with the mapping between the term and atom name
            item_to_atom_name.insert(cloned_term, atom.name);
        }
    }
    println!("itoa: {:?}", item_to_atom_name);
    // Now you have unique_items populated with references to Term
    // You can continue with the rest of your logic...

    // Step 4: Iterate through each vector and remove items that are unique to that vector
    for (atom_index, atom) in vectors.iter_mut().enumerate() {
        atom.terms.retain(|term| {
            unique_items
                .iter()
                .enumerate()
                .filter(|&(i, set)| i != atom_index && set.contains(term))
                .count()
                > 0
        });
    }

    // Step 5: Now you can access the atom names corresponding to each unique item
    // Step 5: Now you can access the atom names corresponding to each non-unique item
    for atom in vectors.iter() {
        for term in &atom.terms {
            if let Some(atom_name) = item_to_atom_name.get(term) {
                println!("Term: {:?}, Atom Name: {}", term, atom_name);
            }
        }
    }
}

/*
pub fn jt3(cj: &ConjunctiveQuery) -> JoinTreeNode {
    let mut node = build_jtnode(cj.body_atoms[0].name.to_string(), Vec::new());
    let mut i = 0;

    while cj.body_atoms.len() > 0{
        let current_atom = &cj.body_atoms[i];
        let next_atom = &cj.body_atoms[i + 1];

        let common = common_terms(current_atom, next_atom);

        if !common.is_empty() {
            node = build_jtnode(cj.body_atoms[i].name.to_string(), common);

            jt3(remove_first_atom(cj));
            //build_jtnode(current_atom.name.to_string(), common);
        }else{
            jt3;
        }
    };
    node

 //   let common = common_terms(&cj.body_atoms[0], &cj.body_atoms[1]);
 //   let mut node = build_jtnode(cj.body_atoms[0].name.to_string(), common);
 //   let commonB = common_terms(&cj.body_atoms[1], &cj.body_atoms[2]);
  //  let mut nodeB = JoinTreeNode::new(cj.body_atoms[1].name.to_string(), commonB);
  //  let commonC = common_terms(&cj.body_atoms[2], &cj.body_atoms[1]);
 //   let mut nodeC = JoinTreeNode::new(cj.body_atoms[2].name.to_string(), commonC);
  //  nodeB.add_child(nodeC);
  //  node.add_child(nodeB);

 //   println!("Node: {:?}", node);
}*/

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
        let value_to_compare = Some(
            column_values1
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i),
        );

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
