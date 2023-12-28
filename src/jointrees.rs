// Join trees:

use std::collections::{HashMap, HashSet};

use arrow::{
    array::{Array, BooleanArray, Float64Array, Int64Array, StringArray},
    compute::filter_record_batch,
    datatypes::DataType,
    record_batch::RecordBatch,
};

// Given a hypergraph H = (V,E), a tree T is a join tree of H if
// • the nodes of T are precisely the hyperedges in E and,
// • for each node v in V , the set of nodes of T in which v is an element
// forms a connected subtree of T.
use crate::queries::{Atom, Term};

// struct for a joinTree(node)
#[derive(Debug, Clone, PartialEq)]
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

// build the join tree from the nodes
fn build_tree(nodes: Vec<JoinTreeNode>) -> Option<JoinTreeNode> {
    let mut thisnodes = nodes.clone();

    if let Some(thisnode) = thisnodes.pop() {
        let mut lnode = thisnode.clone();

        let mut remaining_nodes = Vec::new();
        let mut nodes_to_remove = Vec::new();
        //let mut check_child_nodes = Vec::new();

        for node in &thisnodes {
            let mut common_found = false;

            // Check all common terms with the current thisnode
            for common in &thisnode.common_term {
                if node.common_term.contains(common) {
                    common_found = true;
                    break;
                }
            }

            if common_found {
                lnode.add_child(node.clone());
                nodes_to_remove.push(node.clone());
            } else {
                remaining_nodes.push(node.clone());
            }
        }

        // verify each child with remaingin nodes.

        for child in &mut lnode.children {
            // remove the nodes that are childs now.
            thisnodes.retain(|n| !nodes_to_remove.contains(n));

            let mut child_clone = child.clone();

            for node in &thisnodes {
                let mut common_found = false;

                // Check all common terms with the child
                for common in &child_clone.common_term {
                    if node.common_term.contains(common) {
                        common_found = true;
                        break;
                    }
                }
                if common_found {
                    // Add the node from thisnodes as a child of the current child
                    child_clone.add_child(node.clone());
                }
            }
            *child = child_clone;
        }

        // Remove the nodes that are now children from thisnodes
        // thisnodes.retain(|n| !nodes_to_remove.contains(n));
        Some(lnode)
    } else {
        None
    }
}

pub fn join_tree(atoms: &Vec<Atom>) -> Vec<Vec<String>> {
    let mut term_set: HashMap<&'static str, HashSet<&Term>> = HashMap::new();
    //println!("atoms: {:?}", atoms);
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
    //println!("term_set: {:?}", term_set);

    // Build the join tree
    let mut join_tree_nodes: Vec<JoinTreeNode> = Vec::new();

    for (index, (current_relation, current_terms)) in term_set.iter().enumerate() {
        let mut current_node = JoinTreeNode::new(
            current_relation.to_string(),
            current_terms.iter().cloned().collect(),
        );
        join_tree_nodes.insert(index, current_node);
    }
    println!("join_tree_nodes: {:?}", join_tree_nodes);
    // build the tree from the nodes
    let join_tree = build_tree(join_tree_nodes.clone());
    // extract information from the join_three for the semijoin
    let mut semi_join_info = get_semi_join_info(&join_tree.unwrap(), None);
    // remove last element
    semi_join_info.pop();
    // return the semi_join_info
    semi_join_info
}

// Go trough the JoinTreeNode and extract information for the semijoin.
fn get_semi_join_info(node: &JoinTreeNode, parent: Option<&JoinTreeNode>) -> Vec<Vec<String>> {
    // the vectors to return to semijoin.
    let mut result = Vec::new();
    // dive in the three.
    for child in &node.children {
        result.extend_from_slice(&get_semi_join_info(child, Some(node)));
    }
    // Procces the nodes.
    let mut current_node = node.clone();
    let mut current_parent = parent.unwrap_or(node).clone();
    current_node.children.clear();
    current_parent.children.clear();
    // Find the common common_term.
    let p_set: HashSet<_> = current_parent.common_term.into_iter().collect();
    let n_set: HashSet<_> = current_node.common_term.into_iter().collect();
    let common_term: HashSet<_> = p_set.intersection(&n_set).cloned().collect();
    // get the string of the common term.
    let common = if let Some(Term::Variable(value)) = common_term.iter().next() {
        value
    } else {
        "default_value"
    };
    // make the vector for semijoin: relation1, relation2, common_term.
    let semi_join_info = vec![
        current_parent.relation,
        current_node.relation,
        common.to_string(),
    ];
    result.push(semi_join_info);
    result
}

// make boolean array to filter realtion1 in semijoin.
fn make_boolean_array(
    relation1: &RecordBatch,
    column_index_r1: usize,
    relation2: &RecordBatch,
    column_index_r2: usize,
) -> BooleanArray {
    // Extract the columns from the RecordBatches
    let col_r1 = relation1.column(column_index_r1);
    let col_r2 = relation2.column(column_index_r2);
    // request the datatype of the column(s)
    let data_type = col_r1.data_type();

    // Create a boolean array to represent the result of the semi-join
    let mut result = vec![false; relation1.num_rows()];

    {
        match col_r1.data_type() {
            &DataType::Utf8 => {
                let values_r1 = col_r1.as_any().downcast_ref::<StringArray>().unwrap();
                let values_r2 = col_r2.as_any().downcast_ref::<StringArray>().unwrap();
                for (i, value_r1) in values_r1.iter().enumerate() {
                    result[i] = values_r2.iter().any(|value_r2| value_r1 == value_r2);
                }
            }
            &DataType::Int64 => {
                let values_r1 = col_r1.as_any().downcast_ref::<Int64Array>().unwrap();
                let values_r2 = col_r2.as_any().downcast_ref::<Int64Array>().unwrap();
                for (i, value_r1) in values_r1.iter().enumerate() {
                    result[i] = values_r2.iter().any(|value_r2| value_r1 == value_r2);
                }
            }
            &DataType::Float64 => {
                let values_r1 = col_r1.as_any().downcast_ref::<Float64Array>().unwrap();
                let values_r2 = col_r2.as_any().downcast_ref::<Float64Array>().unwrap();
                for (i, value_r1) in values_r1.iter().enumerate() {
                    result[i] = values_r2.iter().any(|value_r2| value_r1 == value_r2);
                }
            }
            _ => panic!("Unsupported data type: {:?}", col_r1.data_type()),
        }
    }
    //println!("bool: {:?}", result);
    BooleanArray::from(result)
}

// make a boolean array for value depending of the column type:
pub fn make_boolean_array_string(
    relation: &RecordBatch,
    column_index: usize,
    value: &str,
) -> BooleanArray {
    let col = relation
        .column(column_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let result = col
        .iter()
        .map(|item| item.as_deref().map_or(false, |s| s.contains(value)))
        .collect::<Vec<_>>();
    BooleanArray::from(result)
}

pub fn make_boolean_array_int64(
    relation: &RecordBatch,
    column_index: usize,
    value: i64,
) -> BooleanArray {
    let col = relation
        .column(column_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let result = col
        .iter()
        .map(|item| item == Some(value))
        .collect::<Vec<_>>();
    BooleanArray::from(result)
}

pub fn make_boolean_array_float64(
    relation: &RecordBatch,
    column_index: usize,
    value: f64,
) -> BooleanArray {
    let col = relation
        .column(column_index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let result = col
        .iter()
        .map(|item| item == Some(value))
        .collect::<Vec<_>>();
    BooleanArray::from(result)
}

pub fn reduce(infos: Vec<Vec<String>>, data: &mut HashMap<String, RecordBatch>) {
    //let infos2 = infos.clone();
    for info in infos {
        // distribute the info from the vector
        let key1 = info[0].clone();
        let key2 = info[1].clone();
        let column = info[2].as_str();

        // get the required recordbatches
        let record_batch1 = data.get(&key1);
        let record_batch2 = data.get(&key2);
        // get the required column indexes
        let column_index1 = record_batch1
            .unwrap()
            .schema()
            .index_of(column)
            .unwrap_or(42);
        let column_index2 = record_batch2
            .unwrap()
            .schema()
            .index_of(column)
            .unwrap_or(42);
        // make the boolean array
        let boolean_array = make_boolean_array(
            record_batch1.unwrap(),
            column_index1,
            record_batch2.unwrap(),
            column_index2,
        );
        //println!("#true {:?}", boolean_array.true_count());
        // filter relation1
        let filtered_relation1 = filter_record_batch(record_batch1.unwrap(), &boolean_array);
        data.insert(key1.clone(), filtered_relation1.unwrap());
        //println!("filtered {:?}", filtered);
    }
}
