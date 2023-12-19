// gyo - Graham-Yu-Ozsoyoglu

/*
Hypergraph: exists out of nodes and hyperedges.
Hyperedges: exists out of any number of nodes.
Ear:        exists out of two distinc groups of nodes.
Group1:     exists out of nodes that are unique to the hyperedge (not shared with other hyperedges)
Group2:     exists out of nodes that appear in other hyperedges.
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Gyo reduction:
Repeatedly apply the follwowing operations (in no particular order):
1. Delete a vertex that appears in at most one hyperedge.
2. Delete a hyperedge that is contained in another hyperedge.

Gyo reduction is performed on the body atoms of the conjunctive query.
*/

use crate::queries::{ConjunctiveQuery, Term};
use std::collections::HashSet;

// function to test if a conjunctive qury is acyclic
pub fn acyclic_test(query: &ConjunctiveQuery) {
    // make mutable vector containing all ears.
    let mut ears = collect_ears(&query);
    let mut modified = true;
    // loop untill the conjuctive qury is empty or nothing can be roved anymore
    while modified {
        let ears_clone = ears.clone(); // Make a clone to check for modifications
        remove_unique_items(&mut ears);
        println!("removed_unique: {:?}", ears);
        remove_single_item_vectors(&mut ears);
        println!("removed_single_item: {:?}", ears);

        // check if modifications were made
        modified = ears != ears_clone;
    }

    if ears.iter().any(|vector| !vector.is_empty()) {
        println!("cyclic");
    } else {
        println!("acyclic");
    }
}
// collect all ears of the conjunctive query
fn collect_ears(query: &ConjunctiveQuery) -> Vec<Vec<&Term>> {
    // initialize a vector to store the terms vectors
    let mut ears: Vec<Vec<&Term>> = Vec::new();

    // iterate through body atoms
    for body_atom in &query.body_atoms {
        let terms_vector: Vec<&Term> = body_atom.terms.clone();
        ears.push(terms_vector);
    }

    // return the collected ears vector
    println!("collected_ears: {:?}", ears);
    ears
}
// remove all items unique to there ear(vector).
fn remove_unique_items(vectors: &mut Vec<Vec<&Term>>) {
    // create a HashSet for each vector
    let mut unique_items: Vec<HashSet<Term>> = vectors.iter().map(|_| HashSet::new()).collect();

    // iterate through all vectors to populate and update the HashSet
    for (vector_index, vector) in vectors.iter().enumerate() {
        for item in vector {
            // Clone the item to insert it into the HashSet
            let cloned_item = item.clone();
            unique_items[vector_index].insert(cloned_item.clone());
        }
    }

    // iterate through each vector and remove items that are unique to that vector
    for (vector_index, vector) in vectors.iter_mut().enumerate() {
        vector.retain(|item| {
            unique_items
                .iter()
                .enumerate()
                .filter(|&(i, set)| i != vector_index && set.contains(item))
                .count()
                > 0
        });
    }
}

// do not keep ears existing of one element
fn remove_single_item_vectors(vectors: &mut Vec<Vec<&Term>>) {
    vectors.retain(|vector| vector.len() > 1);
}
