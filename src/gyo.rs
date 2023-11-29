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

pub fn acyclic_test(query: &ConjunctiveQuery) {
    // make mutable vector containing all ears.
    let mut ears = collect_ears(&query);

    // Perform GYO:

    // Delete all vertex that appears in at most one hyperedge.
    remove_unique_items(&mut ears);
    // Delete a hyperedge that is contained in another hyperedge.
    remove_single_item_vectors(&mut ears);
    // Delete all vertex that appears in at most one hyperedge.
    remove_unique_items(&mut ears);

    // print!("{:?}", ears);
    // print (a)cyclic depending on items left in the ears vector
    if ears.iter().any(|vector| !vector.is_empty()) {
        println!("cyclic");
    } else {
        println!("acyclic");
    }
}

pub fn jt(query: &ConjunctiveQuery){
    let mut ears = collect_ears(&query);

    // Perform GYO:

    // Delete all vertex that appears in at most one hyperedge.
    remove_unique_items(&mut ears);
    println!("unique_removed: {:?}", ears)
}

fn collect_ears(query: &ConjunctiveQuery) -> Vec<Vec<&Term>> {
    // Initialize a vector to store the terms vectors
    let mut ears: Vec<Vec<&Term>> = Vec::new();

    // Iterate through body atoms
    for body_atom in &query.body_atoms {
        // Extract the terms vector from each body atom and add it to the ears vector
        let terms_vector: Vec<&Term> = body_atom.terms.clone();
        ears.push(terms_vector);
    }

    // Return the collected ears vector
    ears
}

fn remove_unique_items(vectors: &mut Vec<Vec<&Term>>) {
    // Step 1: Create a HashSet for each vector
    let mut unique_items: Vec<HashSet<Term>> = vectors.iter().map(|_| HashSet::new()).collect();

    // Step 2: Iterate through all vectors to populate and update the HashSet
    for (vector_index, vector) in vectors.iter().enumerate() {
        for item in vector {
            // Clone the item to insert it into the HashSet
            let cloned_item = item.clone();
            unique_items[vector_index].insert(cloned_item.clone());
        }
    }

    // Now you have unique_items populated with owned values of Term
    // You can continue with the rest of your logic...


    // Step 3: Iterate through each vector and remove items that are unique to that vector
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

fn remove_single_item_vectors(vectors: &mut Vec<Vec<&Term>>) {
    // Step 1: Find vectors of size one
    let single_item_vectors: Vec<_> = vectors
        .iter()
        .filter(|vector| vector.len() == 1)
        .cloned()
        .collect();

    // Step 2: Create a HashSet of items in vectors with size > 1
    let items_in_multi_item_vectors: HashSet<_> = vectors
        .iter()
        .filter(|vector| vector.len() > 1)
        .flat_map(|vector| vector.iter().cloned())
        .collect();

    // Step 3: Remove vectors of size one if the item exists in another vector
    vectors.retain(|vector| {
        if vector.len() == 1 {
            let item = &vector[0];
            !items_in_multi_item_vectors.contains(item)
        } else {
            true
        }
    });
}