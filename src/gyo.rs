// gyo - Graham-Yu-Ozsoyoglu

/*
Hypergraph: exists out of nodes and hyperedges.
Hyperedges: exists out of any number of nodes.
Ear:        exists out of two distinc groups of nodes.
Group1:     exists out of nodes that are unique to the hyperedge (not shared with other hyperedges)
Group2:     exists out of nodes that appear in other hyperedges.
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
Gyo reduction:
Repeatedly remove ears untill we can't remove anymore, if nothing left = acyclic.
Removing an ear means: remove the nodes from group1, remove that hyperedge (for which nodes in group1  where unique),
leave the nodes from group2 in their "other" hyperedge.

--> A hyperedge is an ear if all vertices are exclusive to thet hyperedge
OR there exists another hyperedge w such that every vertex in e is either exclusive to e or also occuring in w.

--> repeatedly apply the follwowing operations (in no particular order):
1. Delete a vertex that appears in at most one hyperedge.
2. Delete a hyperedge that is contained in another hyperedge.

Gyo reduction is performed on the body atoms of the conjunctive query.
*/

use crate::queries::{Atom, ConjunctiveQuery, Term};
use std::collections::HashSet;

pub fn collect_ears(query: &ConjunctiveQuery) -> Vec<Vec<Term>> {
    // Initialize a vector to store the terms vectors
    let mut ears: Vec<Vec<Term>> = Vec::new();

    // Iterate through body atoms
    for body_atom in &query.body_atoms {
        // Extract the terms vector from each body atom and add it to the ears vector
        let terms_vector: Vec<Term> = body_atom.terms.clone();
        ears.push(terms_vector);
    }

    // Return the collected ears vector
    ears
}

pub fn find_and_print_ears(atom: &Atom) {
    // For simplicity, this function just prints the terms of the atom
    println!("Ears for {}: {:?}", atom.name, atom.terms);
    // You would implement the actual ear detection logic here
    // This could involve more complex analysis based on the specific criteria for ears
}

pub fn remove_unique_items(vectors: &mut Vec<Vec<Term>>) {
    // Step 1: Create a HashSet for each vector
    let mut unique_items: Vec<HashSet<Term>> = vectors.iter().map(|_| HashSet::new()).collect();

    // Step 2: Iterate through all vectors to populate and update the HashSet
    for (vector_index, vector) in vectors.iter().enumerate() {
        for item in vector {
            unique_items[vector_index].insert(item.clone());
        }
    }

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
