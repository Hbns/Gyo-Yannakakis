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
leave the nodes from group2 in there "other" hyperedge.

Gyo reduction is performed on the body atoms of the conjunctive query.
*/

use crate::queries::{Atom, ConjunctiveQuery};

pub fn collect_ears(query: &ConjunctiveQuery) {
    // Iterate through body atoms
    for (i, body_atom) in query.body_atoms.iter().enumerate() {
        println!("Checking Body Atom {}: {:?}", i + 1, body_atom);
        // Call a function to find and print ears for each body atom
        find_and_print_ears(body_atom);
    }
}

pub fn find_and_print_ears(atom: &Atom) {
    // For simplicity, this function just prints the terms of the atom
    println!("Ears for {}: {:?}", atom.name, atom.terms);
    // You would implement the actual ear detection logic here
    // This could involve more complex analysis based on the specific criteria for ears
}