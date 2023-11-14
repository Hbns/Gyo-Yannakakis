// Join trees:

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
