// queries.rs

// Query data structure:
// Define a struct to represent a term, which can be a constant or a Utf8String.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Term {
    Constant(&'static str),
    Variable(&'static str),
}
// Define a struct to represent an atom with a relation name and a tuple of terms.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Atom {
    pub name: (&'static str),
    pub terms: Vec<&'static Term>,
}

// Define a struct to represent a conjunctive query.
#[derive(Debug)]
pub struct ConjunctiveQuery {
    pub head_atom: Atom,
    pub body_atoms: Vec<Atom>,
}

pub fn create_example_query() -> ConjunctiveQuery {
    // Varibales
    let beer_id = &Term::Variable("beer_id");
    let brew_id = &Term::Variable("brew_id");
    let beer = &Term::Variable("beer");
    let abv = &Term::Variable("abv");
    let ibu = &Term::Variable("ibu");
    let ounces = &Term::Variable("ounces");
    let style2 = &Term::Variable("style2");
    let style_id = &Term::Variable("style_id");
    let cat_id = &Term::Variable("cat_id");
    let style = &Term::Variable("style");
    // Constants
    let belfre = &Term::Constant("Belgian and French Ale");

    let answer = Atom {
        name: "Answer",
        terms: vec![],
    };
    let beers = Atom {
        name: "Beers",
        terms: vec![
            beer_id,
            brew_id,
            beer,
            abv,
            ibu,
            ounces,
            style,
            style2,
        ],
    };
    let styles = Atom {
        name: "Styles",
        terms: vec![style_id, cat_id, style],
    };
    let categories = Atom {
        name: "Categories",
        terms: vec![cat_id, belfre],
    };

    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![beers, styles, categories],
    }
}
// book page 156: The canonical example
// of an undirected graph that is not acyclic
pub fn create_cyclic_example_query() -> ConjunctiveQuery {
    let a = &Term::Variable("a");
    let b = &Term::Variable("b");
    let c = &Term::Variable("c");
    let d = &Term::Variable("d");
    let e = &Term::Variable("e");
    let f = &Term::Variable("f");

    let cyclic_query = Atom {
        name: "cyclic_query",
        terms: vec![],
    };
    let abc = Atom {
        name: "abc",
        terms: vec![a, b, c],
    };
    let bef = Atom {
        name: "bef",
        terms: vec![b, e, f],
    };
    let bc = Atom {
        name: "bc",
        terms: vec![b, c],
    };
    let cd = Atom {
        name: "cd",
        terms: vec![c, d],
    };
    let ce = Atom {
        name: "ce",
        terms: vec![c, e],
    };
    ConjunctiveQuery {
        head_atom: cyclic_query,
        body_atoms: vec![abc, bc, cd, bef, ce],
    }
}
