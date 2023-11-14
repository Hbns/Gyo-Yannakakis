// queries.rs

// Query data structure:
// Define a struct to represent a term, which can be a constant or a Utf8String.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Term {
    Utf8String(String),
    Integer(i64),
}
// Define a struct to represent an atom with a relation name and a tuple of terms.
#[derive(Debug)]
pub struct Atom {
    pub name: String,
    pub terms: Vec<Term>,
}

// Define a struct to represent a conjunctive query.
#[derive(Debug)]
pub struct ConjunctiveQuery {
    pub head_atom: Atom,
    pub body_atoms: Vec<Atom>,
}

pub fn create_example_query() -> ConjunctiveQuery {
    let vbeer_id = Term::Utf8String("beer_id".to_string());
    let vbrew_id = Term::Utf8String("brew_id".to_string());
    let vbeer = Term::Utf8String("beer".to_string());
    let vabv = Term::Utf8String("abv".to_string());
    let vibu = Term::Utf8String("ibu".to_string());
    let vounces = Term::Utf8String("ounces".to_string());
    let vstyle2 = Term::Utf8String("style2".to_string());
    let vstyle_id = Term::Utf8String("style_id".to_string());
    let vcat_id = Term::Utf8String("cat_id".to_string());
    let vstyle = Term::Utf8String("style".to_string());
    let cbelfre = Term::Utf8String("Belgian and French Ale".to_string());

    let answer = Atom {
        name: "Answer".to_string(),
        terms: vec![],
    };
    let beers = Atom {
        name: "Beers".to_string(),
        terms: vec![
            vbeer_id,
            vbrew_id,
            vbeer,
            vabv,
            vibu,
            vounces,
            vstyle.clone(),
            vstyle2,
        ],
    };
    let styles = Atom {
        name: "Styles".to_string(),
        terms: vec![vstyle_id, vcat_id.clone(), vstyle.clone()],
    };
    let categories = Atom {
        name: "Categories".to_string(),
        terms: vec![vcat_id.clone(), cbelfre],
    };

    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![beers, styles, categories],
    }
}
// book page 156: The canonical example
// of an undirected graph that is not acyclic
pub fn create_cyclic_example_query() -> ConjunctiveQuery {
    let a = Term::Utf8String("a".to_string());
    let b = Term::Utf8String("b".to_string());
    let c = Term::Utf8String("c".to_string());
    let d = Term::Utf8String("d".to_string());
    let e = Term::Utf8String("e".to_string());
    let f = Term::Utf8String("f".to_string());

    let answer = Atom {
        name: "Answer".to_string(),
        terms: vec![],
    };
    let abc = Atom {
        name: "abc".to_string(),
        terms: vec![a, b.clone(), c.clone()],
    };
    let bef = Atom {
        name: "bef".to_string(),
        terms: vec![b.clone(), e.clone(), f],
    };
    let bc = Atom {
        name: "bc".to_string(),
        terms: vec![b, c.clone()],
    };
    let cd = Atom {
        name: "cd".to_string(),
        terms: vec![c.clone(), d],
    };
    let ce = Atom {
        name: "ce".to_string(),
        terms: vec![c.clone(), e.clone()],
    };
    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![abc, bc, cd, bef, ce],
    }
}
