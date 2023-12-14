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

// the rest of the code exist of functions to generate the queries

// cq1
pub fn create_cq1() -> ConjunctiveQuery{
    let u1 = &Term::Variable("u1");
    let x = &Term::Variable("x");
    let u2 = &Term::Variable("u2");
    let abv = &Term::Constant("0.07");
    let u3 = &Term::Variable("u3");
    let u4 = &Term::Variable("u4");
    let y = &Term::Variable("y");
    let u5 = &Term::Variable("u5");

    let beers = Atom{
        name: "Beers",
        terms: vec![u1,x,u2,abv,u3,u4,y,u5]
    };

    let u6 = &Term::Variable("u6");
    let z = &Term::Variable("z");

    let styles = Atom{
        name: "Styles",
        terms: vec![u6,z,y]
    };
    
    let u7 = &Term::Variable("u7");

    let categories = Atom{
        name: "Categories",
        terms: vec![z,u7]
    };

    let u8 = &Term::Variable("u8");
    let u9 = &Term::Variable("u9");
    let u10 = &Term::Variable("u10");
    let u11 = &Term::Variable("u11");

    let locations = Atom{
        name: "Locations",
        terms: vec![u8,x,u9,u10,u11]
    };

    let u12 = &Term::Variable("u12");
    let u13 = &Term::Variable("u13");
    let u14 = &Term::Variable("u14");
    let u15 = &Term::Variable("u15");
    let u16 = &Term::Variable("u16");
    let u17 = &Term::Variable("u17");
    let u18 = &Term::Variable("u18");

    let breweries = Atom{
        name: "Breweries",
        terms: vec![x,u12,u13,u14,u15,u16,u17,u18,u13,u14,u15]
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![],
    };

    ConjunctiveQuery{
        head_atom: answer,
        body_atoms: vec![beers,styles,categories,locations,breweries],
    } 
}

// cq2
pub fn create_cq2() -> ConjunctiveQuery{
    let w = &Term::Variable("w");
    let x = &Term::Variable("x");
    let westmalle = &Term::Constant("Westmalle");
    let u1 = &Term::Variable("u1");
    let u2 = &Term::Variable("u2");
    let u3 = &Term::Variable("u3");
    let u4 = &Term::Variable("u4");
    let u5 = &Term::Variable("u5");
    let u6 = &Term::Variable("u6");
    let u7 = &Term::Variable("u7");
    let u8 = &Term::Variable("u8");

    let breweries = Atom{
        name: "Breweries",
        terms: vec![w,x,westmalle,u1,u2,u3,u4,u5,u6,u7,u8]
    };

    let u9 = &Term::Variable("u9");
    let y = &Term::Variable("y");
    let z = &Term::Variable("z");
    let u10 = &Term::Variable("u10");

    let locations = Atom{
        name: "Locations",
        terms: vec![u9,w,y,z,u10],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![x,y,z],
    };
   
     ConjunctiveQuery{
        head_atom: answer,
        body_atoms: vec![breweries,locations],
    } 
}

// cq3
pub fn create_cq3() -> ConjunctiveQuery{
    let x = &Term::Variable("x");
    let z = &Term::Variable("z");
    let u1 = &Term::Variable("u1");
    let u2 = &Term::Variable("u2");
    let u3 = &Term::Variable("u3");
    let u4 = &Term::Variable("u4");
    let u5 = &Term::Variable("u5");
    let u6 = &Term::Variable("u6");
  
    let beers = Atom{
        name: "Beers",
        terms: vec![u1,u2,z,u3,u4,u5,x,u6],
    };

    let u7 = &Term::Variable("u7");
    let y = &Term::Variable("y");
    
    let styles = Atom{
        name: "Styles",
        terms: vec![u7,y,x],
    };

    let categories = Atom{
        name: "Categories",
        terms: vec![y,z],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![x,y,z],
    };
   
     ConjunctiveQuery{
        head_atom: answer,
        body_atoms: vec![beers,styles,categories],
    } 
}

// cq4
pub fn create_cq4() -> ConjunctiveQuery{
    let u1 = &Term::Variable("u1");
    let v = &Term::Variable("v");
    let x = &Term::Variable("x");
    let abv = &Term::Constant("0.05");
    let ibu = &Term::Constant("18");
    let u2 = &Term::Variable("u2");
    let style = &Term::Constant("Vienna Larger");
    let u3 = &Term::Variable("u3");
     
    let beers = Atom{
        name: "Beers",
        terms: vec![u1,v,x,abv,ibu,u2,style,u3],
    };

    let u4 = &Term::Variable("u4");
    let y = &Term::Variable("y");
    let z = &Term::Variable("z");
    let w = &Term::Variable("w");
    
    let locations = Atom{
        name: "Locations",
        terms: vec![u4,v,y,z,w],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![x,y,z,w],
    };
   
     ConjunctiveQuery{
        head_atom: answer,
        body_atoms: vec![beers,locations],
    } 
}

// cq5
pub fn create_cq5() -> ConjunctiveQuery{
    let u1 = &Term::Variable("u1");
    let x = &Term::Variable("x");
    let u2 = &Term::Variable("u2");
    let abv = &Term::Constant("0.06");
    let u3 = &Term::Variable("u3");
    let u4 = &Term::Variable("u4");
    let y = &Term::Variable("y");
    let u5 = &Term::Variable("u5");

    let beers = Atom{
        name: "Beers",
        terms: vec![u1,x,u2,abv,u3,u4,y,u5]
    };

    let u6 = &Term::Variable("u6");
    let z = &Term::Variable("z");
    
    let styles = Atom{
        name: "Styles",
        terms: vec![u6,z,y]
    };
    
    let w = &Term::Variable("w");
    
    let categories = Atom{
        name: "Categories",
        terms: vec![z,w]
    };

    let u8 = &Term::Variable("u8");
    let u9 = &Term::Variable("u9");
    let u10 = &Term::Variable("u10");
    let u11 = &Term::Variable("u11");

    let locations = Atom{
        name: "Locations",
        terms: vec![u8,x,u9,u10,u11]
    };

    let u12 = &Term::Variable("u12");
    let u13 = &Term::Variable("u13");
    let u14 = &Term::Variable("u14");
    let u15 = &Term::Variable("u15");
    let u16 = &Term::Variable("u16");
    let u17 = &Term::Variable("u17");
    let u18 = &Term::Variable("u18");

    let breweries = Atom{
        name: "Breweries",
        terms: vec![x,u12,u13,u14,u15,u16,u17,u18,u13,u14,u15]
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![x,y,z,w],
    };

    ConjunctiveQuery{
        head_atom: answer,
        body_atoms: vec![beers,styles,categories,locations,breweries],
    } 
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
