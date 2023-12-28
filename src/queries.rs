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
    pub name: &'static str,
    pub terms: Vec<&'static Term>,
}

// Define a struct to represent a conjunctive query.
#[derive(Debug)]
pub struct ConjunctiveQuery {
    pub head_atom: Atom,
    pub body_atoms: Vec<Atom>,
}

// the rest of the code exist of functions to generate the queries
// this are all the possible columns used for the queries, 
// recordbatch is indexed on index extracted from column name.
/*
// -- beers --
let beer_id = &Term::Variable("beer_id");
let brew_id = &Term::Variable("brew_id");
let beer = &Term::Variable("beer");
let abv = &Term::Variable("abv");
let ibu = &Term::Variable("ibu");
let ounces = &Term::Variable("ounces");
let style = &Term::Variable("style");
let style2 = &Term::Variable("style2");
// -- breweries --
let brew_id = &Term::Variable("brew_id");
let brew_name = &Term::Variable("brew_name");
let address1 = &Term::Variable("address1");
let address2 = &Term::Variable("address2");
let city = &Term::Variable("city");
let state = &Term::Variable("state");
let code = &Term::Variable("code");
let country = &Term::Variable("country");
let phone = &Term::Variable("phone");
let website = &Term::Variable("website");
let description = &Term::Variable("description");
// -- categories --
let cat_id = &Term::Variable("cat_id");
let cat_name = &Term::Variable("cat_name");
// -- locations --
let loc_id = &Term::Variable("loc_id");
let brew_id = &Term::Variable("brew_id");
let latitude = &Term::Variable("latitude");
let longitude = &Term::Variable("longitude");
let accuracy = &Term::Variable("accuracy");
// -- styles --
let style_id = &Term::Variable("style_id");
let cat_id = &Term::Variable("cat_id");
let style = &Term::Variable("style");

 */

// cq1
pub fn create_cq1() -> ConjunctiveQuery {
    let u1 = &Term::Variable("u1");
    let x = &Term::Variable("x");
    let u2 = &Term::Variable("u2");
    let abv = &Term::Constant("0.07");
    let u3 = &Term::Variable("u3");
    let u4 = &Term::Variable("u4");
    let y = &Term::Variable("y");
    let u5 = &Term::Variable("u5");

    let beers = Atom {
        name: "Beers",
        terms: vec![u1, x, u2, abv, u3, u4, y, u5],
    };

    let u6 = &Term::Variable("u6");
    let z = &Term::Variable("z");

    let styles = Atom {
        name: "Styles",
        terms: vec![u6, z, y],
    };

    let u7 = &Term::Variable("u7");

    let categories = Atom {
        name: "Categories",
        terms: vec![z, u7],
    };

    let u8 = &Term::Variable("u8");
    let u9 = &Term::Variable("u9");
    let u10 = &Term::Variable("u10");
    let u11 = &Term::Variable("u11");

    let locations = Atom {
        name: "Locations",
        terms: vec![u8, x, u9, u10, u11],
    };

    let u12 = &Term::Variable("u12");
    let u13 = &Term::Variable("u13");
    let u14 = &Term::Variable("u14");
    let u15 = &Term::Variable("u15");
    let u16 = &Term::Variable("u16");
    let u17 = &Term::Variable("u17");
    let u18 = &Term::Variable("u18");

    let breweries = Atom {
        name: "Breweries",
        terms: vec![x, u12, u13, u14, u15, u16, u17, u18, u13, u14, u15],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![],
    };

    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![beers, styles, categories, locations, breweries],
    }
}

// cq2
pub fn create_cq2() -> ConjunctiveQuery {
    // -- breweries --
    let brew_id = &Term::Variable("brew_id");
    let brew_name = &Term::Variable("brew_name");
    let address1 = &Term::Constant("Westmalle");
    let address2 = &Term::Variable("address2");
    let city = &Term::Variable("city");
    let state = &Term::Variable("state");
    let code = &Term::Variable("code");
    let country = &Term::Variable("country");
    let phone = &Term::Variable("phone");
    let website = &Term::Variable("website");
    let description = &Term::Variable("description");

    let breweries = Atom {
        name: "Breweries",
        terms: vec![
            brew_id,
            brew_name,
            address1,
            address2,
            city,
            state,
            code,
            country,
            phone,
            website,
            description,
        ],
    };

    // -- locations --
    let loc_id = &Term::Variable("loc_id");
    let latitude = &Term::Variable("latitude");
    let longitude = &Term::Variable("longitude");
    let accuracy = &Term::Variable("accuracy");

    let locations = Atom {
        name: "Locations",
        terms: vec![loc_id, brew_id, latitude, longitude, accuracy],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![brew_name, latitude, longitude],
    };

    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![breweries, locations],
    }
}

// cq3
pub fn create_cq3() -> ConjunctiveQuery {
    let x = &Term::Variable("x");
    let z = &Term::Variable("z");
    let u1 = &Term::Variable("u1");
    let u2 = &Term::Variable("u2");
    let u3 = &Term::Variable("u3");
    let u4 = &Term::Variable("u4");
    let u5 = &Term::Variable("u5");
    let u6 = &Term::Variable("u6");

    let beers = Atom {
        name: "Beers",
        terms: vec![u1, u2, z, u3, u4, u5, x, u6],
    };

    let u7 = &Term::Variable("u7");
    let y = &Term::Variable("y");

    let styles = Atom {
        name: "Styles",
        terms: vec![u7, y, x],
    };

    let categories = Atom {
        name: "Categories",
        terms: vec![y, z],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![x, y, z],
    };

    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![beers, styles, categories],
    }
}

// cq4
pub fn create_cq4() -> ConjunctiveQuery {
    // -- beers --
    let beer_id = &Term::Variable("beer_id");
    let brew_id = &Term::Variable("brew_id");
    let beer = &Term::Variable("beer");
    let abv = &Term::Constant("0.05");
    let ibu = &Term::Constant("18");
    let ounces = &Term::Variable("ounces");
    let style = &Term::Constant("Vienna Lager");
    let style2 = &Term::Variable("style2");

    let beers = Atom {
        name: "Beers",
        terms: vec![beer_id, brew_id, beer, abv, ibu, ounces, style, style2],
    };

    // -- locations --
    let loc_id = &Term::Variable("loc_id");

    let latitude = &Term::Variable("latitude");
    let longitude = &Term::Variable("longitude");
    let accuracy = &Term::Variable("accuracy");

    let locations = Atom {
        name: "Locations",
        terms: vec![loc_id, brew_id, latitude, longitude, accuracy],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![beer, latitude, longitude, accuracy],
    };

    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![beers, locations],
    }
}

// cq5
pub fn create_cq5() -> ConjunctiveQuery {
    // only in -- beers --
    let beer_id = &Term::Variable("beer_id");
    let brew_id = &Term::Variable("brew_id");
    let beer = &Term::Variable("beer");
    let abv = &&Term::Constant("0.06");
    let ibu = &Term::Variable("ibu");
    let ounces = &Term::Variable("ounces");
    let style = &Term::Variable("style");
    let style2 = &Term::Variable("style2");
    // only in-- breweries --
    let brew_name = &Term::Variable("brew_name");
    let address1 = &Term::Variable("address1");
    let address2 = &Term::Variable("address2");
    let city = &Term::Variable("city");
    let state = &Term::Variable("state");
    let code = &Term::Variable("code");
    let country = &Term::Variable("country");
    let phone = &Term::Variable("phone");
    let website = &Term::Variable("website");
    let description = &Term::Variable("description");
    // only in -- categories --
    let cat_id = &Term::Variable("cat_id");
    let cat_name = &Term::Variable("cat_name");
    // only in -- locations --
    let loc_id = &Term::Variable("loc_id");
    let latitude = &Term::Variable("latitude");
    let longitude = &Term::Variable("longitude");
    let accuracy = &Term::Variable("accuracy");
    // only in -- styles --
    let style_id = &Term::Variable("style_id");

    let beers = Atom {
        name: "Beers",
        terms: vec![beer_id, brew_id, beer, abv, ibu, ounces, style, style2],
    };

    let styles = Atom {
        name: "Styles",
        terms: vec![style_id, cat_id, style],
    };

    let categories = Atom {
        name: "Categories",
        terms: vec![cat_id, cat_name],
    };

    let locations = Atom {
        name: "Locations",
        terms: vec![loc_id, brew_id, latitude, longitude, accuracy],
    };

    let breweries = Atom {
        name: "Breweries",
        terms: vec![
            brew_id,
            brew_name,
            address1,
            address2,
            city,
            state,
            code,
            country,
            phone,
            website,
            description,
        ],
    };

    let answer = Atom {
        name: "Answer",
        terms: vec![brew_id, style, cat_id, cat_name],
    };

    ConjunctiveQuery {
        head_atom: answer,
        body_atoms: vec![beers, styles, categories, locations, breweries],
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
        terms: vec![beer_id, brew_id, beer, abv, ibu, ounces, style, style2],
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
