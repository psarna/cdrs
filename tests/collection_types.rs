extern crate cdrs;
extern crate uuid;
extern crate time;
extern crate regex;
#[macro_use]
extern crate maplit;

mod common;

use common::*;

use uuid::Uuid;
use cdrs::query::QueryBuilder;
use cdrs::types::{IntoRustByName, AsRust};
use cdrs::types::value::{Value, Bytes};
use cdrs::types::list::List;
use cdrs::types::map::Map;
use cdrs::error::Result;
use cdrs::IntoBytes;

use std::str::FromStr;
use std::collections::HashMap;

#[test]
fn list() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_lists \
               (my_text_list frozen<list<text>> PRIMARY KEY, \
               my_nested_list list<frozen<list<smallint>>>)";
    let mut session = setup(cql).expect("setup");

    let my_text_list = vec!["text1".to_string(), "text2".to_string(), "text3".to_string()];
    let my_nested_list: Vec<Vec<i16>> = vec![
        vec![1, 2, 3],
        vec![999, 888, 777, 666, 555],
        vec![-1, -2],
    ];
    let values: Vec<Value> = vec![my_text_list.clone().into(), my_nested_list.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_lists \
               (my_text_list, my_nested_list) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_lists";
    let query = QueryBuilder::new(cql).finalize();
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_text_list_row: List = row.get_r_by_name("my_text_list").expect("my_text_list");
        let my_text_list_row: Vec<String> = my_text_list_row.as_r_rust().expect("my_text_list as rust");
        let my_nested_list_outer_row: List = row.get_r_by_name("my_nested_list").expect("my_nested_list");
        let my_nested_list_outer_row: Vec<List> = my_nested_list_outer_row.as_r_rust().expect("my_nested_list (outer) as rust");
        let mut my_nested_list_row = Vec::with_capacity(my_nested_list_outer_row.len());
        for my_nested_list_inner_row in my_nested_list_outer_row {
            let my_nested_list_inner_row: Vec<i16> = my_nested_list_inner_row.as_r_rust().expect("my_nested_list (inner) as rust");
            my_nested_list_row.push(my_nested_list_inner_row);
        }
        assert_eq!(my_text_list_row, my_text_list);
        assert_eq!(my_nested_list_row, my_nested_list);
    }
}

#[test]
fn set() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_sets \
               (my_text_set frozen<set<text>> PRIMARY KEY, \
               my_nested_set set<frozen<set<smallint>>>)";
    let mut session = setup(cql).expect("setup");

    let my_text_set = vec!["text1".to_string(), "text2".to_string(), "text3".to_string()];
    let my_nested_set: Vec<Vec<i16>> = vec![
        vec![-2, -1],
        vec![1, 2, 3],
        vec![555, 666, 777, 888, 999],
    ];
    let values: Vec<Value> = vec![my_text_set.clone().into(), my_nested_set.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_sets \
               (my_text_set, my_nested_set) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_sets";
    let query = QueryBuilder::new(cql).finalize();
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_text_set_row: List = row.get_r_by_name("my_text_set").expect("my_text_set");
        let my_text_set_row: Vec<String> = my_text_set_row.as_r_rust().expect("my_text_set as rust");
        let my_nested_set_outer_row: List = row.get_r_by_name("my_nested_set").expect("my_nested_set");
        let my_nested_set_outer_row: Vec<List> = my_nested_set_outer_row.as_r_rust().expect("my_nested_set (outer) as rust");
        let mut my_nested_set_row = Vec::with_capacity(my_nested_set_outer_row.len());
        for my_nested_set_inner_row in my_nested_set_outer_row {
            let my_nested_set_inner_row: Vec<i16> = my_nested_set_inner_row.as_r_rust().expect("my_nested_set (inner) as rust");
            my_nested_set_row.push(my_nested_set_inner_row);
        }
        assert_eq!(my_text_set_row, my_text_set);
        assert_eq!(my_nested_set_row, my_nested_set);
    }
}

#[test]
fn map_without_blob() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_maps_without_blob \
               (my_text_map frozen<map<text, text>> PRIMARY KEY, \
               my_nested_map map<uuid, frozen<map<bigint, tinyint>>>)";
    let mut session = setup(cql).expect("setup");

    let my_text_map = hashmap!{
        "key1".to_string() => "value1".to_string(),
        "key2".to_string() => "value2".to_string(),
        "key3".to_string() => "value3".to_string(),
    };
    let my_nested_map: HashMap<Uuid, HashMap<i64, i8>> = hashmap!{
        Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap() => hashmap!{
            1 => 1,
            2 => 2,
        },
        Uuid::from_str("687d7677-dbf0-4d25-8cf3-e5d9185bba0b").unwrap() => hashmap!{
            1 => 1,
        },
        Uuid::from_str("c4dc6e8b-758a-4af4-ab00-ec356fb688d9").unwrap() => hashmap!{
            1 => 1,
            2 => 2,
            3 => 3,
        },
    };
    let values: Vec<Value> = vec![my_text_map.clone().into(), my_nested_map.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_maps_without_blob \
               (my_text_map, my_nested_map) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_maps_without_blob";
    let query = QueryBuilder::new(cql).finalize();
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_text_map_row: Map = row.get_r_by_name("my_text_map").expect("my_text_map");
        let my_text_map_row: HashMap<String, String> = my_text_map_row.as_r_rust().expect("my_text_map as rust");
        let my_nested_map_outer_row: Map = row.get_r_by_name("my_nested_map").expect("my_nested_map");
        let my_nested_map_outer_row: HashMap<Uuid, Map> = my_nested_map_outer_row.as_r_rust().expect("my_nested_map (outer) as rust");
        let mut my_nested_map_row = HashMap::with_capacity(my_nested_map_outer_row.len());
        for (index, my_nested_map_inner_row) in my_nested_map_outer_row {
            let my_nested_map_inner_row: HashMap<i64, i8> = my_nested_map_inner_row.as_r_rust().expect("my_nested_map (inner) as rust");
            my_nested_map_row.insert(index, my_nested_map_inner_row);
        }
        assert_eq!(my_text_map_row, my_text_map);
        assert_eq!(my_nested_map_row, my_nested_map);
    }
}

#[test]
#[ignore]
fn map() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_maps \
               (my_text_map frozen<map<text, text>> PRIMARY KEY, \
               my_nested_map map<uuid, frozen<map<bigint, blob>>>)";
    let mut session = setup(cql).expect("setup");

    let my_text_map = hashmap!{
        "key1".to_string() => "value1".to_string(),
        "key2".to_string() => "value2".to_string(),
        "key3".to_string() => "value3".to_string(),
    };
    let my_nested_map: HashMap<Uuid, HashMap<i64, Vec<u8>>> = hashmap!{
        Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap() => hashmap!{
            1 => vec![52, 121, 209, 200, 81, 118, 181, 17],
            2 => vec![226, 90, 51, 10, 26, 87, 141, 61],
        },
        Uuid::from_str("687d7677-dbf0-4d25-8cf3-e5d9185bba0b").unwrap() => hashmap!{
            1 => vec![224, 155, 148, 6, 217, 96, 120, 38],
        },
        Uuid::from_str("c4dc6e8b-758a-4af4-ab00-ec356fb688d9").unwrap() => hashmap!{
            1 => vec![164, 238, 196, 10, 149, 169, 145, 239],
            2 => vec![250, 87, 119, 134, 105, 236, 240, 64],
            3 => vec![72, 81, 26, 173, 107, 96, 38, 91],
        },
    };
    let values: Vec<Value> = vec![my_text_map.clone().into(), my_nested_map.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_maps \
               (my_text_map, my_nested_map) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_maps";
    let query = QueryBuilder::new(cql).finalize();
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_text_map_row: Map = row.get_r_by_name("my_text_map").expect("my_text_map");
        let my_text_map_row: HashMap<String, String> = my_text_map_row.as_r_rust().expect("my_text_map as rust");
        let my_nested_map_outer_row: Map = row.get_r_by_name("my_nested_map").expect("my_nested_map");
        let my_nested_map_outer_row: HashMap<Uuid, Map> = my_nested_map_outer_row.as_r_rust().expect("my_nested_map (outer) as rust");
        let mut my_nested_map_row = HashMap::with_capacity(my_nested_map_outer_row.len());
        for (index, my_nested_map_inner_row) in my_nested_map_outer_row {
            let my_nested_map_inner_row: HashMap<i64, Vec<u8>> = my_nested_map_inner_row.as_r_rust().expect("my_nested_map (inner) as rust");
            my_nested_map_row.insert(index, my_nested_map_inner_row);
        }
        assert_eq!(my_text_map_row, my_text_map);
        assert_eq!(my_nested_map_row, my_nested_map);
    }
}
