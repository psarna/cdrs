use std::option::*;
use std::collections::HashMap;

pub const SCYLLA_SHARD: &str = "SCYLLA_SHARD";
pub const SCYLLA_NR_SHARDS: &str = "SCYLLA_NR_SHARDS";
pub const SCYLLA_SHARDING_IGNORE_MSB: &str = "SCYLLA_SHARDING_IGNORE_MSB";
pub const SCYLLA_PARTITIONER: &str = "SCYLLA_PARTITIONER";
pub const SCYLLA_SHARDING_ALGORITHM: &str = "SCYLLA_SHARDING_ALGORITHM";

#[derive(Debug)]
pub struct ShardingInfo {
    shard_id: i32,
    shards_count: i32,
    partitioner: String,
    sharding_algorithm: String,
    ignore_msb: i32,
}

impl ShardingInfo {
    pub fn parse(options : &HashMap<String, Vec<String>>) -> Option<ShardingInfo> {
        let parsed_shard_id = match options.get(&SCYLLA_SHARD.to_string())?[0].parse::<i32>() {
            Ok(shard_count) => shard_count,
            _ => return None,
        };
        let parsed_shards_count = match options.get(&SCYLLA_NR_SHARDS.to_string())?[0].parse::<i32>() {
            Ok(shard_count) => shard_count,
            _ => return None,
        };
        let parsed_ignore_msb = match options.get(&SCYLLA_SHARDING_IGNORE_MSB.to_string())?[0].parse::<i32>() {
            Ok(ignore_msb_value) => ignore_msb_value,
            _ => return None,
        };
        Some(ShardingInfo {
            shard_id: parsed_shard_id,
            shards_count: parsed_shards_count,
            partitioner: options.get(&SCYLLA_PARTITIONER.to_string())?[0].clone(),
            sharding_algorithm: options.get(&SCYLLA_SHARDING_ALGORITHM.to_string())?[0].clone(),
            ignore_msb: parsed_ignore_msb,
        })
    }
}
