mod config;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::process;

use config::CustomConfig;

use systemd::{Journal, journal};

use fluvio::{RecordKey, TopicProducerPool};
use fluvio_connector_common::tracing::debug;
use fluvio_connector_common::{Result, connector};

// list of unit types to match if none specified on a given unit
// TODO: more patterns by default?
static UNIT_TYPES: &[&str] = &[".service", ".timer"];

// SerializationFormat represents the format in which journal entries are serialized into fluvio.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "serialization_format")]
enum SerializationFormat {
    Json,
    ColonDelimited,
}

// Lookup allows us to filter units based on their names.
// if Lookup is empty, all units are allowed.
struct Lookup {
    units: HashMap<String, bool>,
    empty: bool,
    serialization_format: SerializationFormat,
    tags: Vec<String>,
}
impl Lookup {
    fn new(
        units: Vec<String>,
        suffixes: Option<Vec<String>>,
        serialization_format: Option<String>,
        tags: Vec<String>,
    ) -> Self {
        let mut map = HashMap::new();
        let serialization_format = match serialization_format {
            Some(format) => match format.as_str() {
                "json" => SerializationFormat::Json,
                "colon" => SerializationFormat::ColonDelimited,
                _ => {
                    println!(
                        "Invalid serialization format specified: {}. Defaulting to JSON.",
                        format
                    );
                    SerializationFormat::Json
                }
            },
            None => SerializationFormat::Json,
        };
        if units.is_empty() {
            return Self {
                units: map,
                empty: true,
                serialization_format,
                tags,
            };
        }
        let suffixes = match suffixes {
            Some(suffixes) => suffixes,
            None => UNIT_TYPES.iter().map(|f| f.to_string()).collect(),
        };
        for unit in units {
            let mut has_suffix = false;
            for suffix in &suffixes {
                if unit.ends_with(suffix) {
                    has_suffix = true;
                    break;
                }
            }
            if has_suffix {
                map.insert(unit.clone(), true);
            } else {
                for suffix in UNIT_TYPES {
                    map.insert(format!("{}{}", unit, suffix), true);
                }
            }
        }
        Self {
            units: map,
            empty: false,
            serialization_format,
            tags,
        }
    }
    fn contains(&self, unit: &str) -> bool {
        match self.empty {
            true => true,
            false => self.units.contains_key(unit),
        }
    }
    fn process(&self, timestamp: u64, entry: &BTreeMap<String, String>) -> Option<Vec<u8>> {
        if entry.contains_key("UNIT") && self.contains(&entry["UNIT"]) {
            match self.serialization_format {
                SerializationFormat::ColonDelimited => {
                    let mut msg = format!("{}:", timestamp);
                    for tag in &self.tags {
                        if entry.contains_key(&tag.to_string()) {
                            msg += &format!("{}:", entry[&tag.to_string()])
                        }
                    }
                    return Some(msg.into_bytes());
                }
                SerializationFormat::Json => {
                    let mut msg = serde_json::Map::new();
                    // TODO: make timestamp field name configurable for json?
                    msg.insert("ts".to_string(), json!(timestamp));
                    for tag in &self.tags {
                        if entry.contains_key(&tag.to_string()) {
                            msg.insert(
                                tag.to_string(),
                                // TODO: handle more types than just strings
                                // e.g. PRIORITY is an int
                                json!(entry[&tag.to_string()].clone()),
                            );
                        }
                    }
                    return Some(serde_json::to_vec(&msg).unwrap());
                }
            }
        }
        None
    }
}

#[connector(source)]
async fn start(config: CustomConfig, producer: TopicProducerPool) -> Result<()> {
    // configure journal source
    let mut journal: Journal = journal::OpenOptions::default()
        .current_user(config.current_user)
        .system(config.system)
        .local_only(config.local_only)
        .runtime_only(config.runtime_only)
        .open()
        .unwrap();

    // setup lookup table for units
    let lookup = Lookup::new(
        config.units,
        config.unit_types,
        config.serialization_format,
        config.tags,
    );

    let debug = config.debug.is_some() && config.debug.unwrap();
    let mut counter: u64 = 0;
    let mut timestamp: u64 = 0;
    // TODO: implement a way to track the last processed entry
    // config.reprocess bool to indicate whether we should resend
    // entries on restart... or some other better name
    loop {
        match journal.next_entry() {
            Ok(None) => match journal.wait(None) {
                Ok(_) => continue,
                Err(_) => break,
            },
            Ok(Some(ent)) => {
                // TODO: make timestamp field configurable to some format e.g. ISO8601?
                match journal.timestamp_usec() {
                    Ok(usec) => {
                        timestamp = usec;
                    }
                    Err(e) => {
                        eprintln!("Error getting timestamp: {}", e);
                        // HACK: increment usec timestamp if failed call to journal
                        timestamp += 1;
                    }
                };
                // TODO: real logger would be nice
                if debug {
                    for i in ent.keys() {
                        println!("{}:{}: {}", timestamp, i, ent[i])
                    }
                }
                match lookup.process(timestamp, &ent) {
                    None => (),
                    Some(buf) => {
                        producer.send(RecordKey::NULL, buf).await.unwrap();
                        counter += 1;
                        if counter.is_multiple_of(config.flush_batch_size) {
                            match producer.flush().await {
                                Ok(_) => {
                                    // TODO: write timestamp to some file to avoid reprocessing
                                    debug!("Flushed producer");
                                }
                                Err(e) => eprintln!("Error flushing producer: {}", e),
                            }
                        }
                    }
                };
            }
            Err(e) => {
                // TODO: handle error more cleanly
                eprintln!("{}", e);
                process::exit(1);
            }
        };
    }
    Ok(())
}
