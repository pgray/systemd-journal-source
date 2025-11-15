mod config;
use std::collections::HashMap;
use std::process;

use config::CustomConfig;

use systemd::{Journal, journal};

use fluvio::{RecordKey, TopicProducerPool};
use fluvio_connector_common::{Result, connector};

// list of unit types to match if none specified on a given unit
// TODO: more patterns by default?
static UNIT_TYPES: &[&str] = &[".service", ".timer"];
// Lookup allows us to filter units based on their names.
// if Lookup is empty, all units are allowed.
struct Lookup {
    units: HashMap<String, bool>,
    empty: bool,
}
impl Lookup {
    fn new(units: Vec<String>, suffixes: Option<Vec<String>>) -> Self {
        let mut map = HashMap::new();
        if units.is_empty() {
            return Self {
                units: map,
                empty: true,
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
        }
    }
    fn contains(&self, unit: &str) -> bool {
        match self.empty {
            true => true,
            false => self.units.contains_key(unit),
        }
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
    let lookup = Lookup::new(config.units, config.default_unit_types);

    // do work
    let mut counter: u64 = 0;
    // TODO: implement a way to track the last processed entry
    // config.reprocess bool to indicate whether we should resend
    // entries on restart... or some other better name
    loop {
        counter += 1;
        match journal.next_entry() {
            Ok(None) => match journal.wait(None) {
                Ok(_) => continue,
                Err(_) => break,
            },
            Ok(Some(ent)) => {
                // TODO: real logger would be nice
                if config.debug {
                    for i in ent.keys() {
                        println!("{}: {}", i, ent[i])
                    }
                }
                let mut msg = "".to_string();
                if ent.contains_key("UNIT") && lookup.contains(&ent["UNIT"]) {
                    // TODO: serialize as json or other configurable format
                    for tag in config.tags.clone() {
                        if ent.contains_key(&tag) {
                            msg += &format!("{}:", ent[&tag])
                        }
                    }
                    producer.send(RecordKey::NULL, msg).await.unwrap();
                }
                if counter.is_multiple_of(config.flush_batch_size) {
                    producer.flush().await.unwrap();
                }
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
