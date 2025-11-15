mod config;
use std::collections::HashMap;
use std::process;

use config::CustomConfig;

use systemd::{Journal, journal};

use fluvio::{RecordKey, TopicProducerPool};
use fluvio_connector_common::{Result, connector};

// Lookup allows us to filter units based on their names.
// if Lookup is empty, all units are allowed.
struct Lookup {
    units: HashMap<String, bool>,
    empty: bool,
}
impl Lookup {
    fn new(units: Vec<String>) -> Self {
        let mut map = HashMap::new();
        if units.len() == 0 {
            return Self {
                units: map,
                empty: true,
            };
        }
        for unit in units {
            // TODO: more patterns?
            if unit.ends_with(".service") || unit.ends_with(".timer") {
                map.insert(unit, true);
            } else {
                // subscribe to all matches non-suffixed units
                // TODO: more types of units to add here?
                map.insert(format!("{}.service", unit), true);
                map.insert(format!("{}.timer", unit), true);
            };
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
    let lookup = Lookup::new(config.units);

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
                if ent.contains_key("UNIT") {
                    if lookup.contains(&ent["UNIT"]) {
                        // TODO: serialize as json or other configurable format
                        for tag in config.tags.clone() {
                            if ent.contains_key(&tag) {
                                msg += &format!("{}:", ent[&tag])
                            }
                        }
                        producer.send(RecordKey::NULL, msg).await.unwrap();
                    }
                }
                if counter % config.flush_batch_size == 0 {
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
