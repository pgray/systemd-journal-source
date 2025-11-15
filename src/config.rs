use fluvio_connector_common::connector;

// TODO: make some of these optional/defaulted
#[connector(config)]
#[derive(Debug)]
pub(crate) struct CustomConfig {
    pub current_user: bool,
    pub system: bool,
    // only include entries from this host
    pub local_only: bool,
    // only include entries from this boot
    pub runtime_only: bool,
    // TODO: config variable to make tags filter records
    // e.g. drop entry unless it has all tags?
    pub tags: Vec<String>,
    pub units: Vec<String>,
    pub flush_batch_size: u64,
    pub unit_types: Option<Vec<String>>,
    pub serialization_format: Option<String>,
    pub debug: Option<bool>,
}
