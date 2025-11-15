use fluvio_connector_common::connector;

// TODO: make some of these optional/defaulted
#[connector(config)]
#[derive(Debug)]
pub(crate) struct CustomConfig {
    pub current_user: bool,
    pub system: bool,
    pub local_only: bool,
    pub runtime_only: bool,
    pub tags: Vec<String>,
    pub units: Vec<String>,
    pub flush_batch_size: u64,
    pub unit_types: Option<Vec<String>>,
    pub serialization_format: Option<String>,
    pub debug: bool,
}
