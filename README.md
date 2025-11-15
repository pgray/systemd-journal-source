# systemd-journal-source

fluvio systemd journal source

[sample config](./sample-config.yaml)

# links

- [systemd crate being used](https://docs.rs/crate/systemd/latest)
- [fluvio docs](https://www.fluvio.io/docs/latest/cloud/how-to/use-connectors/)

# dev setup
```
# install fluvio
curl -fsS https://hub.infinyon.cloud/install/install.sh | bash
export PATH=$PATH:$HOME/.fluvio/bin

# in one terminal: create topic to consume before starting the source
topic=test-systemd-journal-source-topic
fluvio topic create $topic
fluvio consume $topic &

# in another terminal: start the source from root of repo
cargo run -- --config sample-config.yaml
```
