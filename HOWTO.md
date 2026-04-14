# lsports

This codebase pulls fixtures, scores, leagues, and outright data from the LSports API, stores it in MySQL, and can also consume the live RabbitMQ feed.

## Setup

1. Fill in `.env` with your local MySQL, RabbitMQ, MQTT, memcached, and LSports credentials.
2. Make sure the required services are reachable.
3. Run the Python scripts from this directory.

## Common commands

- `./lsports_misc.sh` updates sports, locations, and leagues.
- `./lsports_scores_fixtures.sh` updates fixtures and scores.
- `./lsports_orlf.sh` updates outright leagues and outright fixtures.
- `./lsports_client.sh` starts the local feed consumer.
- `./lsports_shovel.sh` starts the shovel that copies the LSports feed into the local queue.
