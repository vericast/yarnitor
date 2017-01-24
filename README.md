Yarnitor is a pluggable YARN monitoring API and web frontend.

![Yarnitor screenshot](./screenshot.png)

## Requirements

Yarnitor relies on multiple processes, and is therefore best run using the
provided Docker configuration. You install Docker on
[Windows](https://docs.docker.com/docker-for-windows/),
[OSX](https://docs.docker.com/docker-for-mac/), or
[Linux](https://docs.docker.com/engine/installation/linux/). You'll
specifically need:

* docker>=1.10
* docker-compose>=1.10

## Configure

You must set two environment variables before starting yarnitor.

1. Set `YARN_ENDPOINT` to the YARN application master URL.
2. Set `EXPOSED_PORT` to the port on which yarnitor serve its UI.

For example:

```bash
export YARN_ENDPOINT=yarn-application-master.mydomain.tld:8088
export EXPOSED_PORT=8080
```

## Run

To launch the yarnitor web app, background YARN polling process, and Redis in
linked Docker containers, run the following:

```bash
docker-compose up
```

## Develop

To run the yarnitor web app with debugging enabled, the backgroudn YARN polling
process, and Redis in linked Docker containers for development, run the following:

```bash
make dev
```

If you don't have make, run what make would by hand instead:

```bash
docker-compose build
docker-compose run --rm \
    -e FLASK_APP=yarnitor \
    -e FLASK_DEBUG=1 \
    -p 5000:5000 \
    web \
    flask run -h 0.0.0.0
```

## Test

To run tests in a the Flask container, execute the following:

```bash
make test
```

Or the long-hand equivalent:

```bash
docker-compose build
docker-compose run --rm web pytest
```

## Credits

[Nanumo Park](https://www.linkedin.com/in/nanumo-park-8b3ba713) created the Yarnitor logo.
