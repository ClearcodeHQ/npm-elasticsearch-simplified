# Elasticsearch connector

Simplifies asynchronous connection to Elasticsearch with connection retrying.

If connection cannot be established, a new attempt will be made after a delay set in config (5 seconds by default). Every subsequent retry will occur after two times as long (so by default 10, 20, 40, 80 etc seconds). You can set how many times connection will be retried (10 by default).

## Installation

Add to your dependencies:

```
"dependencies": {
    "elasticsearch-simplified": "https://github.com/ClearcodeHQ/npm-elasticsearch-simplified"
}
```

# Usage

```
const EsConnector = require("elasticsearch-simplified");

// You can, but don't have to pass the config array or any of its values
const config = {
    host: 'elasticsearch', // by default process.env.ELASTIC_HOST and when no specified - 'localhost'
    port: 9200, // by default process.env.ELASTIC_P1 and when no specified - 9200
    maxElasticsearchConnectionRetries: 15, // by default 10
    retryAfter: 1000, // in miliseconds, by default 5000; every subsequent requests has twice as long waiting time
}

const Connector = new EsConnector(config);

// The returned value can be an instance of official Elasticsearch JS
// API client if the connection was successful, or null if it couldn't
// be obtained under constraints specified in config
const client = await Connector.connectToElasticsearch();
```
All configuration options are available in [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html),
following options:
* maxElasticsearchConnectionRetries
* retryAfter

are unique for this library.
