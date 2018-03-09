'use strict';

const timeout = require('synchronous-timeout');

const elasticsearch = require('elasticsearch');

function Connector(userConfig) {
  let elasticsearchConnectionRetries = 0;

  let config = {
    host: process.env.ELASTIC_HOST,
    port: process.env.ELASTIC_P1,
    maxElasticsearchConnectionRetries: 10,
    retryAfter: 5000,
    requestTimeout: 3000,
  };

  if (userConfig) {
    Object.assign(config, userConfig);
  }

  async function connectToElasticsearch(retryAfter) {
    console.log('Connecting to Elasticsearch');

    if (retryAfter) {
      await timeout(retryAfter);
      config.retryAfter *= 2;
    }

    let client = new elasticsearch.Client({
      host: `${config.host}:${config.port}`,
    });

    return client.ping({
      requestTimeout: config.requestTimeout,
    })
      .then(function() {
        console.log('Connected to Elasticsearch');
        return client;
      })
      .catch(function(exception) {
        console.warn('Elasticsearch connection error', exception.stack);
        if (elasticsearchConnectionRetries === config.maxElasticsearchConnectionRetries) {
          console.error(
            `Maximum connection retries of ${config.maxElasticsearchConnectionRetries} to Elasticsearch reached, dying.`
          );
          return null;
        } else {
          console.warn(`Retrying connection to Elasticsearch after delay of ${config.retryAfter} ms`);
          elasticsearchConnectionRetries++;
          return connectToElasticsearch(config.retryAfter);
        }
      });
  }

  function getConnectionRetryCount() {
    return elasticsearchConnectionRetries;
  }

  this.connectToElasticsearch = connectToElasticsearch;
  this.getConnectionRetryCount = getConnectionRetryCount;
}

module.exports = Connector;
