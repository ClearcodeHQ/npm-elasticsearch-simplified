'use strict';

const timeout = require('@clearcodehq/synchronous-timeout');

const elasticsearch = require('@elastic/elasticsearch');

class Connector {

  constructor(userConfig) {
    this.elasticsearchConnectionRetries = 0;
    this.maxElasticsearchConnectionRetries = 10;
    this.retryAfter = 5000;

    this.config = {
      node: [`${process.env.ELASTIC_HOST || 'http://localhost'}:${process.env.ELASTIC_P1 || '9200'}`],
    };

    this._setupConfig(userConfig);
  }

  _setupConfig(userConfig) {
    if (!userConfig) {
      return;
    }

    if (typeof userConfig.maxElasticsearchConnectionRetries === 'number') {
      this.maxElasticsearchConnectionRetries = userConfig.maxElasticsearchConnectionRetries;
      delete userConfig.maxElasticsearchConnectionRetries;
    }

    if (typeof userConfig.retryAfter === 'number') {
      this.retryAfter = userConfig.retryAfter;
      delete userConfig.retryAfter;
    }

    if (
      typeof userConfig.node === 'string' && userConfig.node.trim().length > 0
      && typeof userConfig.port === 'number'
    ) {
      userConfig.node = userConfig.node.split(',').map(node => this.nodeMapper(node, userConfig.port));
      delete userConfig.port;
    }

    if (
      typeof userConfig.nodes === 'string' && userConfig.nodes.trim().length > 0
      && typeof userConfig.port === 'number'
    ) {
      userConfig.nodes = userConfig.nodes.split(',').map(node => this.nodeMapper(node, userConfig.port));
      delete userConfig.port;
    }

    Object.assign(this.config, userConfig);
  }

  async connectToElasticsearch(retryAfter) {
    console.log('Connecting to Elasticsearch');
    let _self = this;

    if (retryAfter) {
      await timeout(retryAfter);
      this.retryAfter *= 2;
    }

    // Cloning config object to avoid https://github.com/elastic/elasticsearch-js/issues/33
    let client = new elasticsearch.Client(Object.assign({}, this.config));

    return client.ping()
      .then(function() {
        client.transport.connectionPool._conns.alive.forEach(function(aliveConnection) {
          console.log(
            `Connected to Elasticsearch node: id: ${aliveConnection.id}, status: ${aliveConnection.status}`,
          );
        });
        return client;
      })
      .catch(function(exception) {
        console.warn('Elasticsearch connection error', exception.stack);
        if (_self.elasticsearchConnectionRetries === _self.maxElasticsearchConnectionRetries) {
          console.error(
            `Maximum connection retries of ${_self.maxElasticsearchConnectionRetries} to Elasticsearch reached, dying.`,
          );
          return null;
        } else {
          console.warn(`Retrying connection to Elasticsearch after delay of ${_self.retryAfter} ms`);
          _self.elasticsearchConnectionRetries++;
          return _self.connectToElasticsearch(_self.retryAfter);
        }
      });
  }

  getConnectionRetryCount() {
    return this.elasticsearchConnectionRetries;
  }

  nodeMapper(node, defaultPort) {
    if (/\:[\d]{1,5}/.test(node)) {
      return node;
    } else {
      return `${node}:${defaultPort}`;
    }
  }
}

module.exports = Connector;
