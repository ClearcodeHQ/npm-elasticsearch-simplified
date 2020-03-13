'use strict';

const assert = require('chai').assert;
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();
const ElasticsearchConnector = require('./../../elasticsearch-connector');

let FakeClient;
let timeout;
let EsConnector;

describe('Elasticsearch connector', async function() {
  beforeEach(function() {
    timeout = sinon.spy();

    FakeClient = sinon.stub();
    FakeClient.prototype.request = sinon.stub();
    FakeClient.prototype.transport = sinon.stub();
    FakeClient.prototype.transport.connectionPool = sinon.stub();
    FakeClient.prototype.transport.connectionPool._conns = sinon.stub();
    FakeClient.prototype.transport.connectionPool._conns.alive = [
      {id: 1, status: 'ok'},
      {id: 2, status: 'not ok'},
    ];

    EsConnector = proxyquire('./../../elasticsearch-connector.js', {
      '@clearcodehq/synchronous-timeout': timeout,
      '@elastic/elasticsearch': {Client: FakeClient},
    });
  });

  describe('Class init', () => {
    it('Class object should be initialized', async function() {
      let connector = new ElasticsearchConnector();
      assert.isTrue(typeof connector === 'object');
    });
  });

  describe('#Connector.connectToElasticsearch', function() {
    it('Should not wait if not specified', async function() {
      FakeClient.prototype.ping = sinon.stub().returns(Promise.resolve());

      const Connector = new EsConnector();
      await Connector.connectToElasticsearch();

      assert.isFalse(timeout.called);
    });

    it('Should wait for specified amount if specified', async function() {
      FakeClient.prototype.ping = sinon.stub().returns(Promise.resolve());

      const Connector = new EsConnector();
      await Connector.connectToElasticsearch(5000);

      assert.isTrue(timeout.called);
      assert.isTrue(timeout.calledWith(5000));
    });

    it('Should double the wait time for subsequent calls', async function() {
      FakeClient.prototype.ping = sinon.stub();
      FakeClient.prototype.ping.onCall(0).returns(Promise.reject({stack: ''}));
      FakeClient.prototype.ping.onCall(1).returns(Promise.reject({stack: ''}));
      FakeClient.prototype.ping.returns(Promise.resolve());

      const Connector = new EsConnector();
      await Connector.connectToElasticsearch();

      let firstTimeout = timeout.withArgs(5000);
      let secondTimeout = timeout.withArgs(10000);

      assert.isTrue(firstTimeout.calledOnce);
      assert.isTrue(secondTimeout.calledOnce);
      assert.isTrue(secondTimeout.calledAfter(firstTimeout));
    });

    it('Should try to reconnect if connection failed', async function() {
      FakeClient.prototype.ping = sinon.stub();
      FakeClient.prototype.ping.onCall(0).returns(Promise.reject({stack: ''}));
      FakeClient.prototype.ping.returns(Promise.resolve());

      const Connector = new EsConnector();
      const result = await Connector.connectToElasticsearch();

      assert.equal(Connector.getConnectionRetryCount(), 1);
      assert.isObject(result);
    });

    it('Should return null if connection retry limit was reached', async function() {
      FakeClient.prototype.ping = sinon.stub();
      FakeClient.prototype.ping.onCall(0).returns(Promise.reject({stack: ''}));
      FakeClient.prototype.ping.returns(Promise.resolve());

      const Connector = new EsConnector({maxElasticsearchConnectionRetries: 0});
      const result = await Connector.connectToElasticsearch();

      assert.equal(Connector.getConnectionRetryCount(), 0);
      assert.isNull(result);
    });
  });

  describe('#Connector._setupConfig', function() {
    beforeEach(function() {
      FakeClient.prototype.ping = sinon.stub().returns(Promise.resolve());
    });

    it('Should pass to elasticsearch client default host when user config not exists', async function() {
      const Connector = new EsConnector();
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], {node: ['http://localhost:9200']});
    });

    it('Should not pass to elasticsearch client specific options: elasticsearchConnectionRetries' +
      ' and retryAfter, but register them as internal variables', async function() {
      let config = {
        maxElasticsearchConnectionRetries: 5,
        retryAfter: 7000,
      };
      const Connector = new EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], {node: ['http://localhost:9200']});
      assert.equal(Connector.maxElasticsearchConnectionRetries, config.maxElasticsearchConnectionRetries);
      assert.equal(Connector.retryAfter, config.retryAfter);
    });

    it('Should pass host string and port number', async function() {
      let config = {
        node: 'http://host123',
        port: 9300,
      };
      const Connector = new EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], {node: ['http://host123:9300']});
    });

    it('Should pass many hosts as string and one port number', async function() {
      let config = {
        node: 'http://host1,http://host2',
        port: 9300,
      };
      const Connector = new EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], {node: ['http://host1:9300', 'http://host2:9300']});
    });

    it('Should pass many hosts with ports as string and skip default port config', async function() {
      let config = {
        node: 'http://host1:1234,http://host2:4321',
        port: 9300,
      };
      const Connector = new EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], {node: ['http://host1:1234', 'http://host2:4321']});
    });

    it('Should pass host/hosts string array', async function() {
      let config = {
        node: ['http://host1:9200', 'http://host2:9201'],
        nodes: ['http://host3:9202', 'http://host4:9203'],
      };
      const Connector = new EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], {
        node: ['http://host1:9200', 'http://host2:9201'],
        nodes: ['http://host3:9202', 'http://host4:9203'],
      });
    });

    it('Should pass host/hosts object', async function() {
      let config = {
        node: [{host: 'http://host1', port: '9200'}, {host: 'http://host2', port: '9201'}],
        nodes: [{host: 'http://host3', port: '9202'}, {host: 'http://host4', port: '9203'}],
      };
      const Connector = new EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], config);
    });

    it('Should pass pingTimeout option to ES config', async function() {
      let config = {
        pingTimeout: 100,
      };
      const Connector = new EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(FakeClient.args[0][0], {node: ['http://localhost:9200'], pingTimeout: config.pingTimeout});
    });
  });
});
