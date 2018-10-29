'use strict';

const assert = require('chai').assert;
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

beforeEach(function() {
  global.timeout = sinon.spy();

  global.FakeClient = sinon.stub();
  FakeClient.prototype.create = sinon.stub();
  FakeClient.prototype.transport = sinon.stub();
  FakeClient.prototype.transport.connectionPool = sinon.stub();
  FakeClient.prototype.transport.connectionPool._conns = sinon.stub();
  FakeClient.prototype.transport.connectionPool._conns.alive = [
    {id: 1, status: 'ok'},
    {id: 2, status: 'not ok'},
  ];

  global.EsConnector = proxyquire('./../../elasticsearch-connector.js', {
    '@clearcodehq/synchronous-timeout': timeout,
    elasticsearch: {Client: FakeClient},
  });
});

describe('Elasticsearch connector', async function() {
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
      global.FakeClient.prototype.ping = sinon.stub().returns(Promise.resolve());
    });

    it('Should pass to elasticsearch client default host when user config not exists', async function() {
      const Connector = new global.EsConnector();
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], {host: ['localhost:9200']});
    });

    it('Should not pass to elasticsearch client specific options: elasticsearchConnectionRetries' +
      ' and retryAfter, but register them as internal variables', async function() {
      let config = {
        maxElasticsearchConnectionRetries: 5,
        retryAfter: 7000,
      };
      const Connector = new global.EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], {host: ['localhost:9200']});
      assert.equal(Connector.maxElasticsearchConnectionRetries, config.maxElasticsearchConnectionRetries);
      assert.equal(Connector.retryAfter, config.retryAfter);
    });

    it('Should pass host string and port number', async function() {
      let config = {
        host: 'host123',
        port: 9300,
      };
      const Connector = new global.EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], {host: ['host123:9300']});
    });

    it('Should pass many hosts as string and one port number', async function() {
      let config = {
        host: 'host1,host2',
        port: 9300,
      };
      const Connector = new global.EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], {host: ['host1:9300', 'host2:9300']});
    });

    it('Should pass many hosts with ports as string and skip default port config', async function() {
      let config = {
        host: 'host1:1234,host2:4321',
        port: 9300,
      };
      const Connector = new global.EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], {host: ['host1:1234', 'host2:4321']});
    });

    it('Should pass host/hosts string array', async function() {
      let config = {
        host: ['host1:9200', 'host2:9201'],
        hosts: ['host3:9202', 'host4:9203'],
      };
      const Connector = new global.EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], {
        host: ['host1:9200', 'host2:9201'],
        hosts: ['host3:9202', 'host4:9203'],
      });
    });

    it('Should pass host/hosts object', async function() {
      let config = {
        host: [{host: 'host1', port: '9200'}, {host: 'host2', port: '9201'}],
        hosts: [{host: 'host3', port: '9202'}, {host: 'host4', port: '9203'}],
      };
      const Connector = new global.EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], config);
    });

    it('Should pass pingTimeout option to ES config', async function() {
      let config = {
        pingTimeout: 100,
      };
      const Connector = new global.EsConnector(Object.assign({}, config));
      await Connector.connectToElasticsearch();

      assert.deepEqual(global.FakeClient.args[0][0], {host: ['localhost:9200'], pingTimeout: config.pingTimeout});
    });
  });
});
