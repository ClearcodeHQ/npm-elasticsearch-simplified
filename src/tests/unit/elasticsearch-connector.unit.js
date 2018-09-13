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
});
