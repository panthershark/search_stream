var SearchStream = require('../../index.js');
var assert = require('assert');
var async = require('async');
var items = require('../fixture/data.js');
var elasticsearch = require('elasticsearch');
var es_type = 'search_stream_test';
var es_index = 'search_stream_test';
var es_options = {
  client: {
    "host": "localhost:9200",
    "log": "error"
  },
  fields: ['id', 'name'],
  index: es_index,
  type: es_type
};

suite('Users with documents notifications on', function () {

  setup(function (done) {
    // insert test data
    var client = new elasticsearch.Client({
      "host": "localhost:9200",
      "log": "error"
    });

    async.series([

      function (cb) {
        client.indices.delete({
          index: es_index
        }, function (err) {
          cb(null);
        });
      },

      function (cb) {
        client.indices.create({
          index: es_index
        }, cb);
      },

      function (cb) {
        client.bulk({
          body: items.map(function (item) {
            return [{
                index: {
                  _index: es_index,
                  _type: es_type
                }
              },
              item
            ];
          }).reduce(function (a, b) {
            return a.concat(b);
          })
        }, cb);
      },

      // wait for commit.
      function (cb) {
        console.log('waiting');
        setTimeout(cb, 1000);
      },
    ], done)

  });

  test('Stream active users', function (done) {
    this.timeout(5000);
    var cnt = 0;
    var rs = new SearchStream({
      elastic_search: es_options,
      filters: {
        "active": true
      }
    });

    rs.on('data', function (doc) {
      cnt++;
      console.log(doc);

      assert.ok(doc.id, 'Record id valid');
      assert.ok(doc.name, 'Name valid');
      assert.equal(doc["active"], true, 'Filter fields should match query');
    });

    rs.on('error', assert.ifError.bind(assert));

    rs.on('end', function () {
      console.log('Documents Test Count: ' + cnt);
      assert.equal(cnt, 1, 'Count should be high');
      done();
    });
  });

});