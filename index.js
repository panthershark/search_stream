var _ = require('lodash');
var generate_filter = require('generate_filter');
var Elastic = require('elastic_filter');
var val = function (r, k) {
  return Array.isArray(r[k]) && r[k].length ? r[k][0] : r[k];
};
var stream = require('stream');
var util = require('util');
var assert = require('assert');

function ResultStream(opt) {

  if (!(this instanceof ResultStream)) {
    return new ResultStream(opt);
  }

  stream.Readable.call(this, _.defaults({
    objectMode: true
  }, opt));

  this.buffer = [];
  this.paused = true;

  this.paging = {
    pagenum: 1,
    pagesize: 50
  };

  this.has_data = true;

  this.app = {
    elastic: new Elastic(opt.elastic_search.client)
  };

  this.elastic_search = opt.elastic_search;
  /** Example:
  {
    "email_preferences.qna": true,
    "functional_areas.area": opt.filters.functional_area
  }
  **/

  this.filters = _.map(opt.filters, generate_filter);
  this.raw_filters = opt.filters;
  debugger;
  // start loading
  var self = this;
  this.app.elastic._setup(function (err) {
    assert.ifError(err);
    self.load_buffer();
  });
}

util.inherits(ResultStream, stream.Readable);

ResultStream.prototype._read = function () {
  this.paused = false;
  if (this.buffer.length === 0) {
    this._readableState.reading = false;
  }
  while (!this.paused && this.buffer.length > 0) {
    this.paused = this.push(this.buffer.shift());
  }

  if (!this.has_data && this.buffer.length === 0) {
    this.push(null);
  }
};

ResultStream.prototype.add = function (chunk) {
  this.buffer.push(chunk);
};

ResultStream.prototype.load_buffer = function () {
  var self = this;
  var default_fields = this.elastic_search.fields || ['id'];

  var searchOptions = _.extend({
    fields: [],
    type: this.elastic_search.type,
    index: this.elastic_search.index
  }, this.paging);

  // inc page number
  this.paging.pagenum++;
  searchOptions.filters = this.filters;
  searchOptions.sort = ['id'];
  searchOptions.result_fields = default_fields.concat(Object.keys(this.raw_filters));

  var query = this.app.elastic.search(null, searchOptions, function (err, data) {

    if (err) {
      self.emit('error', err);
    }

    // are there more results?  then get the next page.
    if (data.results.length) {
      self.load_buffer();
    } else {
      self.has_data = false;
    }

    self.emit('load_buffer', data);

    data.results.forEach(self.add.bind(self));
    self.read(0);
  });
  console.log(query);
};



module.exports = ResultStream;