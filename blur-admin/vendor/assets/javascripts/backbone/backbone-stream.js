Backbone.StreamCollection = Backbone.Collection.extend({
  // Extended version of fetch (used to call update with the given option)
  // Replicates all default functionality
  fetch: function(options) {
    options = options ? _.clone(options) : {};
    if (options.parse === undefined) options.parse = true;
    var collection = this;
    var success = options.success;
    options.success = function(resp, status, xhr) {
      collection[options.update ? 'update' : options.add ? 'add' : 'reset'](collection.parse(resp, xhr), options);
      if (success) success(collection, resp);
    };
    options.error = Backbone.wrapError(options.error, collection, options);
    return (this.sync || Backbone.sync).call(this, 'read', this, options);
  },
  // Updates the current collection removing any missing models
  // Updates the attributes and triggers update events
  update : function(models, options) {
    models  || (models = []);
    options || (options = {});
    var updateMap = _.reduce(this.models, function(map, model){ map[model.id] = false; return map },{});
    _.each( models, function(model) {
      var idAttribute = this.model.prototype.idAttribute;
      var modelId = model[idAttribute];
      if ( modelId == undefined ) throw new Error("Can't update a model with no id attribute. Please use 'reset'.");
      if ( this._byId[modelId] ) {
        var attrs = (model instanceof Backbone.Model) ? _.clone(model.attributes) : _.clone(model);
        delete attrs[idAttribute];
        this._byId[modelId].set( attrs );
        updateMap[modelId] = true;
      }
      else {
        this.add( model );
      }
    }, this);
    _.select(updateMap, function(updated, modelId){
      if (!updated) this.remove( modelId );
    }, this);
    return this;
  },

  // Start streaming data at a set interval
  // Will stop all previous streams and start a new stream
  stream: function(options) {
    this.unstream();
    var _update = _.bind(function() {
      this.fetch(options);
      this._intervalFetch = window.setTimeout(_update, options.interval || 1000);
    }, this);
    _update();
  },

  // Stops the current stream if it is currently streaming
  unstream: function() {
    if (this.isStreaming()){
      window.clearTimeout(this._intervalFetch);
      delete this._intervalFetch;
    }
  },

  // Returns the current state of the stream  
  isStreaming : function() {
    return !_.isUndefined(this._intervalFetch);   
  }
});