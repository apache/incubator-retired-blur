alert(typeof console)
if (typeof console == 'undefined') {
  console = {
    log: function() {
      if(typeof blurconsole != 'undefined' && typeof blurconsole.model != 'undefined' && typeof blurconsole.model.logs != 'undefined') {
        var args = Array.prototype.slice.call(arguments);
        blurconsole.model.logs.logError(args.join(' '), 'javascript');
      }
    },
    info: function() {
      return console.log.apply(null, arguments);
    },
    warn: function() {
      return console.log.apply(null, arguments);
    },
    error: function() {
      return console.log.apply(null, arguments);
    },
    debug: function() {
      return console.log.apply(null, arguments);
    }
  }
}