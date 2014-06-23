// Karma configuration
// Generated on Sat Jun 21 2014 07:42:42 GMT-0400 (EDT)

module.exports = function(config) {
  config.set({

    // base path that will be used to resolve all patterns (eg. files, exclude)
    basePath: '',


    // frameworks to use
    // available frameworks: https://npmjs.org/browse/keyword/karma-adapter
    frameworks: ['mocha', 'chai', 'sinon'],


    // list of files / patterns to load in the browser
    files: [
      'libs/jquery/dist/jquery.js',
      'js/utils/*\.js',
      'libs/twbs-bootstrap-sass/vendor/assets/javascripts/bootstrap/tooltip.js',
      'libs/twbs-bootstrap-sass/vendor/assets/javascripts/bootstrap/modal.js',
      'libs/twbs-bootstrap-sass/vendor/assets/javascripts/bootstrap/transition.js',
      'libs/twbs-bootstrap-sass/vendor/assets/javascripts/bootstrap/popover.js',
      'libs/twbs-bootstrap-sass/vendor/assets/javascripts/bootstrap/collapse.js',
      'libs/twbs-bootstrap-sass/vendor/assets/javascripts/bootstrap/tab.js',
      'libs/flot/jquery.flot.js',
      'libs/flot/jquery.flot.pie.js',
      'libs/flot/jquery.flot.categories.js',
      'libs/flot/jquery.flot.stack.js',
      'libs/typeahead.js/dist/typeahead.jquery.js',
      'js/blurconsole.js',
      'js/*\.js',
      'test/**/*Test.js'
    ],


    // list of files to exclude
    exclude: [
      
    ],


    // preprocess matching files before serving them to the browser
    // available preprocessors: https://npmjs.org/browse/keyword/karma-preprocessor
    preprocessors: {
        'js/*.js': ['coverage']
    },


    // test results reporter to use
    // possible values: 'dots', 'progress'
    // available reporters: https://npmjs.org/browse/keyword/karma-reporter
    reporters: ['progress', 'coverage'],


    // web server port
    port: 9876,


    // enable / disable colors in the output (reporters and logs)
    colors: true,


    // level of logging
    // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
    logLevel: config.LOG_INFO,


    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: true,


    // start these browsers
    // available browser launchers: https://npmjs.org/browse/keyword/karma-launcher
    browsers: ['PhantomJS'],


    // Continuous Integration mode
    // if true, Karma captures browsers, runs the tests and exits
    singleRun: false
  });
};
