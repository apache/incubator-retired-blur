/*

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

module.exports = function(config) {
    // karma start --coverage [coverageType]
    // http://karma-runner.github.io/0.8/config/converage.html

    var karmaConfig = {
        basePath: '',
        frameworks: ['mocha'],
        files: [
            // Source
            {pattern: 'js/**/*.js', included: false},

            // Images
            {pattern: 'img/**/*.*', included: false},

            // Included libs
            'libs/jquery/jquery.js',

            // Libraries
            {pattern: 'libs/**/*.js', included: false},

            // Test Files
            {pattern: 'test/unit/spec/**/*.js', included: false},

            // Test Mocks
            {pattern: 'test/unit/mocks/**/*.js', included: false},
            {pattern: 'test/unit/utils/**/*.js', included: false},

            // Test Runner
            'test/unit/runner/testRunner.js'
        ],
        exclude: [],
        reporters: ['dots'],
        port: 9876,
        colors: true,
        logLevel: config.LOG_WARN,
        autoWatch: true,
        browsers: ['Chrome', 'Firefox'],
        captureTimeout: 60000,
        singleRun: false
    },
    coverageType = 'html',
    coverage = process.argv.filter(function(a, index) {
        if (a == '--coverage') {
            if ((index + 1) < process.argv.length) {
                coverageType = process.argv[index + 1];
            }
            return true;
        }
        return false;
    }).length;

    if (coverage) {
        karmaConfig.preprocessors = {
            'js/*.js': 'coverage',
            'js/**/*.js': 'coverage'
        };
        karmaConfig.reporters.push('coverage');
        karmaConfig.coverageReporter = {
            type: coverageType,
            dir: 'build/coverage/'
        };
    }

    config.set(karmaConfig);
};
