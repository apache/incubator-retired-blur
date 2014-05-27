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

'use strict';

module.exports = function (grunt) {

    // Load grunt tasks automatically
    require('load-grunt-tasks')(grunt);

    // Time how long tasks take. Can help when optimizing build times
    require('time-grunt')(grunt);

    // Define the configuration for all the tasks
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),

        clean: ['css'],

        bower: {
            install: {
                options: {
                    targetDir: './libs',
                    install: true,
                    copy: false,
                    quiet: true
                }
            },
            prune: {
                options: {
                    targetDir: './libs',
                    copy: false,
                    offline: true,
                    quiet: true
                }
            }
        },

        // Compiles Sass to CSS and generates necessary files if requested
        sass: {
            options: {
                sourcemap: true,
                lineNumbers: true,
                loadPath: ['libs']
            },
            development: {
                files: {
                    'css/blurconsole.css': 'sass/blurconsole.scss'
                }
            },
            production: {
                files: {
                    'css/blurconsole.css': 'sass/blurconsole.scss'
                },
                options: {
                    style: 'compressed'
                }
            }
        },

        // Make sure code styles are up to par and there are no obvious mistakes
        jshint: {
            options: {
                jshintrc: true,
            },
            development: {
                src: ['js/**/*\.js']
            },
            ci: {
                src: ['js/**/*\.js'],
                options: {
                    reporter: 'checkstyle',
                    reporterOutput: 'build/jshint-checkstyle.xml'
                }
            }
        },

        // Watches files for changes and runs tasks based on the changed files
        watch: {
            options: {
                dateFormat: function(time) {
                    grunt.log.ok('The watch finished in ' + (time / 1000).toFixed(2) + 's. Waiting...');
                },
                spawn: false,
                interrupt: false
            },
            css: {
                files: ['sass/**/*.scss', 'libs/**/*.css', 'libs/**/*.scss'],
                tasks: ['sass:development', 'notify:css']
            },
            lint: {
                files: ['js/**/*.js'],
                tasks: ['jshint:development']
            },
            livereload: {
                options: {
                    livereload: '<%= connect.options.livereload %>'
                },
                files: [
                    './{,*/}*.html',
                    './img/{,*/}*.{gif,jpeg,jpg,png,svg,webp}'
                ]
            }
        },

        notify: {
            css: {
                options: {
                    title: 'Blur Console',
                    message: 'Sass finished'
                }
            }
        },

        mochaSelenium: {
            options: {
                screenshotAfterEach: true,
                screenshotDir: 'test/reports',
                reporter: 'spec',
                viewport: { width: 900, height: 700 },
                timeout: 30e3,
                slow: 10e3,
                implicitWaitTimeout: 100,
                asyncScriptTimeout: 5000,
                usePromises: true,
                useChaining: true,
                ignoreLeaks: false
            },
            firefox: { src: ['test/functional/spec/**/*.js'], options: { browserName: 'firefox' } },
            chrome: { src: ['text/function/spec/**/*.js'], options: { browserName: 'chrome' } }
        },

        karma: {
            unit: {
                configFile: 'karma.conf.js',
                runnerPort: 9999,
                singleRun: true
            }
        },

        connect: {
            options: {
                port: 3000,
                livereload: 4000,
                // Change this to '0.0.0.0' to access the server from outside
                hostname: '0.0.0.0'
            },
            livereload: {
                options: {
                    open: 'http://0.0.0.0:3000/?fakeIt=true'
                }
            }
        },
    });

    grunt.loadNpmTasks('grunt-bower-task');
    grunt.loadNpmTasks('grunt-exec');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-notify');
    grunt.loadNpmTasks('grunt-mocha-selenium');
    grunt.loadNpmTasks('grunt-karma');

    var initialHintSrc = grunt.config('jshint.development.src');
    grunt.event.on('watch', function(action, filepath){
        var matchingHint = grunt.file.match(initialHintSrc, filepath);
        grunt.config('jshint.development.src', matchingHint);
    });

    grunt.registerTask('deps', 'Install Webapp Dependencies', ['bower:install', 'bower:prune']);
    grunt.registerTask('test:functional:chrome', 'Run JavaScript Functional Tests in Chrome', ['mochaSelenium:chrome']);
    grunt.registerTask('test:functional:firefox', 'Run JavaScript Functional Tests in Firefox', ['mochaSelenium:firefox']);
    grunt.registerTask('test:functional', 'Run JavaScript Functional Tests', ['test:functional:chrome', 'test:functional:firefox']);
    grunt.registerTask('test:unit', 'Run JavaScript Unit Tests', ['karma']);
    grunt.registerTask('test:style', 'Run JavaScript CodeStyle reports', ['jshint:ci'/*, 'plato:ci' */]);
    grunt.registerTask('style:development', 'Run JavaScript CodeStyle reports', ['jshint:development']);
    grunt.registerTask('development', 'Build sass for development', ['sass:development']);
    grunt.registerTask('production', 'Build sass for production', ['sass:production']);
    grunt.registerTask('serve', 'Run development server', ['clean','sass:development', 'connect:livereload','watch']);
    grunt.registerTask('default', ['clean', 'development', 'style:development', 'watch']);
};
