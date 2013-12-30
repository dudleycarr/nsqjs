"use strict"
module.exports = (grunt) ->

  # Project configuration.
  grunt.initConfig
    coffee:
      lib:
        options:
          bare: true
          sourceMap: true
        expand: true
        src: ['src/**/*.coffee']
        dest: 'dest'
        ext: '.js'

      test:
        options:
          bare: true
        expand: true
        src: ['test/**/*.coffee']
        dest: 'test'
        ext: '.js'

    watch:
      lib:
        files: '<%= coffee.lib.src %>'
        tasks: ['coffee:lib']

      test:
        files: [
          '<%= coffee.src.src %>'
          '<%= coffee.test.src %>'
        ]
        tasks: ['coffee', 'test']

    simplemocha:
      all:
        src: ['test/**/*.coffee']
        options:
          timeout: 3000
          ignoreLeaks: false
          ui: 'bdd'
          compilers: 'coffee:coffee-script'

    coffeelint:
      lib: ['*.coffee', 'src/*.coffee', 'test/*.coffee', 'examples/.*coffee']


  # These plugins provide necessary tasks.
  grunt.loadNpmTasks 'grunt-contrib-coffee'
  grunt.loadNpmTasks 'grunt-contrib-watch'
  grunt.loadNpmTasks 'grunt-simple-mocha'
  grunt.loadNpmTasks 'grunt-coffeelint'

  grunt.registerTask 'default', ['watch']
  grunt.registerTask 'test', ['simplemocha']
  grunt.registerTask 'lint', ['coffeelint']
