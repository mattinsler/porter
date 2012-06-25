{Worker} = require 'porter'

Worker.on 'log:log', (opts, callback) ->
  console.log opts.message
  process.nextTick(callback)

Worker.on 'log:error', (opts, callback) ->
  console.log "ERROR: #{opts.message}"
  console.log opts.stack
  process.nextTick(callback)

new Worker().start()
