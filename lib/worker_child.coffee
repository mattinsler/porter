traverse = require 'traverse'
Worker = require './worker'

# console.log = ->
#   process.send {log: Array::slice.call(null, arguments)}


remove_circular = (obj) ->
  traverse(obj).map ->
    @remove() if @circular

log = ->
  arguments[0] = "[Worker Child #{process.pid}] #{arguments[0]}"
  console.log.apply(console, arguments)

error = (err) ->
  process.send {status: 'error', error: remove_circular(err)}

success = ->
  process.send {status: 'success'}

process_command = (command, opts, callback) ->
  log "Processing #{command}"
  command = Worker.registry.get(command)
  return callback("Unknown command: #{command}") unless command?
  try
    command opts, (err) ->
      return callback(err) if err?
      callback()
  catch e
    callback(e)

process.on 'message', (msg) ->
  process_command msg.command, msg.opts, (err) ->
    return error(err) if err?
    success()

log 'Ready'
process.send(status: 'ready')
