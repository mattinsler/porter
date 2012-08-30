Queue = require './queue'
Service = require './service'

class Reaper extends Service
  run: (callback) ->
    start = new Date()
    Queue.reap (err, count) ->
      return callback?(err) if err?
      end = new Date()
      console.log "[Porter] Reaped #{count} in #{(end - start) / 1000}s"
      callback?(null, (end - start) / 1000)
  
module.exports = Reaper
