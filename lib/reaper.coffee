Queue = require './queue'
Service = require './service'

class Reaper extends Service
  run: ->
    start = new Date()
    Queue.reap (err, count) ->
      end = new Date()
      console.log "[Porter] Reaped #{count} in #{(end - start) / 1000}s"
  
module.exports = Reaper
