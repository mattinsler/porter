Queue = require('../index').Queue

class Reaper
  constructor: (@interval = 5000) ->
  
  start: ->
    return if @interval_id
    
    @interval_id = setInterval(=>
      @reap()
    , @interval)
    
    @reap()
  
  stop: ->
    clearInterval(@interval_id)
    @interval_id = null
  
  reap: ->
    start = new Date()
    Queue.reap (err, count) ->
      end = new Date()
      console.log "[Porter] Reaped #{count} in #{(end - start) / 1000}s"
  
module.exports = Reaper
