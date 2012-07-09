Service = require './service'
settings = require './settings'

class WorkerHeartbeat extends Service
  constructor: (@worker, interval, run_on_start) ->
    super(interval, run_on_start)
  
  run: ->
    @worker.info (err, info) =>
      return console.error(err.stack) if err?
      client = settings.redis.create_client()
      client.setex "#{settings.namespace}:worker:#{@worker.name}", 10, JSON.stringify(info)
  
module.exports = WorkerHeartbeat
