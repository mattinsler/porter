_ = require 'underscore'
child_process = require 'child_process'
{EventEmitter} = require 'events'
Queue = require('../index').Queue

log = ->
  arguments[0] = "[Worker #{process.pid}] #{arguments[0]}"
  console.log.apply(console, arguments)

class Registry
  constructor: ->
    @commands = {}
    
  register: (commands) ->
    _(@commands).extend(commands)
  
  get: (command) ->
    @commands[command]
  
  queues: ->
    _.chain(@commands).keys().map((c) -> c.split(':')[0]).uniq().value()

class Worker extends EventEmitter
  @registry = new Registry()
  
  @on: (queue, command, func) ->
    o = {}
    o["#{queue}:#{command}"] = func
    @registry.register(o)
  
  constructor: (@config = {}) ->
    @queue_cache = {}
    Object.defineProperty @config, 'queues', {get: -> Worker.registry.queues()} unless @config.queues?
    
    @config.min_timeout ||= 100
    @config.max_timeout ||= 5000
    @config.concurrent_commands ||= 1
    @config.queue_config = {}
    @config.queue_config.payload_storage = @config.payload_storage if @config.payload_storage?
    @config.queue_config.timeout = @config.timeout if @config.timeout?
    delete @config.payload_storage
    
    @current_queue = 0
    @current_commands = {}
    @current_timeout = config.min_timeout
    
    @_should_run = true
    
    @child_workers = []
    
    done = _.after @config.concurrent_commands, =>
      @initialized = true

    for x in _.range(@config.concurrent_commands)
      do (x) =>
        child = child_process.fork(process.argv[1], ['run', 'lib/queue_worker_child.coffee'], {cwd: '.', env: process.env})
        child.on 'message', (msg) =>
          if msg.status is 'ready'
            @child_workers.push(child)
            done()
  
  next_queue: ->
    queues = @config.queues
    @current_queue = (@current_queue + 1) % queues.length
    queue_name = queues[@current_queue]
    @queue_cache[queue_name] ||= new JobQueue(queues[@current_queue], @config.queue_config)
  
  increase_timeout: ->
    c = Math.max(@current_timeout, @config.min_timeout)
    @current_timeout = Math.min(@config.max_timeout, c * 4 / 3)
    c

  reset_timeout: ->
    @current_timeout = 1
  
  process_command: (envelope) ->
    @reset_timeout()
    
    command = Worker.registry.get(envelope.command)
    return @error("Unknown command: #{envelope.command}", envelope) unless command?
    
    worker = @child_workers.shift()

    @emit 'log', "Processing #{envelope.command}", envelope
    status = {
      envelope: envelope
      worker: worker
    }
    @current_commands[envelope.id] = status
    
    on_message = (msg) =>
      if msg.status is 'error'
        @child_workers.push(status.worker)
        @error(msg.error, envelope)
        # child.kill()
      else if msg.status is 'success'
        @child_workers.push(status.worker)
        @success(envelope)
        # child.kill()
    
    status.worker.once 'message', on_message
    
    log "Sending #{envelope.command} to worker #{status.worker.pid}"
    status.worker.send {command: envelope.command, opts: envelope.value}
    
    # try
    #   command envelope.value, (err) =>
    #     return @error(err, envelope) if err?
    #     @success(envelope)
    # catch e
    #   @error(e, envelope)

  success: (envelope) ->
    delete @current_commands[envelope.id]
    @emit 'success', {envelope: envelope}
    envelope.remove()
  
  error: (err, envelope) ->
    delete @current_commands[envelope.id]
    if envelope?
      # possible retries here...
      envelope.remove()

    err = new Error(err) if typeof err is 'string'
    @emit 'error', {error: err, envelope: envelope}
  
  no_envelope: ->
    @emit 'log', 'Waiting for more commands'
    @increase_timeout()
  
  ready_for_next: ->
    Object.keys(@current_commands).length < @config.concurrent_commands and @child_workers.length > 0
  
  next: (timeout) ->
    setTimeout((=> @work()), timeout || @current_timeout)
  
  work: ->
    return unless @_should_run
    return @next(@increase_timeout()) unless @ready_for_next()
    
    @emit 'work'
    
    @next_queue().pop (err, envelope) =>
      if err?
        @error(err)
      else if not envelope?
        @no_envelope()
      else
        @process_command(envelope)
      @next()
  
  start: ->
    unless @initialized
      log 'Waiting for initialization' 
      return setTimeout =>
        @start()
      , 1000
    log "Initialized! Let's go!"
    @next(1)
  
  stop: (callback) ->
    @_should_run = false
  
module.exports = Worker
