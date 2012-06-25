_ = require 'underscore'
async = require 'async'
child_process = require 'child_process'
{EventEmitter} = require 'events'
Queue = require './queue'
settings = require './settings'

create_client = settings.redis.create_client

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
  
  constructor: (options = {}) ->
    @_is_child = process.send?
    
    @_queue_cache = {}
    @_queues = options.queues || settings.queues
    Object.defineProperty @, '_queues', {get: -> Worker.registry.queues()} unless @_queues?

    @_min_poll_timeout = options.min_poll_timeout || settings.worker.min_poll_timeout
    @_max_poll_timeout = options.max_poll_timeout || settings.worker.max_poll_timeout
    @_concurrent_commands = options.concurrent_commands || settings.worker.concurrent_commands

    @_timeout = options.timeout || settings.timeout
    
    @_current_queue = 0
    @_current_commands = {}
    @_current_poll_timeout = @_min_poll_timeout

    @_state = 'uninitialized'
    
    @_child_workers = []
  
  initialize: (callback) ->
    return unless @_state is 'uninitialized'
    @_state = 'initializing'
    @emit @_state
    
    async.map _.range(@_concurrent_commands), (idx, cb) ->
      start_child = (command, args) ->
        log "Starting child #{command} #{args}"
        child = child_process.fork(command, args, {cwd: '.', env: process.env})
        child.on 'message', (msg) ->
          cb(null, child) if msg.status is 'ready'
      
      if process.argv[0] is 'node'
        start_child(process.argv[1], process.argv.slice(2))
      else if process.argv[0] is 'coffee'
        start_child(process.argv[1], process.argv.slice(2))
    , (err, children) =>
      return callback(err) if err?
      @_child_workers = children
      @_state = 'initialized'
      callback()
      @emit @_state
  
  next_queue: ->
    queues = @_queues
    @_current_queue = (@_current_queue + 1) % queues.length
    queue_name = queues[@_current_queue]
    @_queue_cache[queue_name] ||= new Queue(queues[@current_queue])
  
  increase_timeout: ->
    c = Math.max(@_current_poll_timeout, @_min_poll_timeout)
    @_current_poll_timeout = Math.min(@_max_poll_timeout, c * 4 / 3)
    c

  reset_timeout: ->
    @_current_poll_timeout = 1
  
  process_command: (envelope) ->
    @reset_timeout()
    
    command = Worker.registry.get(envelope.command)
    return @error("Unknown command: #{envelope.command}", envelope) unless command?
    
    worker = @_child_workers.shift()

    @emit 'log', "Processing #{envelope.command}", envelope
    status = {
      envelope: envelope
      worker: worker
    }
    @_current_commands[envelope.id] = status
    
    on_message = (msg) =>
      if msg.status is 'error'
        @_child_workers.push(status.worker)
        @error(msg.error, envelope)
        # child.kill()
      else if msg.status is 'success'
        @_child_workers.push(status.worker)
        @success(envelope)
        # child.kill()
    
    status.worker.once 'message', on_message
    
    log "Sending #{envelope.command} to worker #{status.worker.pid}"
    status.worker.send {command: envelope.command, opts: envelope.value}

  success: (envelope) ->
    delete @_current_commands[envelope.id]
    @emit 'success', {envelope: envelope}
    envelope.remove()
  
  error: (err, envelope) ->
    delete @_current_commands[envelope.id]
    if envelope?
      # possible retries here...
      envelope.remove()

    err = new Error(err) if typeof err is 'string'
    @emit 'error', {error: err, envelope: envelope}
  
  no_envelope: ->
    @emit 'log', 'Waiting for more commands'
    @increase_timeout()
  
  ready_for_next: ->
    Object.keys(@_current_commands).length < @_concurrent_commands and @_child_workers.length > 0
  
  next: (timeout) ->
    setTimeout((=> @work()), timeout || @_current_poll_timeout)
  
  work: ->
    return unless @_state is 'running'
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
  
  start: (callback) ->
    return require('./worker_child') if @_is_child

    switch @_state
      when 'initialized' then process.nextTick(-> callback?())
      when 'running' then process.nextTick(-> callback?())

      when 'uninitialized'
        log 'Waiting for initialization'
        @initialize (err) =>
          return callback?(err) if err?
          log "Initialized! Let's go!"
          process.nextTick(=> @next(1))
          callback?()
      when 'initializing'
        @.once('initialized', -> callback?())
  
  stop: (callback) ->
    @_state = 'stopping'
  
module.exports = Worker
