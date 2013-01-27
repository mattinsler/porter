_ = require 'underscore'
os = require 'os'
async = require 'async'
procinfo = require 'procinfo'
child_process = require 'child_process'
{EventEmitter} = require 'events'

Queue = require './queue'
settings = require './settings'
WorkerHeartbeat = require './worker_heartbeat'
create_client = settings.redis.create_client

log = ->
  arguments[0] = "[Worker #{process.pid}] #{arguments[0]}"
  console.log.apply(console, arguments)

pretty_mem = (value) ->
  "#{Math.floor(100 * value / (1024 * 1024)) / 100} MB"

flatten_object = (obj, into = {}, prefix = '', sep = '_') ->
  for own key, prop of obj
    if typeof prop is 'object' and prop not instanceof Date and prop not instanceof RegExp
      flatten_object(prop, into, prefix + key + sep, sep)
    else
      into[prefix + key] = prop
  into


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

  @on: (command, func) ->
    o = {}
    o[command] = func
    @registry.register(o)

  constructor: (options = {}) ->
    @_is_child = process.send?

    @_queue_cache = {}
    @_queues = options.queues || settings.worker.queues
    Object.defineProperty(@, '_queues', {get: -> Worker.registry.queues()}) unless @_queues?

    @_min_poll_timeout = options.min_poll_timeout || settings.worker.min_poll_timeout
    @_max_poll_timeout = options.max_poll_timeout || settings.worker.max_poll_timeout
    @_concurrent_commands = options.concurrent_commands || settings.worker.concurrent_commands

    @_timeout = options.timeout || settings.timeout

    @_current_queue = 0
    @_current_commands = {}
    @_current_poll_timeout = @_min_poll_timeout

    @_state = 'uninitialized'

    @_child_workers = []
    @_all_children = []

    @_worker_heartbeat = new WorkerHeartbeat(@)

    @name = process.pid + new Date().getTime() + require('crypto').randomBytes(8).toString('hex')

  initialize: (callback) ->
    return unless @_state is 'uninitialized'
    @_state = 'initializing'
    @emit(@_state)

    async.map _.range(@_concurrent_commands), (idx, cb) ->
      start_child = (command, args) ->
        log "Starting child #{command} #{args.join(' ')}"
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
      @_all_children = children.slice(0)
      @_state = 'initialized'
      callback()
      @emit(@_state)

  next_queue: ->
    queues = @_queues
    @_current_queue = (@_current_queue + 1) % queues.length
    queue_name = queues[@_current_queue]
    @_queue_cache[queue_name] ||= new Queue(queues[@_current_queue])

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

    @emit('log', "Processing #{envelope.command}", envelope)
    @emit('work', envelope)
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

      # If this is the last in a graceful shutdown
      @graceful_stop() if @ready_for_graceful()

    status.worker.once 'message', on_message

    log "Sending #{envelope.command} to worker #{status.worker.pid}"
    status.worker.send {command: envelope.command, opts: envelope.value}

  success: (envelope) ->
    delete @_current_commands[envelope.id]
    envelope.remove()
    @emit('success', envelope)

  error: (err, envelope) ->
    delete @_current_commands[envelope.id]
    if envelope?
      # possible retries here...
      envelope.remove()

    err = new Error(err) if typeof err is 'string'
    @emit('failure', err, envelope)

  no_envelope: ->
    @emit('log', 'Waiting for more commands')
    @increase_timeout()

  ready_for_next: ->
    Object.keys(@_current_commands).length < @_concurrent_commands and @_child_workers.length > 0

  next: (timeout) ->
    setTimeout((=> @work()), timeout || @_current_poll_timeout)

  work: ->
    return @graceful_stop() if @ready_for_graceful()
    return unless @_state is 'running'
    return @next(@increase_timeout()) unless @ready_for_next()

    queue = @next_queue()
    log "Checking #{queue.name}"
    queue.pop (err, envelope) =>
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
          @_state = 'running'
          @emit @_state
          @_worker_heartbeat.start()
          log "Initialized! Let's go!"
          process.nextTick(=> @next(1))
          callback?()
      when 'initializing'
        @.once('initialized', -> callback?())

  stop: (callback) ->
    @_state = 'stopping'

  graceful: (callback) ->
    @_state = 'graceful'
    callback?()

  graceful_stop: () ->
    _.map(@_child_workers, (w) -> w.kill('SIGHUP'))
    process.exit(0)

  ready_for_graceful: () ->
    _.keys(@_current_commands).length is 0 and @_state is 'graceful'

  info: (callback) ->
    mem = process.memoryUsage()
    o = {
      name: @name
      pid: process.pid
      uptime: process.uptime()
      config: {
        queues: @_queues
        min_timeout: @_min_poll_timeout
        max_timeout: @_max_poll_timeout
        concurrent_commands: @_concurrent_commands
      }
      stats: {
        loadavg: os.loadavg()
        resident_total: 0
        os: {
          free: pretty_mem(os.freemem())
          total: pretty_mem(os.totalmem())
          in_use: pretty_mem(os.totalmem() - os.freemem())
        }
        process: {
          rss: pretty_mem(mem.rss)
          heap_total: pretty_mem(mem.heapTotal)
          heap_used: pretty_mem(mem.heapUsed)
        }
      }
    }

    async.map @_all_children, (c, cb) ->
      procinfo.memory c.pid, (err, mem) ->
        return cb(err) if err?
        o.stats.resident_total += mem.resident
        cb(null, procinfo.pretty_object(mem))
    , (err, children) ->
      return callback(err) if err?
      o.stats.children = children
      o.stats.resident_total = procinfo.pretty(o.stats.resident_total)
      callback(null, o)

  @workers: (callback) ->
    create_client().keys "#{settings.namespace}:worker:*", (err, keys) ->
      return callback(err) if err?
      callback(null, keys.map (k) -> k.slice("#{settings.namespace}:worker:".length))

  @stats: (callback) ->
    client = create_client()

    @workers (err, workers) ->
      return callback(err) if err?
      return callback(null, []) unless workers? and workers.length > 0
      client.mget workers.map((w) -> "#{settings.namespace}:worker:#{w}"), (err, workers) ->
        data = workers.map (w) ->
          w = JSON.parse(w)
          _.extend({type: 'worker', full_name: w.name}, flatten_object(w.stats))

        callback(null, data)

module.exports = Worker
