_ = require 'underscore'
async = require 'async'
crypto = require 'crypto'
uuid = require 'node-uuid'

settings = require './settings'
create_client = settings.redis.create_client

class Queue
  class Envelope
    constructor: (value, queue) ->
      _(@).extend(value, {queue: queue})

    remove: (callback) ->
      @queue.remove(@, callback)

  @lock_name: (name, namespace = settings.namespace) ->
    "#{namespace}:l:#{name}"

  @queue_name: (name, namespace = settings.namespace) ->
    "#{namespace}:q:#{name}"

  @work_queue_name: (name, namespace = settings.namespace) ->
    "#{namespace}:w:#{name}"

  @stats_hash_name: (name, type, namespace = settings.namespace) ->
    if type is 'queue'
      "#{namespace}:s:q:#{name}"
    else if type is 'command'
      "#{namespace}:s:c:#{name}"

  lock_name: (name) -> Queue.lock_name(name, @_namespace)
  queue_name: (name) -> Queue.queue_name(name, @_namespace)
  work_queue_name: (name) -> Queue.work_queue_name(name, @_namespace)
  stats_hash_name: (name, type) -> Queue.stats_hash_name(name, type, @_namespace)

  constructor: (queue_name, options = {}) ->
    @name = queue_name

    @_namespace = options.namespace || settings.namespace
    @_timeout = options.timeout || settings.timeout
    @_payload_storage = options.payload_storage || settings.payload_storage

    @_base_queue_name = queue_name
    @_queue_name = @queue_name(queue_name)
    @_work_queue_name = @work_queue_name(queue_name)
    @_stats_hash_name = @stats_hash_name(queue_name, 'queue')

  push: (command, item, callback) ->
    client = create_client()
    command = "#{@_base_queue_name}:#{command}" unless command.indexOf(@_base_queue_name + ':') is 0
    e = {
      id: uuid.v1()
      queued_at: new Date().getTime()
      command: command
      value: item
    }

    @_payload_storage.store e, (err) =>
      return callback?(err) if err?

      client.lpush @_queue_name, e.id, (err) =>
        return callback?(err) if err?
        callback?(null, new Envelope(e, @))

  pop: (callback) ->
    client = create_client()
    client.rpoplpush @_queue_name, @_work_queue_name, (err, envelope_id) =>
      return callback(err) if err?
      return callback() unless envelope_id?

      @_payload_storage.load envelope_id, (err, e) =>
        return callback(err) if err?
        unless e?
          return client.lrem @_work_queue_name, -1, envelope_id, (err) ->
            return callback(err) if err?
            callback()

        e.popped_at = new Date().getTime()
        client.setex(@lock_name(e.id), @_timeout, 1)
        callback(null, new Envelope(e, @))

  peek: (how_many, callback) ->
    client = create_client()
    if typeof how_many is 'function'
      callback = how_many
      how_many = 1
    client.lrange @_queue_name, -how_many, -1, (err, items) =>
      return callback(err) if err?
      @_payload_storage.load_many items, callback

  remove: (envelope, callback) ->
    client = create_client()
    envelope.removed_at = new Date().getTime()
    client.multi()
      .del(@lock_name(envelope.id))
      .lrem(@_work_queue_name, -1, envelope.id)

      .hincrby(@_stats_hash_name, 'count', 1)
      .hincrby(@_stats_hash_name, 'queue_pop', envelope.popped_at - envelope.queued_at)
      .hincrby(@_stats_hash_name, 'pop_remove', envelope.removed_at - envelope.popped_at)
      .hincrby(@_stats_hash_name, 'queue_remove', envelope.removed_at - envelope.queued_at)

      .hincrby(@stats_hash_name(envelope.command, 'command'), 'count', 1)
      .hincrby(@stats_hash_name(envelope.command, 'command'), 'queue_pop', envelope.popped_at - envelope.queued_at)
      .hincrby(@stats_hash_name(envelope.command, 'command'), 'pop_remove', envelope.removed_at - envelope.popped_at)
      .hincrby(@stats_hash_name(envelope.command, 'command'), 'queue_remove', envelope.removed_at - envelope.queued_at)

      .exec (err, replies) =>
        return callback?(err) if err?
        return callback?(null, 0) if replies[0] is 0
        @_payload_storage.remove envelope.id, callback

  count: (callback) ->
    create_client().llen @_queue_name, callback

  work_count: (callback) ->
    create_client().llen @_work_queue_name, callback

  stats: (callback) ->
    Queue.__stats__(@_stats_hash_name, callback)

  commands: (callback) ->
    Queue.__commands__(@stats_hash_name(@_base_queue_name, 'command') + ':*', @stats_hash_name(@_base_queue_name, 'command') + ':', callback)

  command_stats: (command, callback) ->
    command = "#{@_base_queue_name}:#{command}" unless command.indexOf(@_base_queue_name + ':') is 0
    Queue.__stats__(@stats_hash_name(command, 'command'), callback)

  commands_stats: (callback) ->
    @commands (err, commands) =>
      return callback(err) if err?
      async.parallel _(commands).inject(((o, c) =>
        o[c] = (cb) => @command_stats(c, cb)
        o
      ), {}), (err, data) ->
        return callback(err) if err?
        callback(null, _(commands).inject ((o, c) ->
          o[c] = {
            name: c
            stats: data[c]
          }
          o
        ), {})

  clear: (callback) ->
    client = create_client()
    client.lrange @_queue_name, 0, -1, (err, items) =>
      return callback?(err) if err?
      @_payload_storage.remove_many items, (err) =>
        return callback?(err) if err?
        client.ltrim @_queue_name, 1, 0, callback

  clear_stats: (callback) ->
    create_client().del @_stats_hash_name, callback

  clear_command_stats: (command, callback) ->
    command = "#{@_base_queue_name}:#{command}" unless command.indexOf(@_base_queue_name + ':') is 0
    create_client().del(@stats_hash_name(command, 'command'), callback)

  @__stats__: (search, callback) ->
    create_client().hgetall search, (err, stats) ->
      return callback(err) if err?
      stats = {} unless stats?
      stats.count ?= 0
      stats.queue_pop ?= 0
      stats.pop_remove ?= 0
      stats.queue_remove ?= 0
      stats.avg_time_on_queue = if stats.count is 0 then 0 else stats.queue_pop / stats.count
      stats.avg_process_time = if stats.count is 0 then 0 else stats.pop_remove / stats.count
      stats.avg_queue_to_completion = if stats.count is 0 then 0 else stats.queue_remove / stats.count
      callback(null, stats)

  @__commands__: (search, prefix_to_remove, callback) ->
    create_client().keys search, (err, commands) ->
      return callback(err) if err?
      callback(null, commands.map (q) -> q.slice(prefix_to_remove.length))

  @commands: (callback) ->
    Queue.__commands__("#{settings.namespace}:s:c:*", "#{settings.namespace}:s:c:", callback)

  @command_stats: (command, callback) ->
    Queue.__stats__("#{settings.namespace}:s:c:#{command}", callback)

  @queues: (callback) ->
    client = create_client()
    async.parallel {
      queues: (cb) -> client.keys("#{settings.namespace}:q:*", cb)
      work_queues: (cb) -> client.keys("#{settings.namespace}:w:*", cb)
      stats_queues: (cb) -> client.keys("#{settings.namespace}:s:q:*", cb)
    }, (err, data) ->
      return callback(err) if err?
      queues = data.queues.map (q) -> q.slice("#{settings.namespace}:q:".length)
      work_queues = data.work_queues.map (q) -> q.slice("#{settings.namespace}:w:".length)
      stats_queues = data.stats_queues.map (q) -> q.slice("#{settings.namespace}:s:q:".length)
      callback(null, _(queues).union(work_queues, stats_queues))

  @stats: (callback) ->
    @queues (err, queues) ->
      return callback(err) if err?
      async.parallel _(queues).inject((o, q) ->
        queue = new Queue(q)
        o["#{q}-stats"] = (cb) -> queue.stats(cb)
        o["#{q}-count"] = (cb) -> queue.count(cb)
        o["#{q}-work-count"] = (cb) -> queue.work_count(cb)
        o
      , {}), (err, data) ->
        return callback(err) if err?
        callback(null, _(queues).inject (o, q) ->
          o[q] = {
            type: 'queue'
            name: q
            full_name: q
            queue: q
            queued: data["#{q}-count"]
            in_progress: data["#{q}-work-count"]
            stats: data["#{q}-stats"]
          }
          o
        , {})

  @commands_stats: (callback) ->
    @commands (err, commands) ->
      return callback(err) if err?
      async.parallel _(commands).inject((o, c) ->
        o[c] = (cb) -> Queue.command_stats(c, cb)
        o
      , {}), (err, data) ->
        return callback(err) if err?
        callback(null, _(commands).inject (o, c) ->
          [queue, command] = c.split(':')
          o[c] = {
            type: 'command'
            name: command
            full_name: c
            queue: queue
            command: command
            stats: data[c]
          }
          o
        , {})

  @__reap__: (lock_name, queue_name, work_queue_name, callback) ->
    client = create_client()
    client.lrange work_queue_name, 0, -1, (err, items) ->
      return process.nextTick(-> callback?(null, 0)) if items.length is 0

      count = 0
      cb = _.after items.length, -> callback?(null, count)

      items.forEach (item) ->
        client.exists lock_name(item), (err, exists) ->
          return cb() if exists
          client.multi()
                .lrem(work_queue_name, -1, item)
                .lpush(queue_name, item)
                .exec ->
                  count += 1
                  cb()

  @reap: (callback) ->
    @queues (err, queues) ->
      return callback?(err) if err?
      return callback?(null, 0) if queues.length is 0
      async.parallel queues.map((q) ->
        (cb) ->
          Queue.__reap__(Queue.lock_name, Queue.queue_name(q), Queue.work_queue_name(q), cb)
      ), (err, data) ->
        return callback?(err) if err?
        callback?(null, _(data).inject ((o, i) -> i + o), 0)

  reap: (callback) ->
    Queue.__reap__(@lock_name, @_queue_name, @_work_queue_name, callback)

module.exports =  Queue
