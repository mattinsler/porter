settings = require './settings'

client = settings.redis.create_client

exports.type = 'redis'

exports.store = (envelope, callback) ->
  client().set(envelope.id, JSON.stringify(envelope), callback)

exports.store_many = (envelopes, callback) ->
  values = []
  for e in envelopes
    values.push(e.id, JSON.stringify(e))
  client().mset(values, callback)

exports.remove = (id, callback) ->
  client().del(id, callback)

exports.remove_many = (ids, callback) ->
  client().del(ids, callback)

exports.load = (id, callback) ->
  client().get id, (err, envelope) ->
    return callback(err) if err?
    callback(null, JSON.parse(envelope))

exports.load_many = (ids, callback) ->
  return process.nextTick(-> callback(null, [])) if ids.length is 0
  client().mget ids, (err, envelopes) ->
    return callback(err) if err?
    callback(null, envelopes.map (e) -> JSON.parse(e))
