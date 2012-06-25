# redis namespace
exports.namespace = 'porter'

# timeout for a job to get reaped (set on pop)
exports.timeout = 30

exports.redis = {
  host: 'localhost'
  port: 6379
  create_client: ->
    # possible pooling?
    return exports.redis.__client__ if exports.redis.__client__?
    
    redis = require 'redis'
    
    url_config = require('url').parse(exports.url) if exports.url?
    [url_username, url_password] = url_config.auth.split(':') if url_config?.auth?
    
    host = url_config?.hostname || exports.host
    port = url_config?.port || exports.port
    username = url_username || exports.username
    password = url_password || exports.password
    
    client = redis.createClient(port, host)
    client.auth(password) if password?
    
    exports.redis.__client__ = client
}

exports.worker = {
  queues: null
  min_poll_timeout: 100
  max_poll_timeout: 5000
  concurrent_commands: 1
}

exports.RedisPayloadStorage = require './redis_payload_storage'

exports.payload_storage = exports.RedisPayloadStorage
