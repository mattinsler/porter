class Service
  @Status = {
    STOPPED: 'stopped'
    RUNNING: 'running'
  }

  constructor: (@interval = 5000, @run_on_start = true) ->
    @status = Service.Status.STOPPED
  
  start: ->
    return if @interval_id
    
    @started_at = new Date()
    @status = Service.Status.RUNNING
    @interval_id = setInterval(=>
      @run()
    , @interval)
    
    @run() if @run_on_start
  
  stop: ->
    clearInterval(@interval_id)
    @interval_id = null
    @status = Service.Status.STOPPED
    @stopped_at = new Date()
  
  status: ->
    {
      state: @status
      started_at: @started_at
      stopped_at: @stopped_at
    }

  run: ->
    throw new Error('You must implement the run method in your service')

module.exports = Service
