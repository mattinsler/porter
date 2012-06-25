var Worker = require('porter').Worker;

Worker.on('log:log', function(opts, callback) {
  console.log(opts.message);
  process.nextTick(callback);
});

Worker.on('log:error', function(opts, callback) {
  console.log('ERROR: ' + opts.message);
  console.log(opts.stack);
  process.nextTick(callback);
});

new Worker().start();
