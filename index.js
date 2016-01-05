var Importer = require('./lib/importer');

exports.handler = function(event, context) {
    if(!event.Records) {
        console.log('No records found');
        context.done();
    } else {
        var importer = new Importer(event.Records);
        importer.run();    
    }
};
