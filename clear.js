
const { DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');

(async () => {
    console.log('limpando tabelas');
    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    try {
        await db.query("DELETE FROM api_data;")
		console.log( await db.api_data.find() );

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();




 

