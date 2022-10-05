
const { DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');

const axios = require('axios');


const startDate ='2018';
const endDate ='2020';
        


async function loadData(){
    const response = await axios.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population');

    return response.data.data;
}


function calculateAveragePopulation( result ){
    let ano = result.filter((element) => {
        return element.Year >= startDate && element.Year <= endDate;
    })

    // console.log(ano)

    let population = ano.reduce(
        (previous, currentValue) => previous + currentValue.Population, 0
    );

    // console.log(population)

    return population;
}


// Call start
(async () => {
    console.log('main.js: before start');
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

        //puxar dados e calcular média localmente
        const data = await loadData();
        const localResult = calculateAveragePopulation(data);



        //inserir dados no banco
        const db_data = data.map( dt => {

            return {
                doc_name: dt["Nation"],
                doc_id: dt["ID Nation"],
                doc_record: dt,
            }
        })

        await db.api_data.insert(db_data);


        
        //exemplo select
        // const db_result = await db.query("SELECT * FROM api_data")
        // console.log("dados",db_result);
        //exemplo de find
        // const result2 = await db.api_data.find({
        //     is_active: true
        // });

        //calcular somatório da população banco

        const QUERY_SUM =  `SUM( CASE WHEN ( (doc_record->>'Year')::text <= '${endDate}' AND (doc_record->>'Year')::text >= '${startDate}' ) THEN (doc_record->>'Population')::int ELSE 0 END)`;

        const sum = ( await db.query(`SELECT ${QUERY_SUM} AS result FROM api_data`) )[0].result;

        console.log("resultados")
        console.table({
            local: localResult,
            db: sum
        });
        

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();




 

