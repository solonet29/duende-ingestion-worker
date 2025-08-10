// ingesta.js - (Versión corregida y con registro de analíticas integrado)
require('dotenv').config();
const { MongoClient } = require('mongodb');
// Asumimos que 'runIngestionProcess' está en un archivo separado. Si no, intégralo aquí.
const { runIngestionProcess } = require('./ingestion-logic.js'); 

const MONGO_URI = process.env.MONGO_URI || process.env.MONGODB_URI;
const DB_NAME = "DuendeDB";

/**
 * // <-- AÑADIDO: Función auxiliar para registrar la ejecución en la colección de analíticas.
 * Es una función separada para mantener el código principal más limpio.
 */
async function logRun(db, botName, status, durationMs, eventsFound, results, errorMessage = null) {
    try {
        const analyticsCollection = db.collection('analytics_runs');
        const runData = {
            botName,
            runTimestamp: new Date(),
            status,
            durationMs,
            eventsFound, // El número de eventos que se intentaron procesar
            results,
            errorMessage
        };
        await analyticsCollection.insertOne(runData);
        console.log("📝 Registro de analíticas guardado con éxito.");
    } catch (logError) {
        // Si falla el guardado de la analítica, solo lo mostramos en consola
        // para no interrumpir el flujo principal.
        console.error("❌ ¡Fallo al guardar el registro de analíticas!:", logError);
    }
}


async function runManualIngestion() {
    console.log("Iniciando script de ingesta manual...");
    
    const startTime = Date.now(); // <-- AÑADIDO: Capturamos el tiempo de inicio.
    let client; // <-- MODIFICADO: Definimos client fuera para que sea accesible en 'finally'.
    let database; // <-- AÑADIDO: Definimos database aquí para que sea accesible en 'catch'.

    if (!MONGO_URI) {
        console.error("Error: La variable MONGODB_URI no está definida en tu archivo .env");
        // Registramos el fallo si es posible (aunque sin URI es improbable)
        const durationMs = Date.now() - startTime;
        console.log("📝 No se pudo registrar el fallo por falta de URI de la BD.");
        return;
    }
    
    client = new MongoClient(MONGO_URI);

    try {
        await client.connect();
        console.log("✅ Conectado con éxito a la base de datos.");
        database = client.db(DB_NAME); // <-- AÑADIDO: Asignamos valor a la variable.

        console.log("Leyendo eventos desde la colección temporal 'temp_scraped_events'...");
        const tempCollection = database.collection('temp_scraped_events');
        const eventosDesdeDB = await tempCollection.find({}).toArray();

        const data = {
            eventos: eventosDesdeDB,
            artistas: [],
            salas: []
        };
        console.log(`Se han encontrado ${data.eventos.length} eventos para procesar.`);

        const summary = await runIngestionProcess(database, data);
        
        console.log("----------------------------------------");
        console.log("📊 Proceso de ingesta completado.");
        console.log("Resumen:", summary);

        // <-- AÑADIDO: Registramos el éxito en la colección de analíticas.
        const durationMs = Date.now() - startTime;
        await logRun(database, 'ingestor-manual', 'success', durationMs, data.eventos.length, summary);

    } catch (error) {
        console.error("❌ Ha ocurrido un error durante el proceso:", error);

        // <-- AÑADIDO: Registramos el fallo en la colección de analíticas.
        const durationMs = Date.now() - startTime;
        if (database) { // Solo registramos si llegamos a conectarnos a la BD.
            await logRun(database, 'ingestor-manual', 'failure', durationMs, 0, {}, error.message);
        }

    } finally {
        if (client) { // <-- MODIFICADO: Comprobamos que client se haya inicializado.
            await client.close();
            console.log("Conexión con la base de datos cerrada.");
        }
    }
}

runManualIngestion();