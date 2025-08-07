// ingesta.js - (Versión corregida para ejecutar a mano)
require('dotenv').config();
const { MongoClient } = require('mongodb');
const { runIngestionProcess } = require('./ingestion-logic.js');

const MONGO_URI = process.env.MONGO_URI || process.env.MONGODB_URI;
const DB_NAME = "DuendeDB";

async function runManualIngestion() {
    console.log("Iniciando script de ingesta manual...");
    if (!MONGO_URI) {
        console.error("Error: La variable MONGODB_URI no está definida en tu archivo .env");
        return;
    }
    const client = new MongoClient(MONGO_URI);

    try {
        await client.connect();
        console.log("✅ Conectado con éxito a la base de datos.");
        const database = client.db(DB_NAME);

        // =================================================================
        // --- LA CORRECCIÓN ESTÁ AQUÍ ---
        // =================================================================
        // Antes leía de un archivo JSON. Ahora lee de la colección temporal.
        console.log("Leyendo eventos desde la colección temporal 'temp_scraped_events'...");
        const tempCollection = database.collection('temp_scraped_events');
        const eventosDesdeDB = await tempCollection.find({}).toArray();

        // Creamos el objeto 'data' que nuestra lógica de ingestión espera
        const data = {
            eventos: eventosDesdeDB,
            artistas: [], // Dejamos estos vacíos porque el ojeador solo trae eventos
            salas: []
        };
        console.log(`Se han encontrado ${data.eventos.length} eventos para procesar.`);
        // =================================================================

        // Llamamos a la lógica central con los datos correctos de la DB
        const summary = await runIngestionProcess(database, data);
        
        console.log("----------------------------------------");
        console.log("📊 Proceso de ingesta completado.");
        console.log("Resumen:", summary);

    } catch (error) {
        console.error("❌ Ha ocurrido un error durante el proceso:", error);
    } finally {
        await client.close();
        console.log("Conexión con la base de datos cerrada.");
    }
}

runManualIngestion();