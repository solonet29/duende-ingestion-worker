// ingesta.js - (Para ejecutar a mano con 'node ingesta.js')
require('dotenv').config(); // <-- Usamos variables de entorno, ¡más seguro!
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const { runIngestionProcess } = require('./ingestion-logic.js'); // <-- Importamos el mismo cerebro

const MONGO_URI = process.env.MONGODB_URI; // <-- Leemos la URI del .env
const DB_NAME = "DuendeDB";
const JSON_FILE_PATH = path.join(__dirname, 'nuevos_eventos.json');

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

        console.log(`Leyendo el archivo de nuevos eventos desde: ${JSON_FILE_PATH}`);
        const data = JSON.parse(fs.readFileSync(JSON_FILE_PATH, 'utf-8'));

        // Llamamos a la misma lógica central que usa Vercel
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