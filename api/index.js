// api/index.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const { runIngestionProcess } = require('../ingestion-logic.js');

const app = express();
const uri = process.env.MONGODB_URI;

app.use(express.json());
app.use(cors());

// Lógica de ingesta unificada en una sola función
const ingestionHandler = async (req, res) => {
    console.log('Petición de ingesta recibida...');
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const database = client.db('DuendeDB');
        
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
        res.status(200).json({ message: 'Ingesta completada con éxito desde la base de datos.', summary });
    } catch (error) {
        console.error('Error durante la ingesta:', error);
        res.status(500).json({ error: 'Error interno del servidor durante la ingesta.' });
    } finally {
        await client.close();
    }
};

// Este es el handler GET que Vercel ejecutará con el cron job
app.get('/ingest', ingestionHandler);

// Este handler POST se mantiene para peticiones externas con autenticación
const authMiddleware = (req, res, next) => {
    const password = process.env.INGESTA_PASSWORD;
    const providedPassword = req.headers['x-api-password'];
    if (providedPassword === password && password) {
        next();
    } else {
        res.status(401).json({ error: 'Acceso no autorizado' });
    }
};
app.post('/ingest', authMiddleware, ingestionHandler);

app.get('/', (req, res) => {
    res.status(200).send('Duende Ingestion Worker (v2 Unificado) está vivo.');
});

module.exports = app;