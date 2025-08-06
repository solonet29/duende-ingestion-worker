// api/index.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const { runIngestionProcess } = require('../ingestion-logic.js'); // <-- Importamos el cerebro

const app = express();
const uri = process.env.MONGODB_URI;

app.use(express.json());
app.use(cors());

const authMiddleware = (req, res, next) => {
    const password = process.env.INGESTA_PASSWORD;
    const providedPassword = req.headers['x-api-password'];
    if (providedPassword === password && password) {
        next();
    } else {
        res.status(401).json({ error: 'Acceso no autorizado' });
    }
};

app.post('/ingest', authMiddleware, async (req, res) => {
    console.log('Petición de ingesta recibida...');
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const database = client.db('DuendeDB');
        
        // La lógica pesada ya no está aquí
        const filePath = path.join(__dirname, '..', 'nuevos_eventos.json');
        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));

        // Simplemente llamamos a nuestro módulo compartido
        const summary = await runIngestionProcess(database, data);

        res.status(200).json({ message: 'Ingesta completada con éxito.', summary });
    } catch (error) {
        console.error('Error durante la ingesta:', error);
        res.status(500).json({ error: 'Error interno del servidor durante la ingesta.' });
    } finally {
        await client.close();
    }
});

app.get('/', (req, res) => {
    res.status(200).send('Duende Ingestion Worker (v2 Unificado) está vivo.');
});

module.exports = app;