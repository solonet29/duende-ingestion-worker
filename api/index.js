// api/index.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');

const app = express();
const uri = process.env.MONGODB_URI;

// Middleware
app.use(express.json());
app.use(cors());

// Middleware para verificar la contraseña
const authMiddleware = (req, res, next) => {
    const password = process.env.INGESTA_PASSWORD;
    const providedPassword = req.headers['x-api-password'];
    if (providedPassword === password && password) {
        next();
    } else {
        console.error('Acceso no autorizado.');
        res.status(401).json({ error: 'Acceso no autorizado' });
    }
};

// Endpoint principal para la ingesta
app.post('/ingest', authMiddleware, async (req, res) => {
    console.log('Petición de ingesta recibida...');
    const client = new MongoClient(uri);

    try {
        await client.connect();
        const database = client.db('DuendeDB');
        
        // --- ¡CAMBIO IMPORTANTE! AHORA TRABAJAREMOS CON 3 COLECCIONES ---
        const artistsCollection = database.collection('artists');
        const venuesCollection = database.collection('venues');
        const eventsCollection = database.collection('events');

        const filePath = path.join(__dirname, '..', 'nuevos_eventos.json');
        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));

        let summary = {
            artistas: { added: 0, updated: 0 },
            salas: { added: 0, updated: 0 },
            eventos: { added: 0, updated: 0 }
        };

        // --- 1. PROCESAR ARTISTAS ---
        if (data.artistas && data.artistas.length > 0) {
            console.log(`Procesando ${data.artistas.length} artistas...`);
            for (const artista of data.artistas) {
                const result = await artistsCollection.updateOne(
                    { id: artista.id },
                    { $set: artista },
                    { upsert: true }
                );
                if (result.upsertedCount > 0) summary.artistas.added++;
                else if (result.matchedCount > 0) summary.artistas.updated++;
            }
        }

        // --- 2. PROCESAR SALAS ---
        if (data.salas && data.salas.length > 0) {
            console.log(`Procesando ${data.salas.length} salas...`);
            for (const sala of data.salas) {
                const result = await venuesCollection.updateOne(
                    { id: sala.id },
                    { $set: sala },
                    { upsert: true }
                );
                if (result.upsertedCount > 0) summary.salas.added++;
                else if (result.matchedCount > 0) summary.salas.updated++;
            }
        }

        // --- 3. PROCESAR EVENTOS (LA LÓGICA QUE FALTABA) ---
        if (data.eventos && data.eventos.length > 0) {
            console.log(`Procesando ${data.eventos.length} eventos...`);
            for (const evento of data.eventos) {
                const result = await eventsCollection.updateOne(
                    { id: evento.id },
                    { $set: evento },
                    { upsert: true }
                );
                if (result.upsertedCount > 0) summary.eventos.added++;
                else if (result.matchedCount > 0) summary.eventos.updated++;
            }
        }

        console.log('Ingesta completada.', summary);
        res.status(200).json({ message: 'Ingesta completada con éxito.', summary });

    } catch (error) {
        console.error('Error durante la ingesta:', error);
        res.status(500).json({ error: 'Error interno del servidor durante la ingesta.' });
    } finally {
        await client.close();
    }
});

// Ruta de salud para comprobar que el worker está vivo
app.get('/', (req, res) => {
    res.status(200).send('Duende Ingestion Worker (v2 Unificado) está vivo.');
});

module.exports = app;