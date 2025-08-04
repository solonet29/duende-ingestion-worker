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
    console.error('Acceso no autorizado. Contraseña incorrecta.');
    res.status(401).json({ error: 'Acceso no autorizado' });
  }
};

// Endpoint principal para la ingesta de eventos
app.post('/ingest', authMiddleware, async (req, res) => {
  console.log('Petición recibida para la ingesta de eventos...');

  const client = new MongoClient(uri);

  try {
    // Conexión a la base de datos y colecciones correctas
    await client.connect();
    const database = client.db('DuendeDB'); // <-- ¡Base de datos corregida!
    const eventsCollection = database.collection('events'); // <-- ¡Colección corregida!

    // Cargar los datos del archivo JSON
    const filePath = path.join(__dirname, '..', 'nuevos_eventos.json');
    const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));

    let bulkOps = [];

    // Preparar operaciones para artistas
    if (data.artistas && data.artistas.length > 0) {
      console.log(`Preparando ${data.artistas.length} artistas para la ingesta...`);
      const opsArtistas = data.artistas.map(artista => ({
        updateOne: {
          filter: { id: artista.id_artista }, // Usamos un campo 'id' para la unificación
          update: { $set: { ...artista, tipo: 'artista' } }, // Añadimos un campo 'tipo'
          upsert: true
        }
      }));
      bulkOps = bulkOps.concat(opsArtistas);
    }

    // Preparar operaciones para salas
    if (data.salas_tablos_festivales && data.salas_tablos_festivales.length > 0) {
      console.log(`Preparando ${data.salas_tablos_festivales.length} salas para la ingesta...`);
      const opsSalas = data.salas_tablos_festivales.map(sala => ({
        updateOne: {
          filter: { id: sala.id_sala }, // Usamos un campo 'id' para la unificación
          update: { $set: { ...sala, tipo: 'sala' } }, // Añadimos un campo 'tipo'
          upsert: true
        }
      }));
      bulkOps = bulkOps.concat(opsSalas);
    }

    // Ejecutar todas las operaciones en una sola escritura masiva
    if (bulkOps.length > 0) {
      await eventsCollection.bulkWrite(bulkOps);
      console.log('Ingesta en la colección "events" completada con éxito.');
    } else {
      console.log('No hay datos para ingestar.');
    }

    // Envía una respuesta de éxito
    res.status(200).json({ message: 'Ingesta de eventos completada con éxito.' });

  } catch (error) {
    console.error('Error durante la ingesta:', error);
    res.status(500).json({ error: 'Error interno del servidor durante la ingesta.' });
  } finally {
    // Asegúrate de cerrar la conexión a la base de datos
    await client.close();
  }
});

module.exports = app;