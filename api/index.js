// api/index.js

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path'); // <-- Añade esta línea

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
    // Conexión a la base de datos
    await client.connect();
    const database = client.db('duende');
    const artistasCollection = database.collection('artistas');
    const salasCollection = database.collection('salas_tablos_festivales');

    // Cargar los datos del archivo JSON usando una ruta absoluta
    const filePath = path.join(__dirname, 'nuevos_eventos.json'); // <-- Línea corregida
    const data = JSON.parse(fs.readFileSync(filePath, 'utf8')); // <-- Línea corregida

    // Insertar artistas, evitando duplicados
    if (data.artistas && data.artistas.length > 0) {
      console.log(`Intentando insertar ${data.artistas.length} artistas...`);
      const bulkOpsArtistas = data.artistas.map(artista => ({
        updateOne: {
          filter: { id_artista: artista.id_artista },
          update: { $set: artista },
          upsert: true
        }
      }));
      await artistasCollection.bulkWrite(bulkOpsArtistas);
      console.log('Artistas insertados/actualizados con éxito.');
    }

    // Insertar salas, evitando duplicados
    if (data.salas_tablos_festivales && data.salas_tablos_festivales.length > 0) {
      console.log(`Intentando insertar ${data.salas_tablos_festivales.length} salas...`);
      const bulkOpsSalas = data.salas_tablos_festivales.map(sala => ({
        updateOne: {
          filter: { id_sala: sala.id_sala },
          update: { $set: sala },
          upsert: true
        }
      }));
      await salasCollection.bulkWrite(bulkOpsSalas);
      console.log('Salas/tablos/festivales insertados/actualizados con éxito.');
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