// api/index.js

require('dotenv').config(); // Carga las variables de entorno desde .env
const express = require('express');
const cors = require('cors'); // Para permitir peticiones desde otros dominios
const { MongoClient } = require('mongodb');
const fs = require('fs');

const app = express();
const uri = process.env.MONGODB_URI; // URI de conexión a MongoDB del archivo .env

// Middleware
app.use(express.json());
app.use(cors());

// Middleware para verificar la contraseña
const authMiddleware = (req, res, next) => {
  const password = process.env.INGESTA_PASSWORD; // Contraseña del archivo .env
  const providedPassword = req.headers['x-api-password']; // La contraseña se enviará en el header 'x-api-password'
  if (providedPassword === password && password) {
    next(); // Si la contraseña es correcta, continúa con la ejecución
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

    // Cargar los datos del archivo JSON
    const data = JSON.parse(fs.readFileSync('nuevos_eventos.json', 'utf8'));

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

// ¡Importante!: en lugar de app.listen, exportamos la app para que Vercel la use.
module.exports = app;