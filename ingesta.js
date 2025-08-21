// ======================================================================
// SCRIPT: ingesta.js
// OBJETIVO: Este script procesa eventos desde una colección temporal,
//           sanea sus datos, evita duplicados y los inserta en la
//           colección principal de eventos.
// Plataforma: Vercel Cron Jobs.
// ======================================================================

// --- Dependencias y Configuración ---
// ----------------------------------------------------------------------
require('dotenv').config();
const { MongoClient, ObjectId } = require('mongodb');
const axios = require('axios'); // Usado para futuras extensiones de geocodificación, aunque no en este flujo de ingesta.

// Variables de entorno para la conexión a la base de datos
const uri = process.env.MONGO_URI;
const dbName = 'duende-db';

// Nombres de las colecciones. Asegúrate de que coincidan con tu base de datos.
const tempCollectionName = 'temp_scraped_events';
const finalCollectionName = 'events';

// Cliente de MongoDB para la conexión
const client = new MongoClient(uri);

// ======================================================================
// FUNCIÓN PRINCIPAL: processEvents()
// Orquestador del flujo de ingesta.
// ======================================================================
async function processEvents() {
  console.log('Iniciando proceso de ingesta de eventos...');

  // Objeto para llevar un registro de las operaciones
  const summary = {
    eventos: { added: 0, duplicates: 0, failed: 0 }
  };

  try {
    // --- Conexión y acceso a las colecciones ---
    await client.connect();
    console.log('Conectado a MongoDB.');
    const database = client.db(dbName);
    const tempCollection = database.collection(tempCollectionName);
    const finalCollection = database.collection(finalCollectionName);

    // --- Lectura de los eventos a procesar ---
    // Se leen todos los documentos de la colección temporal.
    const eventsToProcess = await tempCollection.find({}).toArray();
    console.log(`Se encontraron ${eventsToProcess.length} eventos para procesar.`);

    // --- Bucle de procesamiento de eventos ---
    // Iteramos sobre cada evento encontrado para sanearlo y guardarlo.
    for (const event of eventsToProcess) {
      console.log(`Procesando evento: ${event.name}`);

      // Paso 1: Saneamiento de Datos 🧹
      // Creamos una copia del objeto para trabajar con él.
      const eventData = { ...event };

      // Verificamos y asignamos valores por defecto si los campos están vacíos o nulos.
      if (!eventData.artist || eventData.artist.length === 0) {
        eventData.artist = 'Artista no especificado';
      }
      if (!eventData.date || eventData.date.length === 0) {
        eventData.date = 'Fecha no disponible';
      }
      if (!eventData.description || eventData.description.length === 0) {
        eventData.description = 'Más información en la web del evento.';
      }

      // Verificamos un campo clave para el control de errores.
      if (!eventData.sourceUrl) {
        console.warn(`⚠️ Evento descartado por falta de 'sourceUrl':`, eventData.name);
        await tempCollection.deleteOne({ _id: new ObjectId(event._id) });
        summary.eventos.failed++;
        continue; // Pasamos al siguiente evento del bucle.
      }

      // Paso 2: Deduplicación por sourceUrl 💾
      // Buscamos si ya existe un evento con la misma URL en la colección final.
      const existingEvent = await finalCollection.findOne({
        sourceUrl: eventData.sourceUrl
      });

      if (existingEvent) {
        console.log(`⏭️ Evento duplicado de '${eventData.name}' no se inserta.`);
        summary.eventos.duplicates++;
      } else {
        // Si no existe, lo insertamos como un nuevo evento.
        try {
          const insertResult = await finalCollection.insertOne({
            ...eventData,
            contentStatus: 'pending' // Etiquetamos para el bot de contenidos.
          });
          console.log(`✅ Nuevo evento insertado con ID: ${insertResult.insertedId}`);
          summary.eventos.added++;
        } catch (error) {
          // Manejo de errores de inserción
          console.error('Error al insertar el evento:', error);
          summary.eventos.failed++;
        }
      }

      // Paso 3: Limpieza de la colección temporal 🧹
      // Eliminamos el evento de la colección temporal, independientemente de si se insertó o no.
      await tempCollection.deleteOne({ _id: new ObjectId(event._id) });
    }

    console.log('Proceso de ingesta completado. Resumen:', summary);

  } catch (err) {
    // Manejo de errores de conexión o del proceso general
    console.error('Ocurrió un error durante el proceso de ingesta:', err);
  } finally {
    // Aseguramos que la conexión a la base de datos siempre se cierre.
    await client.close();
    console.log('Conexión a MongoDB cerrada.');
  }
}

// Ejecutar el script
processEvents();