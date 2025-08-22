// ======================================================================
// SCRIPT: ingesta.js
// OBJETIVO: Procesar eventos desde una colección temporal, sanear
//           sus datos, evitar duplicados y transferirlos a la
//           colección principal.
// Plataforma: Vercel Cron Jobs.
// ======================================================================

// --- Dependencias y Configuración ---
// ----------------------------------------------------------------------
require('dotenv').config();
const { MongoClient, ObjectId } = require('mongodb');

// Variables de entorno
const uri = process.env.MONGO_URI;
const dbName = 'DuendeDB';

// Nombres de las colecciones para evitar errores de tipeo
const TEMP_COLLECTION_NAME = 'temp_scraped_events';
const FINAL_COLLECTION_NAME = 'events';

// Constantes para valores por defecto y estados
const DEFAULT_ARTIST = 'Artista no especificado';
const DEFAULT_DATE = 'Fecha no disponible';
const DEFAULT_DESCRIPTION = 'Más información en la web del evento.';
const CONTENT_STATUS_PENDING = 'pending';

const client = new MongoClient(uri);

// --- Funciones Auxiliares ---
// ----------------------------------------------------------------------

/**
 * Sanea un objeto de evento, aplicando valores por defecto a campos vacíos.
 * @param {object} event - El objeto del evento original.
 * @returns {object} Un nuevo objeto de evento saneado.
 */
function sanitizeEvent(event) {
  const sanitized = { ...event };
  if (!sanitized.artist || sanitized.artist.length === 0) {
    sanitized.artist = DEFAULT_ARTIST;
  }
  if (!sanitized.date || sanitized.date.length === 0) {
    sanitized.date = DEFAULT_DATE;
  }
  if (!sanitized.description || sanitized.description.length === 0) {
    sanitized.description = DEFAULT_DESCRIPTION;
  }
  return sanitized;
}

/**
 * Busca en la colección final qué URLs de un listado ya existen.
 * @param {string[]} urls - Un array de referenceURL a verificar.
 * @param {Db.Collection} finalCollection - La colección donde buscar.
 * @returns {Promise<Set<string>>} Un Set con las URLs que ya existen.
 */
async function findExistingUrls(urls, finalCollection) {
  if (urls.length === 0) {
    return new Set();
  }
  const existingEvents = await finalCollection.find({
    referenceURL: { $in: urls } // CORREGIDO
  }).project({ referenceURL: 1 }).toArray(); // CORREGIDO

  return new Set(existingEvents.map(e => e.referenceURL)); // CORREGIDO
}

// ======================================================================
// FUNCIÓN PRINCIPAL: processEvents()
// Orquestador del flujo de ingesta.
// ======================================================================
async function processEvents() {
  console.log('🚀 Iniciando proceso de ingesta de eventos...');
  const summary = {
    processed: 0,
    added: 0,
    duplicates: 0,
    invalid: 0
  };

  try {
    await client.connect();
    console.log('🔗 Conectado a MongoDB.');
    const database = client.db(dbName);
    const tempCollection = database.collection(TEMP_COLLECTION_NAME);
    const finalCollection = database.collection(FINAL_COLLECTION_NAME);

    // 1. Leer todos los eventos de la colección temporal
    const eventsToProcess = await tempCollection.find({}).toArray();
    summary.processed = eventsToProcess.length;

    if (summary.processed === 0) {
      console.log('No hay eventos nuevos para procesar. Finalizando.');
      return;
    }
    console.log(`🔎 Se encontraron ${summary.processed} eventos para procesar.`);

    // 2. Optimización: Buscar todos los duplicados en una sola consulta
    const urlsToCheck = eventsToProcess
      .map(event => event.referenceURL) // CORREGIDO
      .filter(Boolean);
    const existingUrls = await findExistingUrls(urlsToCheck, finalCollection);
    console.log(`✅ Comprobación de duplicados realizada. ${existingUrls.size} URLs ya existen.`);

    // 3. Procesar cada evento individualmente
    for (const event of eventsToProcess) {
      const sanitizedEvent = sanitizeEvent(event);

      if (!sanitizedEvent.referenceURL) { // CORREGIDO
        console.warn(`⚠️ Evento descartado por falta de 'referenceURL':`, sanitizedEvent.name); // CORREGIDO
        summary.invalid++;
      } else if (existingUrls.has(sanitizedEvent.referenceURL)) { // CORREGIDO
        console.log(`⏭️  Evento duplicado (misma referenceURL) descartado: '${sanitizedEvent.name}'`); // CORREGIDO
        summary.duplicates++;
      } else {
        try {
          const insertResult = await finalCollection.insertOne({
            ...sanitizedEvent,
            contentStatus: CONTENT_STATUS_PENDING
          });
          console.log(`✨ Nuevo evento insertado con ID: ${insertResult.insertedId}`);
          summary.added++;
        } catch (error) {
          console.error(`❌ Error al insertar el evento '${sanitizedEvent.name}':`, error);
          summary.invalid++;
        }
      }

      await tempCollection.deleteOne({ _id: new ObjectId(event._id) });
    }

  } catch (err) {
    console.error('💥 Ocurrió un error crítico durante el proceso de ingesta:', err);
  } finally {
    await client.close();
    console.log('🚪 Conexión a MongoDB cerrada.');
  }

  console.log('\n--- Resumen de la Ingesta ---');
  console.log(`Eventos Procesados: ${summary.processed}`);
  console.log(`Nuevos Eventos Añadidos: ${summary.added}`);
  console.log(`Duplicados Descartados: ${summary.duplicates}`);
  console.log(`Inválidos/Fallidos: ${summary.invalid}`);
  console.log('-----------------------------\n');
}

// Ejecutar el script
processEvents();