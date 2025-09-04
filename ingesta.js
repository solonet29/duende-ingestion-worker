// ======================================================================
// SCRIPT: ingesta.js (Versión 2.0 con Fallback a Ciudad)
// OBJETIVO: Procesar eventos, aplicando fallback de geocodificación
//           para maximizar la calidad del dato en el momento de entrada.
// Plataforma: Vercel Cron Jobs.
// ======================================================================

// --- Dependencias y Configuración ---
// ----------------------------------------------------------------------
require('dotenv').config();
const { MongoClient, ObjectId } = require('mongodb');
const { Client } = require("@googlemaps/google-maps-services-js");
const googleMapsClient = new Client({});

// Variables de entorno
const uri = process.env.MONGO_URI;
const dbName = 'DuendeDB';
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY;

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
    referenceURL: { $in: urls }
  }).project({ referenceURL: 1 }).toArray();

  return new Set(existingEvents.map(e => e.referenceURL));
}

/**
 * Geocodifica una dirección usando la API de Google Maps.
 * @param {string} address - La dirección a geocodificar.
 * @returns {Promise<number[] | null>} Un array con [longitud, latitud] o null si falla.
 */
async function geocodeAddress(address) {
  if (!address) return null;
  try {
    const response = await googleMapsClient.geocode({
      params: {
        address: address,
        key: GOOGLE_MAPS_API_KEY,
        components: 'country:ES' // Prioriza resultados en España
      },
    });
    const { results } = response.data;
    if (results.length > 0) {
      const location = results[0].geometry.location;
      // MongoDB usa [longitud, latitud]
      return [location.lng, location.lat];
    }
  } catch (error) {
    console.error(`❌ Error al geocodificar la dirección '${address}':`, error.response?.data?.error_message || error.message);
  }
  return null;
}


// ======================================================================
// FUNCIÓN PRINCIPAL: processEvents()
// Orquestador del flujo de ingesta.
// ======================================================================
async function processEvents() {
  console.log('🚀 Iniciando proceso de ingesta de eventos (v2.0 con Fallback)...');
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
      .map(event => event.referenceURL)
      .filter(Boolean);
    const existingUrls = await findExistingUrls(urlsToCheck, finalCollection);
    console.log(`✅ Comprobación de duplicados realizada. ${existingUrls.size} URLs ya existen.`);

    // 3. Procesar cada evento individualmente
    for (const event of eventsToProcess) {
      const sanitizedEvent = sanitizeEvent(event);

      if (!sanitizedEvent.referenceURL) {
        console.warn(`⚠️ Evento descartado por falta de 'referenceURL':`, sanitizedEvent.name);
        summary.invalid++;
      } else if (existingUrls.has(sanitizedEvent.referenceURL)) {
        console.log(`⏭️  Evento duplicado (misma referenceURL) descartado: '${sanitizedEvent.name}'`);
        summary.duplicates++;
      } else {
        try {
          // ============================================================
          // Bloque de Geocodificación con Fallback
          // ============================================================
          let coordinates = null;
          let isApproximate = false;

          // 1. Plan A: Intentar con la dirección exacta
          if (sanitizedEvent.address) {
            console.log(`   📍 Geocodificando dirección para '${sanitizedEvent.name}': ${sanitizedEvent.address}`);
            coordinates = await geocodeAddress(sanitizedEvent.address);
          }

          // 2. Plan B: Si falla, intentar con la ciudad
          if (!coordinates && sanitizedEvent.city) {
            console.log(`   🏙️  Fallback a ciudad para '${sanitizedEvent.name}': ${sanitizedEvent.city}`);
            coordinates = await geocodeAddress(`${sanitizedEvent.city}, España`);
            if (coordinates) {
              isApproximate = true;
            }
          }

          // 3. Asignar o eliminar el campo 'location'
          if (coordinates) {
            sanitizedEvent.location = {
              type: 'Point',
              coordinates: coordinates,
              isApproximate: isApproximate
            };
            const type = isApproximate ? 'Aproximada' : 'Exacta';
            console.log(`   ✨ Ubicación ${type} encontrada.`);
          } else if (sanitizedEvent.location) {
            // Si no se encontró nada y el campo existía por alguna razón, lo eliminamos
            delete sanitizedEvent.location;
            console.log(`   ⚠️ No se pudo geocodificar. Descartando campo 'location'.`);
          }
          // ============================================================

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

// Exportar la función para que pueda ser llamada desde otros módulos, como el endpoint de la API.
module.exports = { processEvents };

// Iniciar el proceso si el script se ejecuta directamente
if (require.main === module) {
  processEvents();
}