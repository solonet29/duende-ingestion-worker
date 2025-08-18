require('dotenv').config();
const { MongoClient, ObjectId } = require('mongodb');
const axios = require('axios');

// --- Configuración ---
const uri = process.env.MONGODB_URI;
const dbName = 'duende-db';
const tempCollectionName = 'temp_events';
const finalCollectionName = 'events';

// --- Cliente de MongoDB ---
const client = new MongoClient(uri);

/**
 * Geocodifica una dirección usando la API de Nominatim (OpenStreetMap).
 * @param {string} address - La dirección a geocodificar.
 * @returns {Promise<object|null>} Un objeto GeoJSON Point o null si falla.
 */
async function getCoordinates(address) {
  if (!address || typeof address !== 'string' || address.trim() === '') {
    return null;
  }

  const encodedAddress = encodeURIComponent(address);
  const url = `https://nominatim.openstreetmap.org/search?q=${encodedAddress}&format=json&limit=1`;

  try {
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'DuendeFinder/1.0 (https://github.com/YOUR_USERNAME/DuendeFinderProject)'
      }
    });

    if (response.data && response.data.length > 0) {
      const result = response.data[0];
      const lon = parseFloat(result.lon);
      const lat = parseFloat(result.lat);

      return {
        type: 'Point',
        coordinates: [lon, lat]
      };
    } else {
      return null;
    }
  } catch (error) {
    console.error(`Error al geocodificar la dirección "${address}":`, error.message);
    return null;
  }
}

/**
 * Procesa los eventos de la colección temporal, los enriquece y los mueve a la colección final.
 */
async function processEvents() {
  console.log('Iniciando proceso de ingesta de eventos...');
  try {
    await client.connect();
    console.log('Conectado a MongoDB.');

    const database = client.db(dbName);
    const tempCollection = database.collection(tempCollectionName);
    const finalCollection = database.collection(finalCollectionName);

    // Si tu Ingestor crea el índice, asegúrate de que esté correcto
    await finalCollection.createIndex({ location: "2dsphere" });
    console.log('Índice 2dsphere asegurado en la colección final.');

    const eventsToProcess = await tempCollection.find({}).toArray();
    console.log(`Se encontraron ${eventsToProcess.length} eventos para procesar.`);

    for (const event of eventsToProcess) {
      console.log(`Procesando evento: ${event.name}`);

      // MODIFICADO: Combinar los campos de lugar para crear una dirección completa
      const fullAddress = [event.venue, event.city, event.country]
        .filter(Boolean)
        .join(', ');

      const location = await getCoordinates(fullAddress);

      if (location) {
        // Enriquecer el documento del evento con la ubicación
        const enrichedEvent = {
          ...event,
          location: location,
          contentStatus: 'pending' // Añadir el estado para el bot de contenido
        };

        // Insertar en la colección final y evitar duplicados
        const exists = await finalCollection.findOne({ id: enrichedEvent.id });
        if (!exists) {
          await finalCollection.insertOne(enrichedEvent);
          console.log(`-> Evento "${event.name}" enriquecido y guardado.`);
        } else {
          console.log(`-> Evento "${event.name}" ya existe. Se omite.`);
        }

        // Eliminar de la colección temporal después de procesar
        await tempCollection.deleteOne({ _id: new ObjectId(event._id) });

      } else {
        console.warn(`[AVISO] No se pudo geocodificar la dirección para el evento "${event.name}". Se moverá a la colección de fallidos.`);
        await database.collection('failed_ingestion_events').insertOne(event);
        await tempCollection.deleteOne({ _id: new ObjectId(event._id) });
      }
    }

    console.log('Proceso de ingesta completado.');

  } catch (err) {
    console.error('Ocurrió un error durante el proceso de ingesta:', err);
  } finally {
    await client.close();
    console.log('Conexión a MongoDB cerrada.');
  }
}

// Ejecutar el script
processEvents();
