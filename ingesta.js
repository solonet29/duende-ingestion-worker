
require('dotenv').config();
const { MongoClient, ObjectId } = require('mongodb');
const axios = require('axios');

// --- Configuración ---
// Lee la URI de MongoDB del archivo .env para mayor seguridad
const uri = process.env.MONGODB_URI;
const dbName = 'duende-db'; // O el nombre de tu base de datos
const tempCollectionName = 'temp_events'; // Colección de origen
const finalCollectionName = 'events'; // Colección de destino

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

  // Codifica la dirección para que sea segura en una URL
  const encodedAddress = encodeURIComponent(address);
  const url = `https://nominatim.openstreetmap.org/search?q=${encodedAddress}&format=json&limit=1`;

  try {
    // Nominatim requiere un User-Agent descriptivo para evitar bloqueos.
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'DuendeFinder/1.0 (https://github.com/YOUR_USERNAME/DuendeFinderProject)'
      }
    });

    // Verifica si la API devolvió resultados
    if (response.data && response.data.length > 0) {
      const result = response.data[0];
      const lon = parseFloat(result.lon);
      const lat = parseFloat(result.lat);

      // Devuelve el objeto en formato GeoJSON
      return {
        type: 'Point',
        coordinates: [lon, lat] // Formato: [longitud, latitud]
      };
    } else {
      return null; // No se encontraron resultados para la dirección
    }
  } catch (error) {
    // Maneja errores de red o de la API
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

    // Asegúrate de que la colección final tenga un índice geoespacial
    await finalCollection.createIndex({ location: "2dsphere" });
    console.log('Índice 2dsphere asegurado en la colección final.');

    const eventsToProcess = await tempCollection.find({}).toArray();
    console.log(`Se encontraron ${eventsToProcess.length} eventos para procesar.`);

    for (const event of eventsToProcess) {
      console.log(`Procesando evento: ${event.title}`);

      const location = await getCoordinates(event.address);

      if (location) {
        // Enriquecer el documento del evento con la ubicación
        const enrichedEvent = {
          ...event,
          location: location
        };

        // Insertar en la colección final
        await finalCollection.insertOne(enrichedEvent);
        console.log(`-> Evento "${event.title}" enriquecido y guardado.`);
        
        // Opcional: Eliminar de la colección temporal después de procesar
        await tempCollection.deleteOne({ _id: new ObjectId(event._id) });

      } else {
        // Si la geocodificación falla, muestra una advertencia y continúa
        console.warn(`  [AVISO] No se pudo geocodificar la dirección para el evento "${event.title}". Se omitirá.`);
        // Opcional: podrías moverlo a una colección de "fallidos" en lugar de omitirlo
        // await database.collection('failed_events').insertOne(event);
        // await tempCollection.deleteOne({ _id: new ObjectId(event._id) });
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
