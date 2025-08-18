require('dotenv').config();
const { MongoClient, ObjectId } = require('mongodb');
const axios = require('axios');

// --- Configuración ---
const uri = process.env.MONGO_URI; // Usamos el nombre de tu variable de entorno
const dbName = 'duende-db';
const collectionName = 'events'; // La colección principal
const batchSize = 50; // Procesar de 50 en 50 para evitar sobrecargar la API

// --- Cliente de MongoDB ---
const client = new MongoClient(uri);

/**
 * Geocodifica una dirección usando la API de Nominatim.
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
            headers: { 'User-Agent': 'DuendeFinder/1.0 (https://github.com/YOUR_USERNAME/DuendeFinderProject)' }
        });
        if (response.data && response.data.length > 0) {
            const result = response.data[0];
            const lon = parseFloat(result.lon);
            const lat = parseFloat(result.lat);
            return { type: 'Point', coordinates: [lon, lat] };
        } else {
            return null;
        }
    } catch (error) {
        console.error(`Error al geocodificar la dirección "${address}":`, error.message);
        return null;
    }
}

/**
 * Procesa los eventos existentes que no tienen coordenadas.
 */
async function backfillEventLocations() {
    console.log('Iniciando script de limpieza de geolocalización...');
    try {
        await client.connect();
        console.log('Conectado a MongoDB.');

        const database = client.db(dbName);
        const eventsCollection = database.collection(collectionName);

        // Busca eventos que no tienen el campo 'location' o que es nulo
        const eventsToUpdate = await eventsCollection.find({ location: { $exists: false } }).toArray();
        console.log(`Se encontraron ${eventsToUpdate.length} eventos sin coordenadas para actualizar.`);

        if (eventsToUpdate.length === 0) {
            console.log('No hay eventos que necesiten geocodificación. ¡La base de datos está al día!');
            return;
        }

        let processedCount = 0;
        for (const event of eventsToUpdate) {
            const fullAddress = [event.venue, event.city, event.country]
                .filter(Boolean)
                .join(', ');

            const location = await getCoordinates(fullAddress);

            if (location) {
                // Actualiza el documento en la base de datos con el nuevo campo 'location'
                await eventsCollection.updateOne(
                    { _id: new ObjectId(event._id) },
                    { $set: { location: location } }
                );
                console.log(`-> Evento "${event.name}" actualizado con éxito.`);
            } else {
                console.warn(`[AVISO] No se pudo geocodificar la dirección para el evento "${event.name}".`);
            }
            processedCount++;
        }

        console.log(`Proceso de limpieza completado. ${processedCount} eventos procesados.`);

    } catch (err) {
        console.error('Ocurrió un error durante el script de limpieza:', err);
    } finally {
        await client.close();
        console.log('Conexión a MongoDB cerrada.');
    }
}

// Ejecutar el script
backfillEventLocations();