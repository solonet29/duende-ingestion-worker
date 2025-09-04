// ======================================================================
// SCRIPT: reparador-geocodificacion.js (Versión 2.0 con Fallback a Ciudad)
// OBJETIVO: Geocodificar eventos sin 'location'. Si la dirección
//           falla, intenta geocodificar la ciudad como fallback.
// ======================================================================

require('dotenv').config();
const { MongoClient } = require('mongodb');
const { Client } = require("@googlemaps/google-maps-services-js");
const googleMapsClient = new Client({});

// --- Configuración ---
const uri = process.env.MONGO_URI;
const dbName = 'DuendeDB';
const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY;
const FINAL_COLLECTION_NAME = 'events';

const client = new MongoClient(uri);

async function geocodeAddress(address) {
    if (!address) return null;
    try {
        const response = await googleMapsClient.geocode({
            params: {
                address: address,
                key: GOOGLE_MAPS_API_KEY,
                // Añadimos un componente para priorizar resultados en España
                components: 'country:ES'
            },
        });
        const { results } = response.data;
        if (results.length > 0) {
            const location = results[0].geometry.location;
            return [location.lng, location.lat];
        }
    } catch (error) {
        console.error(`❌ Error al geocodificar '${address}':`, error.response?.data?.error_message || error.message);
    }
    return null;
}

async function fixMissingLocations() {
    console.log('🚀 Iniciando script para reparar geolocalizaciones (v2.0 con Fallback)...');
    let updatedExact = 0;
    let updatedApprox = 0;
    let failedCount = 0;

    if (!GOOGLE_MAPS_API_KEY) {
        console.error('🛑 Error: La variable de entorno GOOGLE_MAPS_API_KEY no está definida.');
        return;
    }

    try {
        await client.connect();
        console.log('🔗 Conectado a MongoDB.');
        const database = client.db(dbName);
        const eventsCollection = database.collection(FINAL_COLLECTION_NAME);

        const eventsToFix = await eventsCollection.find({
            $or: [
                // Condición 1: Tiene dirección pero no ubicación
                { address: { $exists: true, $ne: null, $ne: "" }, location: { $exists: false } },
                // Condición 2: Tiene ciudad pero no ubicación
                { city: { $exists: true, $ne: null, $ne: "" }, location: { $exists: false } }
            ]
        }).toArray();


        if (eventsToFix.length === 0) {
            console.log('✅ ¡No hay eventos que necesiten reparación!');
            return;
        }

        console.log(`🔎 Se encontraron ${eventsToFix.length} eventos para intentar reparar.`);

        for (const event of eventsToFix) {
            console.log(`\n🛠️  Procesando evento: '${event.name}' (ID: ${event._id})`);
            let coordinates = null;
            let isApproximate = false;

            // 1. Plan A: Intentar con la dirección exacta
            if (event.address) {
                console.log(`   📍 Intentando con dirección exacta: ${event.address}`);
                coordinates = await geocodeAddress(event.address);
            }

            // 2. Plan B: Si falla el Plan A, intentar con la ciudad
            if (!coordinates && event.city) {
                console.log(`   🏙️  Fallback: Intentando con la ciudad: ${event.city}`);
                coordinates = await geocodeAddress(`${event.city}, España`);
                if (coordinates) {
                    isApproximate = true;
                }
            }

            // 3. Si tenemos coordenadas (del Plan A o B), actualizamos
            if (coordinates) {
                const newLocation = {
                    type: 'Point',
                    coordinates: coordinates,
                    isApproximate: isApproximate
                };

                await eventsCollection.updateOne(
                    { _id: event._id },
                    { $set: { location: newLocation } }
                );

                if (isApproximate) {
                    console.log(`   ✨ ¡Éxito (Aproximado)! Coordenadas añadidas: [${coordinates.join(', ')}]`);
                    updatedApprox++;
                } else {
                    console.log(`   ✨ ¡Éxito (Exacto)! Coordenadas añadidas: [${coordinates.join(', ')}]`);
                    updatedExact++;
                }
            } else {
                console.log(`   ⚠️  Fallo en ambos intentos. El evento no será actualizado.`);
                failedCount++;
            }
        }

    } catch (err) {
        console.error('💥 Ocurrió un error crítico durante el proceso:', err);
    } finally {
        await client.close();
        console.log('\n🚪 Conexión a MongoDB cerrada.');
    }

    console.log('\n--- Resumen de la Reparación (v2.0) ---');
    console.log(`Eventos Reparados (Ubicación Exacta): ${updatedExact}`);
    console.log(`Eventos Reparados (Ubicación de Ciudad): ${updatedApprox}`);
    console.log(`Eventos Fallidos: ${failedCount}`);
    console.log('---------------------------------------\n');
}

fixMissingLocations();