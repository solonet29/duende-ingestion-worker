// ingestion-logic.js (Versión con deduplicación mejorada por artista y fecha)
const { ObjectId } = require('mongodb');

async function runIngestionProcess(database, data) {
    const artistsCollection = database.collection('artists');
    const venuesCollection = database.collection('venues');
    const eventsCollection = database.collection('events');

    let summary = {
        artistas: { added: 0, updated: 0 },
        salas: { added: 0, updated: 0 },
        eventos: { added: 0, updated: 0, duplicates: 0 } // <-- AÑADIDO: Contador para duplicados
    };

    // La lógica para artistas y salas se mantiene, ya que no son eventos que puedan duplicarse
    // de la misma forma y el upsert es la estrategia correcta para ellos.
    const listaArtistas = data.artistas || data.artists || [];
    if (listaArtistas.length > 0) {
        for (const artista of listaArtistas) {
            const { _id, ...artistData } = artista;
            const result = await artistsCollection.updateOne({ id: artistData.id }, { $set: artistData }, { upsert: true });
            if (result.upsertedCount > 0) summary.artistas.added++;
            else if (result.matchedCount > 0) summary.artistas.updated++;
        }
    }

    const listaSalas = data.salas || data.venues || [];
    if (listaSalas.length > 0) {
        for (const sala of listaSalas) {
            const { _id, ...venueData } = sala;
            const result = await venuesCollection.updateOne({ id: venueData.id }, { $set: venueData }, { upsert: true });
            if (result.upsertedCount > 0) summary.salas.added++;
            else if (result.matchedCount > 0) summary.salas.updated++;
        }
    }

    // --- LÓGICA MODIFICADA PARA EVENTOS ---
    const listaEventos = data.eventos || data.events || [];
    if (listaEventos.length > 0) {
        console.log(`Procesando ${listaEventos.length} eventos...`);
        for (const evento of listaEventos) {
            const { _id, ...eventData } = evento;

            // <-- CAMBIO CLAVE: Usamos artista y fecha para la deduplicación
            // Es la forma más fiable de identificar un evento único.
            // Primero, validamos que los campos existen.
            if (!eventData.artist || !eventData.date) {
                console.log(`⚠️ Evento descartado por falta de datos clave (artista o fecha):`, eventData);
                continue;
            }

            // Buscamos si ya existe un evento con el mismo artista y fecha
            const existingEvent = await eventsCollection.findOne({
                artist: eventData.artist,
                date: eventData.date
            });

            if (existingEvent) {
                // Si existe un evento con el mismo artista y fecha, lo consideramos duplicado
                console.log(`⏭️ Evento duplicado de '${eventData.artist}' en '${eventData.date}' no se inserta.`);
                summary.eventos.duplicates++;
            } else {
                // Si no existe, lo insertamos como un nuevo evento.
                const insertResult = await eventsCollection.insertOne({
                    ...eventData,
                    contentStatus: 'pending' // <-- LA ETIQUETA PARA EL GEM DE CONTENIDOS
                });
                console.log(`✅ Nuevo evento insertado con ID: ${insertResult.insertedId}`);
                summary.eventos.added++;
            }
        }
    }

    console.log('Lógica de ingesta completada. Resumen:', summary);
    return summary;
}

module.exports = { runIngestionProcess };