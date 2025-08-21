// ingestion-logic.js (Versión con deduplicación mejorada y saneamiento)
const { ObjectId } = require('mongodb');

async function runIngestionProcess(database, data) {
    const artistsCollection = database.collection('artists');
    const venuesCollection = database.collection('venues');
    const eventsCollection = database.collection('events');
    const tempCollection = database.collection('temp_scraped_events'); // Añadimos la colección temporal

    let summary = {
        artistas: { added: 0, updated: 0 },
        salas: { added: 0, updated: 0 },
        eventos: { added: 0, updated: 0, duplicates: 0, discarded: 0, failed: 0 } // <-- Actualizamos el resumen
    };

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

            // --- PASO 1: Saneamiento de Datos ---
            // Asignamos valores por defecto a los campos nulos o vacíos.
            if (!eventData.artist || eventData.artist.length === 0) {
                eventData.artist = 'Artista no especificado';
            }
            if (!eventData.date || eventData.date.length === 0) {
                eventData.date = 'Fecha no disponible';
            }
            if (!eventData.description || eventData.description.length === 0) {
                eventData.description = 'Más información en la web del evento.';
            }

            // <-- IMPORTANTE: Ahora el 'continue' solo ocurre si no hay un identificador
            // Esto es crucial para no perder la referencia original.
            if (!eventData.sourceUrl) {
                console.log(`⚠️ Evento descartado por falta de 'sourceUrl':`, eventData);
                summary.eventos.discarded++;
                continue;
            }

            // --- PASO 2: Deduplicación ---
            // La lógica de deduplicación se mantiene, pero ahora los campos ya están saneados.
            const existingEvent = await eventsCollection.findOne({
                sourceUrl: eventData.sourceUrl // <-- Cambio clave: Usamos la URL de origen como identificador único
            });

            if (existingEvent) {
                console.log(`⏭️ Evento duplicado de '${eventData.sourceUrl}' no se inserta.`);
                summary.eventos.duplicates++;
            } else {
                try {
                    // --- PASO 3: Inserción y Limpieza ---
                    const insertResult = await eventsCollection.insertOne({
                        ...eventData,
                        contentStatus: 'pending'
                    });
                    console.log(`✅ Nuevo evento insertado con ID: ${insertResult.insertedId}`);
                    summary.eventos.added++;
                } catch (error) {
                    console.error('Error al insertar el evento:', error);
                    summary.eventos.failed++;
                }
            }

            // Independientemente de si se insertó o se descartó por duplicado, lo eliminamos de la colección temporal
            await tempCollection.deleteOne({ _id: new ObjectId(evento._id) });
        }
    }

    console.log('Lógica de ingesta completada. Resumen:', summary);
    return summary;
}

module.exports = { runIngestionProcess };