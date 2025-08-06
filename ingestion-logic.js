// ingestion-logic.js - VERSIÓN FLEXIBLE

async function runIngestionProcess(database, data) {
    const artistsCollection = database.collection('artists');
    const venuesCollection = database.collection('venues');
    const eventsCollection = database.collection('events');

    let summary = {
        artistas: { added: 0, updated: 0 },
        salas: { added: 0, updated: 0 },
        eventos: { added: 0, updated: 0 }
    };

    // --- 1. PROCESAR ARTISTAS (DE FORMA FLEXIBLE) ---
    // Intentamos coger 'data.artistas', si no existe, cogemos 'data.artists'. Si no, una lista vacía.
    const listaArtistas = data.artistas || data.artists || [];
    if (listaArtistas.length > 0) {
        console.log(`Procesando ${listaArtistas.length} artistas...`);
        for (const artista of listaArtistas) {
            const result = await artistsCollection.updateOne({ id: artista.id }, { $set: artista }, { upsert: true });
            if (result.upsertedCount > 0) summary.artistas.added++;
            else if (result.matchedCount > 0) summary.artistas.updated++;
        }
    }

    // --- 2. PROCESAR SALAS (DE FORMA FLEXIBLE) ---
    const listaSalas = data.salas || data.venues || [];
    if (listaSalas.length > 0) {
        console.log(`Procesando ${listaSalas.length} salas...`);
        for (const sala of listaSalas) {
            const result = await venuesCollection.updateOne({ id: sala.id }, { $set: sala }, { upsert: true });
            if (result.upsertedCount > 0) summary.salas.added++;
            else if (result.matchedCount > 0) summary.salas.updated++;
        }
    }

    // --- 3. PROCESAR EVENTOS (DE FORMA FLEXIBLE) ---
    const listaEventos = data.eventos || data.events || [];
    if (listaEventos.length > 0) {
        console.log(`Procesando ${listaEventos.length} eventos...`);
        for (const evento of listaEventos) {
            const result = await eventsCollection.updateOne({ id: evento.id }, { $set: evento }, { upsert: true });
            if (result.upsertedCount > 0) summary.eventos.added++;
            else if (result.matchedCount > 0) summary.eventos.updated++;
        }
    }

    console.log('Lógica de ingesta completada.', summary);
    return summary;
}

module.exports = { runIngestionProcess };