// ingestion-logic.js

async function runIngestionProcess(database, data) {
  const artistsCollection = database.collection('artists');
  const venuesCollection = database.collection('venues');
  const eventsCollection = database.collection('events');

  let summary = {
      artistas: { added: 0, updated: 0 },
      salas: { added: 0, updated: 0 },
      eventos: { added: 0, updated: 0 }
  };

  // --- 1. PROCESAR ARTISTAS ---
  const listaArtistas = data.artistas || data.artists || [];
  if (listaArtistas.length > 0) {
      console.log(`Procesando ${listaArtistas.length} artistas...`);
      for (const artista of listaArtistas) {
          const { _id, ...artistData } = artista; // Ignoramos el _id temporal
          const result = await artistsCollection.updateOne({ id: artistData.id }, { $set: artistData }, { upsert: true });
          if (result.upsertedCount > 0) summary.artistas.added++;
          else if (result.matchedCount > 0) summary.artistas.updated++;
      }
  }

  // --- 2. PROCESAR SALAS ---
  const listaSalas = data.salas || data.venues || [];
  if (listaSalas.length > 0) {
      console.log(`Procesando ${listaSalas.length} salas...`);
      for (const sala of listaSalas) {
          const { _id, ...venueData } = sala; // Ignoramos el _id temporal
          const result = await venuesCollection.updateOne({ id: venueData.id }, { $set: venueData }, { upsert: true });
          if (result.upsertedCount > 0) summary.salas.added++;
          else if (result.matchedCount > 0) summary.salas.updated++;
      }
  }

  // --- 3. PROCESAR EVENTOS ---
  const listaEventos = data.eventos || data.events || [];
  if (listaEventos.length > 0) {
      console.log(`Procesando ${listaEventos.length} eventos...`);
      for (const evento of listaEventos) {
          // ==============================================
          // --- LA SOLUCIÓN ESTÁ AQUÍ ---
          // Creamos una copia del objeto 'evento' pero sin el campo '_id'
          const { _id, ...eventData } = evento;
          // ==============================================

          const result = await eventsCollection.updateOne(
              { id: eventData.id }, // Buscamos por el 'id' que es un slug
              { $set: eventData },  // Actualizamos con los datos sin el '_id'
              { upsert: true }
          );
          if (result.upsertedCount > 0) summary.eventos.added++;
          else if (result.matchedCount > 0) summary.eventos.updated++;
      }
  }

  console.log('Lógica de ingesta completada.', summary);
  return summary;
}

module.exports = { runIngestionProcess };