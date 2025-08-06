// ingestion-logic.js

// Esta función es el "cerebro". Recibe el cliente de la base de datos y los datos a procesar.
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
  if (data.artistas && data.artistas.length > 0) {
      console.log(`Procesando ${data.artistas.length} artistas...`);
      for (const artista of data.artistas) {
          const result = await artistsCollection.updateOne({ id: artista.id }, { $set: artista }, { upsert: true });
          if (result.upsertedCount > 0) summary.artistas.added++;
          else if (result.matchedCount > 0) summary.artistas.updated++;
      }
  }

  // --- 2. PROCESAR SALAS ---
  if (data.salas && data.salas.length > 0) {
      console.log(`Procesando ${data.salas.length} salas...`);
      for (const sala of data.salas) {
          const result = await venuesCollection.updateOne({ id: sala.id }, { $set: sala }, { upsert: true });
          if (result.upsertedCount > 0) summary.salas.added++;
          else if (result.matchedCount > 0) summary.salas.updated++;
      }
  }

  // --- 3. PROCESAR EVENTOS ---
  if (data.eventos && data.eventos.length > 0) {
      console.log(`Procesando ${data.eventos.length} eventos...`);
      for (const evento of data.eventos) {
          const result = await eventsCollection.updateOne({ id: evento.id }, { $set: evento }, { upsert: true });
          if (result.upsertedCount > 0) summary.eventos.added++;
          else if (result.matchedCount > 0) summary.eventos.updated++;
      }
  }

  console.log('Lógica de ingesta completada.', summary);
  return summary; // Devuelve el resumen
}

// Exportamos la función para que otros archivos la puedan usar
module.exports = { runIngestionProcess };