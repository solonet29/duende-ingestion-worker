// ingestion-logic.js (Versión Final con "marcado" de contenido pendiente)

async function runIngestionProcess(database, data) {
  const artistsCollection = database.collection('artists');
  const venuesCollection = database.collection('venues');
  const eventsCollection = database.collection('events');

  let summary = {
      artistas: { added: 0, updated: 0 },
      salas: { added: 0, updated: 0 },
      eventos: { added: 0, updated: 0 }
  };

  // La lógica para artistas y salas puede seguir usando upsert si lo deseas,
  // ya que no generan contenido.
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

          // Buscamos si el evento ya existe por su 'id' (el slug único)
          const existingEvent = await eventsCollection.findOne({ id: eventData.id });

          if (existingEvent) {
              // Si ya existe, simplemente lo actualizamos (UPDATE)
              await eventsCollection.updateOne(
                  { id: eventData.id },
                  { $set: eventData }
              );
              summary.eventos.updated++;
          } else {
              // SI ES NUEVO, lo insertamos añadiendo el sello (INSERT)
              await eventsCollection.insertOne({
                  ...eventData,
                  contentStatus: 'pending' // <-- LA ETIQUETA PARA EL GEM DE CONTENIDOS
              });
              summary.eventos.added++;
          }
      }
  }

  console.log('Lógica de ingesta completada.', summary);
  return summary;
}

module.exports = { runIngestionProcess };