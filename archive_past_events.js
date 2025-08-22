// ======================================================================
// SCRIPT: archive_past_events.js
// OBJETIVO: Encontrar eventos pendientes ('pending') cuya fecha de inicio
//           sea anterior a hoy y cambiar su estado a 'archived'.
// ======================================================================

require('dotenv').config();
const { MongoClient } = require('mongodb');

const uri = process.env.MONGO_URI;
const dbName = 'DuendeDB';
const COLLECTION_NAME = 'events';

const client = new MongoClient(uri);

async function archivePastPendingEvents() {
    console.log('🚀 Iniciando script para archivar eventos pasados...');

    try {
        await client.connect();
        console.log('🔗 Conectado a MongoDB.');
        const database = client.db(dbName);
        const eventsCollection = database.collection(COLLECTION_NAME);

        // Obtenemos la fecha de hoy, pero a medianoche (para comparar solo el día)
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        // El filtro busca eventos pendientes Y cuya fecha de inicio sea menor que hoy.
        // NOTA: Asumo que la fecha está en un campo llamado 'date' y es un string
        // en formato ISO ("YYYY-MM-DD"). Si tu campo se llama diferente, ajústalo.
        const filter = {
            contentStatus: 'pending',
            date: { $lt: today.toISOString().split('T')[0] } // Compara solo la parte de la fecha YYYY-MM-DD
        };

        const updateDoc = {
            $set: { contentStatus: 'archived' }, // Un nuevo estado para eventos pasados
        };

        const result = await eventsCollection.updateMany(filter, updateDoc);
        console.log(`✅ Operación completada. Eventos pasados archivados: ${result.modifiedCount}`);

    } catch (err) {
        console.error('💥 Ocurrió un error crítico durante el archivado:', err);
    } finally {
        await client.close();
        console.log('🚪 Conexión a MongoDB cerrada.');
    }
}

archivePastPendingEvents();