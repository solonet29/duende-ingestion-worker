// ======================================================================
// SCRIPT: fix_status.js
// OBJETIVO: Asignar 'contentStatus: "pending"' a todos los eventos
//           en la colección 'events' que no tengan este campo.
// ======================================================================

require('dotenv').config();
const { MongoClient } = require('mongodb');

const uri = process.env.MONGO_URI;
const dbName = 'DuendeDB';
const COLLECTION_NAME = 'events';

const client = new MongoClient(uri);

async function addPendingStatus() {
    console.log('🚀 Iniciando script para actualizar estado de eventos...');

    try {
        await client.connect();
        console.log('🔗 Conectado a MongoDB.');
        const database = client.db(dbName);
        const eventsCollection = database.collection(COLLECTION_NAME);

        // El filtro busca todos los documentos donde el campo 'contentStatus' NO exista.
        const filter = { contentStatus: { $exists: false } };

        const updateDoc = {
            $set: { contentStatus: 'pending' },
        };

        const result = await eventsCollection.updateMany(filter, updateDoc);
        console.log(`✅ Operación completada. Documentos encontrados y actualizados: ${result.modifiedCount}`);

    } catch (err) {
        console.error('💥 Ocurrió un error crítico durante la actualización:', err);
    } finally {
        await client.close();
        console.log('🚪 Conexión a MongoDB cerrada.');
    }
}

addPendingStatus();