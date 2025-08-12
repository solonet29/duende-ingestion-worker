// mark-duplicates.js
require('dotenv').config();
// <-- CAMBIO CLAVE: Importamos ObjectId directamente del paquete 'mongodb'
const { MongoClient, ObjectId } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI || process.env.MONGODB_URI;
const DB_NAME = "DuendeDB";

async function markDuplicates() {
    console.log("Iniciando script para marcar duplicados...");
    let client;

    if (!MONGO_URI) {
        console.error("❌ Error: La variable MONGODB_URI no está definida.");
        return;
    }

    try {
        client = new MongoClient(MONGO_URI);
        await client.connect();
        console.log("✅ Conectado con éxito a la base de datos.");
        const db = client.db(DB_NAME);
        const eventsCollection = db.collection('events');

        const pipeline = [
            {
                $sort: { _id: 1 }
            },
            {
                $group: {
                    _id: { artist: "$artist", date: "$date" },
                    duplicados: { $push: "$_id" },
                    count: { $sum: 1 }
                }
            },
            {
                $match: {
                    count: { $gt: 1 }
                }
            }
        ];

        const duplicadosEncontrados = await eventsCollection.aggregate(pipeline).toArray();

        if (duplicadosEncontrados.length === 0) {
            console.log("🎉 ¡No se encontraron duplicados! La base de datos está limpia.");
            return;
        }

        console.log(`🔎 Se han encontrado ${duplicadosEncontrados.length} grupos de eventos duplicados.`);

        let totalMarcados = 0;

        for (const grupo of duplicadosEncontrados) {
            const [primerId, ...idsAMarcar] = grupo.duplicados;

            if (idsAMarcar.length > 0) {
                // <-- CAMBIO CLAVE: Usamos 'new ObjectId(id)'
                const objectIdsAMarcar = idsAMarcar.map(id => new ObjectId(id));

                const updateResult = await eventsCollection.updateMany(
                    { _id: { $in: objectIdsAMarcar } },
                    { $set: { isDuplicate: true } }
                );
                totalMarcados += updateResult.modifiedCount;
                console.log(`📌 Se marcaron ${updateResult.modifiedCount} duplicados para el evento de ${grupo._id.artist} en la fecha ${grupo._id.date}.`);
            }
        }

        console.log("----------------------------------------");
        console.log(`✨ Proceso de marcado finalizado. Total de documentos marcados: ${totalMarcados}.`);

    } catch (error) {
        console.error("❌ Ha ocurrido un error durante el proceso de marcado:", error);
    } finally {
        if (client) {
            await client.close();
            console.log("Conexión con la base de datos cerrada.");
        }
    }
}

markDuplicates();