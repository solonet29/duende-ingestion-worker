// mark-duplicates.js
require('dotenv').config();
const { MongoClient } = require('mongodb');

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

        // Paso 1: Obtenemos todos los eventos, agrupados por artista y fecha, ordenados para conservar el más antiguo
        const pipeline = [
            {
                // Ordenamos por fecha de creación (si tienes un campo como 'createdAt')
                // Si no, podemos usar el ObjectId que contiene el timestamp de creación.
                $sort: { _id: 1 } // Ordenamos por _id para mantener el más antiguo
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
                    count: { $gt: 1 } // Solo nos interesan los grupos con más de un evento
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

        // Paso 2: Iteramos sobre los grupos de duplicados y marcamos todos menos el primero
        for (const grupo of duplicadosEncontrados) {
            const [primerId, ...idsAMarcar] = grupo.duplicados;

            if (idsAMarcar.length > 0) {
                // Creamos un array de ObjectId para la actualización
                const objectIdsAMarcar = idsAMarcar.map(id => new MongoClient.ObjectId(id));

                const updateResult = await eventsCollection.updateMany(
                    { _id: { $in: objectIdsAMarcar } },
                    { $set: { isDuplicate: true } } // <-- CAMBIO CLAVE: Marcamos el campo
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