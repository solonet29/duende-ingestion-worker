// Importamos el driver de MongoDB
const { MongoClient } = require('mongodb');
// Importamos las herramientas para leer archivos del sistema
const fs = require('fs');
const path = require('path');

// --- ZONA DE CONFIGURACIÓN ---
// ¡ACCIÓN REQUERIDA! Pega aquí tu Connection String de MongoDB Atlas.
// Reemplaza la parte de <password> con tu contraseña real.
const MONGO_URI="mongodb+srv://hola:Ang2lyl4l125@duende-cluster.fafouol.mongodb.net/?retryWrites=true&w=majority&appName=duende-cluster";

// Nombre de tu base de datos y de la colección (tabla) donde guardas los eventos.
// Asegúrate de que coincidan con los de tu base de datos.
const DB_NAME = "AFLandDB"; // <-- Cambia esto si tu base de datos se llama diferente
const COLLECTION_NAME = "eventos"; // <-- Cambia esto si tu colección se llama diferente

// Ruta al archivo JSON con los nuevos eventos que te proporciono.
const JSON_FILE_PATH = path.join(__dirname, 'nuevos_eventos.json');
// --- FIN DE LA ZONA DE CONFIGURACIÓN ---


// Función principal que ejecutará todo el proceso
async function main() {
  // Creamos un nuevo cliente de MongoDB
  const client = new MongoClient(MONGO_URI);
  console.log("Iniciando script de ingesta...");

  try {
    // 1. Conectar al servidor de MongoDB
    await client.connect();
    console.log("✅ Conectado con éxito a la base de datos.");

    const database = client.db(DB_NAME);
    const collection = database.collection(COLLECTION_NAME);

    // 2. Leer y procesar el archivo JSON local
    console.log(`Leyendo el archivo de nuevos eventos desde: ${JSON_FILE_PATH}`);
    const nuevosDatos = JSON.parse(fs.readFileSync(JSON_FILE_PATH, 'utf-8'));
    
    // Nos aseguramos de que el JSON tiene una sección de "eventos"
    if (!nuevosDatos.eventos || nuevosDatos.eventos.length === 0) {
      console.log("El archivo JSON no contiene eventos nuevos. Finalizando.");
      return;
    }

    console.log(`Se han encontrado ${nuevosDatos.eventos.length} eventos en el archivo.`);
    let nuevosEventosAnadidos = 0;

    // 3. Recorrer cada evento y realizar la operación "upsert"
    for (const evento of nuevosDatos.eventos) {
      // El "filtro" busca un documento que coincida con este id_evento
      const filter = { id_evento: evento.id_evento };
      
      // Los "datosAInsertar" son todo el objeto del evento
      const updateDoc = {
        $set: evento,
      };

      // La opción "upsert: true" es la que crea el documento si no existe
      const options = { upsert: true };

      const result = await collection.updateOne(filter, updateDoc, options);

      // Si el evento fue insertado (upserted), contamos uno más.
      if (result.upsertedCount > 0) {
        console.log(`   -> Evento nuevo añadido: ${evento.titulo}`);
        nuevosEventosAnadidos++;
      }
    }
    
    console.log("----------------------------------------");
    console.log("📊 Proceso de ingesta completado.");
    console.log(`Total de eventos nuevos añadidos a la base de datos: ${nuevosEventosAnadidos}`);
    

  } catch (error) {
    console.error("❌ Ha ocurrido un error durante el proceso:", error);
  } finally {
    // 4. Asegurarse de que el cliente se cierra cuando acabemos o si hay un error
    await client.close();
    console.log("Conexión con la base de datos cerrada.");
  }
}

// Ejecutamos la función principal
main();