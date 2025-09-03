// api/index.js
// Este archivo es el punto de entrada para el Cron Job de Vercel.

// Importamos la lógica de ingesta real desde el script consolidado.
const { processEvents } = require('../ingesta.js');

// Vercel espera una función handler que tome (request, response).
module.exports = async (request, response) => {
  try {
    console.log('CRON JOB: Iniciando la ejecución de processEvents...');
    // Ejecutamos la función principal de ingesta y esperamos a que termine.
    await processEvents();
    console.log('CRON JOB: La ejecución de processEvents ha finalizado con éxito.');
    // Enviamos una respuesta exitosa.
    response.status(200).send('Ingestión completada con éxito.');
  } catch (error) {
    // Si algo sale mal dentro de processEvents, lo capturamos aquí.
    console.error('CRON JOB: Ha ocurrido un error fatal durante la ejecución.', error);
    // Enviamos una respuesta de error para que Vercel sepa que el job falló.
    response.status(500).send('Error en el proceso de ingestión.');
  }
};
