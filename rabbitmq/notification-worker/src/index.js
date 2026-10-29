// notification-worker (RabbitMQ): consumidor de mensajes.
// Escucha la cola "pedidos", simula el envío de una notificación y confirma con ack manual.
// No expone HTTP ni recibe llamadas de la API.
const amqp = require('amqplib');
const os = require('os');

const SERVICE = 'notification-worker';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.RABBITMQ_QUEUE || 'pedidos';
const PREFETCH = Number(process.env.PREFETCH || 1);
const PROCESSING_MS_MIN = Number(process.env.PROCESSING_MS_MIN || 2000);
const PROCESSING_MS_MAX = Number(process.env.PROCESSING_MS_MAX || 5000);
const RETRY_MS = Number(process.env.RETRY_MS || 3000);
const WORKER_ID = os.hostname();

function log(message, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [${SERVICE}] ${message}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomBetween = (min, max) => min + Math.floor(Math.random() * (max - min + 1));

let connection = null;
let shuttingDown = false;
let processed = 0;

async function handle(channel, msg) {
  if (!msg) {
    log('El broker canceló el consumidor');
    return;
  }

  let data;
  try {
    data = JSON.parse(msg.content.toString());
  } catch (err) {
    log('Mensaje con formato inválido, se descarta', { deliveryTag: msg.fields.deliveryTag, error: err.message });
    channel.nack(msg, false, false);
    return;
  }

  const fields = {
    orderId: data.orderId,
    correlationId: data.correlationId || msg.properties.correlationId,
    worker: WORKER_ID,
    deliveryTag: msg.fields.deliveryTag,
    redelivered: msg.fields.redelivered,
  };
  log('Mensaje recibido', { ...fields, type: data.type });
  console.log(JSON.stringify(data));

  const processingMs = randomBetween(PROCESSING_MS_MIN, PROCESSING_MS_MAX);
  log(`Simulando envío de notificación a ${data.customerName} (${data.quantity} x ${data.product})...`, {
    orderId: data.orderId,
    processingMs,
  });
  await sleep(processingMs);

  try {
    channel.ack(msg);
    processed++;
    log('Notificación enviada, mensaje confirmado (ack)', { ...fields, totalProcesados: processed });
  } catch (err) {
    // Si la conexión se perdió durante el procesamiento, el broker reencolará el mensaje.
    log('No se pudo confirmar el mensaje (quedará para reentrega)', { ...fields, error: err.message });
  }
}

async function start() {
  for (let attempt = 1; ; attempt++) {
    try {
      connection = await amqp.connect(RABBITMQ_URL);
      connection.on('error', (err) => log('Error en la conexión AMQP', { error: err.message }));
      connection.on('close', () => {
        if (shuttingDown) return;
        log('Conexión con RabbitMQ cerrada, reintentando...');
        setTimeout(start, RETRY_MS);
      });

      const channel = await connection.createChannel();
      await channel.assertQueue(QUEUE, { durable: true });
      // prefetch(1): cada worker recibe un mensaje a la vez, así la carga se reparte entre workers.
      await channel.prefetch(PREFETCH);
      log('Conectado a RabbitMQ, escuchando cola', { queue: QUEUE, prefetch: PREFETCH, worker: WORKER_ID });
      await channel.consume(QUEUE, (msg) => handle(channel, msg), { noAck: false });
      return;
    } catch (err) {
      log('RabbitMQ no disponible, reintentando', { attempt, error: err.message, retryInMs: RETRY_MS });
      await sleep(RETRY_MS);
    }
  }
}

async function shutdown(signal) {
  shuttingDown = true;
  log(`${signal} recibido, cerrando worker (los mensajes sin ack vuelven a la cola)`, { worker: WORKER_ID });
  try {
    if (connection) await connection.close();
  } catch (err) {
    // la conexión ya estaba cerrada
  }
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

log('Iniciando worker', { worker: WORKER_ID });
start();
