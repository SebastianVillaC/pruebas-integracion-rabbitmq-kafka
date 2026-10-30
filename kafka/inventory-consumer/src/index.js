// inventory-consumer (Kafka): reacciona a OrderCreated simulando la reserva de inventario.
// Grupo propio (inventory-service-group): recibe todos los eventos aunque existan otros consumidores.
const { Kafka, logLevel } = require('kafkajs');
const os = require('os');

const SERVICE = 'inventory-consumer';
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'inventory-service-group';
const PROCESSING_MS_MIN = Number(process.env.PROCESSING_MS_MIN || 2000);
const PROCESSING_MS_MAX = Number(process.env.PROCESSING_MS_MAX || 5000);
const RETRY_MS = Number(process.env.RETRY_MS || 3000);
const INSTANCE = os.hostname();

function log(message, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [${SERVICE}] ${message}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomBetween = (min, max) => min + Math.floor(Math.random() * (max - min + 1));

const kafka = new Kafka({
  clientId: SERVICE,
  brokers: [BROKER],
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 300, retries: 8 },
});
const consumer = kafka.consumer({ groupId: GROUP_ID, sessionTimeout: 10000, heartbeatInterval: 3000 });

// Acción de negocio simulada
async function reserveStock(order) {
  log(`Reservando ${order.quantity} unidad(es) de "${order.product}"...`, { orderId: order.orderId });
  await sleep(randomBetween(PROCESSING_MS_MIN, PROCESSING_MS_MAX));
  log('Inventario reservado', { orderId: order.orderId, product: order.product, quantity: order.quantity });
}

async function handleMessage({ topic, partition, message, heartbeat }) {
  let event;
  try {
    event = JSON.parse(message.value.toString());
  } catch (err) {
    log('Evento con formato inválido, se ignora', { partition, offset: message.offset, error: err.message });
    return;
  }
  if (event.eventType !== 'OrderCreated') return;

  const ctx = {
    orderId: event.data.orderId,
    eventId: event.eventId,
    correlationId: event.correlationId,
    topic,
    partition,
    offset: message.offset,
    group: GROUP_ID,
    instance: INSTANCE,
  };
  log('Evento recibido: OrderCreated', ctx);
  console.log(JSON.stringify(event));

  await reserveStock(event.data);
  await heartbeat();
  // Al terminar eachMessage, kafkajs confirma (commit) el offset: si el proceso muere antes,
  // el evento se vuelve a entregar al reiniciar.
  log('Evento procesado, offset confirmado', { ...ctx, nextOffset: Number(message.offset) + 1 });
}

async function start() {
  for (let attempt = 1; ; attempt++) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      await consumer.run({ autoCommitThreshold: 1, eachMessage: handleMessage });
      log('Consumidor suscrito', { broker: BROKER, topic: TOPIC, group: GROUP_ID, instance: INSTANCE });
      return;
    } catch (err) {
      log('Kafka no disponible, reintentando', { attempt, error: err.message, retryInMs: RETRY_MS });
      try {
        await consumer.disconnect();
      } catch (e) {
        // sin conexión previa
      }
      await sleep(RETRY_MS);
    }
  }
}

consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  const assigned = Object.entries(payload.memberAssignment)
    .map(([t, partitions]) => `${t}[${partitions.join(',')}]`)
    .join(' ');
  log('Unido al consumer group', { group: payload.groupId, instance: INSTANCE, particionesAsignadas: assigned || 'ninguna' });
});
consumer.on(consumer.events.CRASH, ({ payload }) => {
  log('El consumidor falló, kafkajs reintentará', { error: payload.error.message, restart: payload.restart });
});

async function shutdown(signal) {
  log(`${signal} recibido, saliendo del grupo`, { instance: INSTANCE });
  try {
    await consumer.disconnect();
  } catch (err) {
    // ya desconectado
  }
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

log('Iniciando consumidor', { instance: INSTANCE, group: GROUP_ID });
start();
