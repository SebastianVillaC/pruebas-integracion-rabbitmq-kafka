// orders-api (Kafka): productor de eventos.
// Recibe pedidos por HTTP y publica el evento de dominio OrderCreated en el tópico orders.events.
// No conoce a los consumidores (inventory, billing, notification) ni los llama.
const express = require('express');
const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { randomUUID, randomBytes } = require('crypto');

const SERVICE = 'orders-api';
const PORT = Number(process.env.PORT || 3000);
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const TOPIC_PARTITIONS = Number(process.env.KAFKA_TOPIC_PARTITIONS || 3);
const RETRY_MS = Number(process.env.RETRY_MS || 3000);

function log(message, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [${SERVICE}] ${message}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const kafka = new Kafka({
  clientId: SERVICE,
  brokers: [BROKER],
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 300, retries: 8 },
});
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
let ready = false;

// Crea el tópico con varias particiones (necesario para repartir carga entre instancias
// de un mismo consumer group). Si ya existe, no hace nada.
async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: TOPIC, numPartitions: TOPIC_PARTITIONS, replicationFactor: 1 }],
    });
    const [metadata] = (await admin.fetchTopicMetadata({ topics: [TOPIC] })).topics;
    log(created ? 'Tópico creado' : 'Tópico ya existente', { topic: TOPIC, partitions: metadata.partitions.length });
  } finally {
    await admin.disconnect();
  }
}

// Conexión con reintentos: depends_on no garantiza que Kafka esté listo para recibir eventos.
async function connect() {
  for (let attempt = 1; ; attempt++) {
    try {
      await ensureTopic();
      await producer.connect();
      ready = true;
      log('Productor conectado a Kafka', { broker: BROKER, topic: TOPIC });
      return;
    } catch (err) {
      log('Kafka no disponible, reintentando', { attempt, error: err.message, retryInMs: RETRY_MS });
      await sleep(RETRY_MS);
    }
  }
}

function validate(body) {
  const errors = [];
  if (!body || typeof body !== 'object') return ['El cuerpo debe ser un JSON'];
  if (typeof body.customerName !== 'string' || !body.customerName.trim()) errors.push('customerName es obligatorio');
  if (typeof body.product !== 'string' || !body.product.trim()) errors.push('product es obligatorio');
  if (!Number.isInteger(body.quantity) || body.quantity <= 0) errors.push('quantity debe ser un entero mayor que 0');
  if (typeof body.unitPrice !== 'number' || body.unitPrice < 0) errors.push('unitPrice debe ser un número mayor o igual a 0');
  return errors;
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.status(ready ? 200 : 503).json({ service: SERVICE, kafka: ready ? 'connected' : 'disconnected' });
});

app.post('/orders', async (req, res) => {
  const start = Date.now();
  const errors = validate(req.body);
  if (errors.length) {
    log('Pedido rechazado por datos inválidos', { errors: errors.join('; ') });
    return res.status(400).json({ status: 'rejected', errors });
  }
  if (!ready) {
    log('Pedido no aceptado: productor aún no conectado a Kafka');
    return res.status(503).json({ status: 'unavailable', message: 'Kafka no disponible, intente de nuevo' });
  }

  const orderId = `ORD-${Date.now()}-${randomBytes(2).toString('hex')}`;
  const correlationId = req.get('X-Correlation-Id') || randomUUID();
  const { customerName, product, quantity, unitPrice } = req.body;

  // Evento de dominio: un hecho que ya ocurrió (nombre en pasado), no un comando.
  const event = {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: SERVICE,
    correlationId,
    data: {
      orderId,
      customerName: customerName.trim(),
      product: product.trim(),
      quantity,
      unitPrice,
      total: quantity * unitPrice,
    },
  };

  let metadata;
  try {
    [metadata] = await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: orderId, // mismo pedido -> misma partición
          value: JSON.stringify(event),
          headers: { eventType: event.eventType, correlationId, eventId: event.eventId },
        },
      ],
    });
  } catch (err) {
    log('Error publicando evento en Kafka', { orderId, correlationId, error: err.message });
    return res.status(503).json({ status: 'unavailable', message: 'No fue posible publicar el evento' });
  }

  // Se responde de inmediato: los consumidores reaccionarán al evento por su cuenta.
  const responseTimeMs = Date.now() - start;
  log('Evento OrderCreated publicado', {
    orderId,
    eventId: event.eventId,
    correlationId,
    topic: TOPIC,
    partition: metadata.partition,
    offset: metadata.baseOffset,
    responseTimeMs,
  });
  res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
    orderId,
    correlationId,
  });
});

// JSON mal formado
app.use((err, req, res, next) => {
  log('Petición con JSON inválido', { error: err.message });
  res.status(400).json({ status: 'rejected', errors: ['JSON inválido'] });
});

app.listen(PORT, () => log('API escuchando', { port: PORT, endpoint: 'POST /orders' }));
connect();

process.on('SIGTERM', async () => {
  log('SIGTERM recibido, cerrando API');
  try {
    await producer.disconnect();
  } catch (err) {
    // ya desconectado
  }
  process.exit(0);
});
