// orders-api (RabbitMQ): productor de mensajes.
// Recibe pedidos por HTTP y publica un mensaje "ProcessOrder" en la cola "pedidos".
// No conoce ni llama al worker: la única comunicación con él es a través de RabbitMQ.
const express = require('express');
const amqp = require('amqplib');
const { randomUUID, randomBytes } = require('crypto');

const SERVICE = 'orders-api';
const PORT = Number(process.env.PORT || 3000);
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.RABBITMQ_QUEUE || 'pedidos';
const RETRY_MS = Number(process.env.RETRY_MS || 3000);

function log(message, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [${SERVICE}] ${message}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let channel = null;

// Conexión con reintentos: depends_on no garantiza que RabbitMQ esté listo,
// y si el broker se reinicia la API debe volver a conectarse sola.
async function connect() {
  for (let attempt = 1; ; attempt++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      connection.on('error', (err) => log('Error en la conexión AMQP', { error: err.message }));
      connection.on('close', () => {
        channel = null;
        log('Conexión con RabbitMQ cerrada, reintentando...');
        setTimeout(connect, RETRY_MS);
      });
      const ch = await connection.createConfirmChannel();
      await ch.assertQueue(QUEUE, { durable: true });
      channel = ch;
      log('Conectado a RabbitMQ', { url: RABBITMQ_URL.replace(/\/\/.*@/, '//***@'), queue: QUEUE });
      return;
    } catch (err) {
      log('RabbitMQ no disponible, reintentando', { attempt, error: err.message, retryInMs: RETRY_MS });
      await sleep(RETRY_MS);
    }
  }
}

function publish(message) {
  // Canal con confirmación: el callback se ejecuta cuando el broker aceptó el mensaje
  // (no cuando el worker lo procesa).
  return new Promise((resolve, reject) => {
    channel.sendToQueue(
      QUEUE,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: 'application/json',
        messageId: message.orderId,
        correlationId: message.correlationId,
        type: message.type,
        timestamp: Math.floor(Date.now() / 1000),
        appId: SERVICE,
      },
      (err) => (err ? reject(err) : resolve())
    );
  });
}

function validate(body) {
  const errors = [];
  if (!body || typeof body !== 'object') return ['El cuerpo debe ser un JSON'];
  if (typeof body.customerName !== 'string' || !body.customerName.trim()) errors.push('customerName es obligatorio');
  if (typeof body.product !== 'string' || !body.product.trim()) errors.push('product es obligatorio');
  if (!Number.isInteger(body.quantity) || body.quantity <= 0) errors.push('quantity debe ser un entero mayor que 0');
  return errors;
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.status(channel ? 200 : 503).json({ service: SERVICE, rabbitmq: channel ? 'connected' : 'disconnected' });
});

app.post('/orders', async (req, res) => {
  const start = Date.now();
  const errors = validate(req.body);
  if (errors.length) {
    log('Pedido rechazado por datos inválidos', { errors: errors.join('; ') });
    return res.status(400).json({ status: 'rejected', errors });
  }
  if (!channel) {
    log('Pedido no aceptado: sin conexión con RabbitMQ');
    return res.status(503).json({ status: 'unavailable', message: 'RabbitMQ no disponible, intente de nuevo' });
  }

  const orderId = `ORD-${Date.now()}-${randomBytes(2).toString('hex')}`;
  const correlationId = req.get('X-Correlation-Id') || randomUUID();
  const message = {
    type: 'ProcessOrder',
    orderId,
    correlationId,
    customerName: req.body.customerName.trim(),
    product: req.body.product.trim(),
    quantity: req.body.quantity,
    createdAt: new Date().toISOString(),
  };

  try {
    await publish(message);
  } catch (err) {
    log('Error publicando en RabbitMQ', { orderId, correlationId, error: err.message });
    return res.status(503).json({ status: 'unavailable', message: 'No fue posible publicar el pedido' });
  }

  // Se responde de inmediato: el worker procesará el mensaje después, por su cuenta.
  const responseTimeMs = Date.now() - start;
  log('Pedido recibido y publicado en RabbitMQ', { orderId, correlationId, queue: QUEUE, responseTimeMs });
  res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y enviado a RabbitMQ',
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

process.on('SIGTERM', () => {
  log('SIGTERM recibido, cerrando API');
  process.exit(0);
});
