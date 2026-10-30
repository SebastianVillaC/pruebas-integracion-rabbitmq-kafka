FROM node:20-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY src ./src
USER node
# Sin EXPOSE: el consumidor no ofrece ningún endpoint, solo lee eventos de Kafka.
CMD ["node", "src/index.js"]
