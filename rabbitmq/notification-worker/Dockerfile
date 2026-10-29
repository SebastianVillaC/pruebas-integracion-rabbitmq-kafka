FROM node:20-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY src ./src
USER node
# Sin EXPOSE: el worker no ofrece ningún endpoint, solo consume de RabbitMQ.
CMD ["node", "src/index.js"]
