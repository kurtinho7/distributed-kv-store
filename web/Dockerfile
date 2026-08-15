FROM node:24-alpine

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY index.html tsconfig.json vite.config.ts ./
COPY src ./src
EXPOSE 5173
CMD ["npm", "run", "dev"]
