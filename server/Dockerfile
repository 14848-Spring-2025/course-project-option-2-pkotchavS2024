# backend/Dockerfile
FROM node:18-alpine

WORKDIR /usr/src/app

# Copy package info first so we can do a more efficient build
COPY package*.json ./

# Install dependencies
RUN npm install

# Now copy the rest of the code
COPY . .

EXPOSE 3000

CMD ["node", "server.js"]
