# Base stage for dependencies
FROM node:20-slim AS base
WORKDIR /app

# Install system dependencies required for ODBC and building native modules
RUN apt-get update && apt-get install -y \
    python3 \
    make \
    g++ \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Dependencies stage
FROM base AS deps
COPY package*.json ./
COPY prisma ./prisma/
# Install dependencies (including devDependencies for build)
RUN npm install --legacy-peer-deps

# Build stage
FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY . .
# Generate Prisma Client
RUN npx prisma generate
# Build the application
RUN npm run build

# Production stage
FROM base AS runner
ENV NODE_ENV=production
WORKDIR /app

# Copy necessary files
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/prisma ./prisma

# Expose the port
EXPOSE 3000

# Start command
CMD ["npm", "run", "start:prod"]
