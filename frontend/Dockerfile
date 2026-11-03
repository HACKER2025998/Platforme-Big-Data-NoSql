FROM node:24.11.0-alpine AS build
WORKDIR /app
COPY package*.json ./
ARG NPM_REGISTRY=https://registry.npmjs.org/
RUN npm ci --registry="${NPM_REGISTRY}"
COPY . .
RUN npm run build
FROM nginx:1.28-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
