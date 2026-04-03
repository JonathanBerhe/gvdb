FROM node:22-alpine AS frontend
WORKDIR /app
COPY web/package.json web/yarn.lock ./
RUN yarn install --frozen-lockfile
COPY web/ .
RUN yarn build

FROM golang:1.25-alpine AS backend
WORKDIR /app
COPY gateway/ .
COPY --from=frontend /app/dist/ static/
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o gvdb-ui .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=backend /app/gvdb-ui /usr/local/bin/gvdb-ui
EXPOSE 8080
ENTRYPOINT ["gvdb-ui"]
