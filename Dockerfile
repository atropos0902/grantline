FROM node:24.13.0-alpine3.23 AS console
WORKDIR /w
COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY console/ ./
RUN npm run build

FROM golang:1.26.2-alpine3.23 AS build
WORKDIR /w
COPY go.mod go.sum ./
COPY operator/go.mod operator/go.sum operator/
RUN go mod download && cd operator && go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/grantline ./cmd/loadgen \
 && cd operator && go build -trimpath -ldflags="-s -w" -o /out/operator .

FROM alpine:3.23.3 AS grantline
RUN adduser -D -u 65532 grantline && mkdir /spill && chown grantline /spill
COPY --from=build /out/ /usr/local/bin/
COPY --from=console /w/dist /console
USER 65532
ENTRYPOINT ["/usr/local/bin/grantline"]
