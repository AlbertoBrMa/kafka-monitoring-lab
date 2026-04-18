# Kafka Monitoring Lab

Pipeline de monitoreo en tiempo real usando Apache Kafka, Python y MongoDB Atlas.

## Arquitectura

```
[Productor] ──► [Kafka: system-metrics-topic] ──► [Consumidor] ──► MongoDB Atlas
  ├── system_metrics_raw
  └── system_metrics_kpis
```

Cinco servidores simulados (`web01`, `web02`, `db01`, `app01`, `cache01`) emiten métricas cada segundo. El consumidor guarda cada mensaje en crudo y calcula KPIs agregados por ventana de 20 mensajes.

## Requisitos

- Docker y Docker Compose
- Cuenta de MongoDB Atlas con un cluster y su cadena de conexión

## Levantar el entorno

### 1. Configurar variables de entorno

Edita el archivo `.env` y rellena `MONGODB_CONNECTION_STRING` con tu URI de MongoDB Atlas.

### 2. Arrancar todos los servicios

```bash
docker compose -f docker/docker-compose.yml up -d --build
```

Esto levanta:
- **Zookeeper** — coordinación del clúster Kafka
- **Kafka** — broker de mensajes
- **Productor** — genera métricas de los servidores simulados
- **Consumidor** — lee métricas y las persiste en MongoDB

### 3. Ver los logs

```bash
# Ver logs del productor
docker compose -f docker/docker-compose.yml logs -f producer

# Ver logs del consumidor
docker compose -f docker/docker-compose.yml logs -f consumer
```

### 4. Parar el entorno

```bash
docker compose -f docker/docker-compose.yml down
```
## Formato de los mensajes

Cada mensaje que produce el productor tiene esta estructura:

```json
{
  "server_id": "web01",
  "timestamp_utc": "2026-04-17T10:00:00.000000+00:00",
  "metrics": {
    "cpu_percent": 42.5,
    "memory_percent": 67.3,
    "disk_io_mbps": 12.8,
    "network_mbps": 5.1,
    "error_count": 2
  },
  "message_uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Estructura del proyecto

```
kafka-monitoring-lab/
├── docker/
│   └── docker-compose.yml
├── producer/
│   └── productor_metrics.py
├── consumer/
│   └── consumidor_metrics.py
├── docs/
│   └──screenshots
│   └── evidencias.md
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```
