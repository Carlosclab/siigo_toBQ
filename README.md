# Siigo to BigQuery ETL Pipeline

Pipeline ETL para extracción de datos desde API Siigo hacia BigQuery, implementado como Google Cloud Functions con rate limiting y manejo de paginación.

## Arquitectura

Dos Cloud Functions independientes con el mismo código base:
- **CF1 (env1)**: Extrae `invoices`, `products`, `credit-notes`, `customers`, `vouchers`, `journals` - Ejecuta 8x/día
- **CF2 (env2)**: Extrae `purchases` - Ejecuta 1x/día (limitación: endpoint no filtra por fecha correctamente)

### Rate Limiting

- **Token Bucket Algorithm**: 95 req/min máximo
- **Pausas preventivas**: Cada 20 requests, pausa de 30s
- **Retry con backoff**: Manejo de HTTP 429

### Features

- ✅ Extracción incremental por fecha (`created_start`, `updated_start`)
- ✅ Paginación automática
- ✅ Procesamiento especial de vouchers → facturas individuales
- ✅ Schema evolution en BigQuery (`ALLOW_FIELD_ADDITION`)
- ✅ Multi-source (2 credenciales simultáneas)
- ✅ Rate limiting global compartido

## Deployment

### Pre-requisitos

- `gcloud` CLI configurado
- Permisos IAM:
  - `cloudfunctions.admin`
  - `iam.serviceAccountUser`
  - `bigquery.admin`

### Variables Sensibles

Crear secrets en Google Secret Manager:
```bash
echo -n "gerencia@tirecenter.com.co" | gcloud secrets create SIIGO_USER1 --data-file=-
echo -n "MGEyM2QyMDEtNjBiMy00ZTUwLTlkYzMtMjI4ZjVlNDlhNGYyOlg5Xzg4PEtxUVM=" | gcloud secrets create SIIGO_KEY1 --data-file=-
echo -n "gerencia@tirecenter.com.co" | gcloud secrets create SIIGO_USER2 --data-file=-
echo -n "MmJlMDIwYzEtOGY3Yy00YTdjLWJhOTctZWRjMTQxODQwMGE5OjJLcyl3QEQtVUs=" | gcloud secrets create SIIGO_KEY2 --data-file=-
```

### Deploy Functions
```bash
# CF1 - Endpoints principales
./deploy/deploy-env1.sh

# CF2 - Purchases
./deploy/deploy-env2.sh
```

### Schedules (Cloud Scheduler)
```bash
# CF1: Cada 3 horas (8x/día)
gcloud scheduler jobs create http siigo-etl-env1 \
  --schedule="0 */3 * * *" \
  --uri="https://REGION-PROJECT_ID.cloudfunctions.net/siigo-etl-env1" \
  --http-method=POST \
  --time-zone="America/Bogota"

# CF2: Diario a las 02:00
gcloud scheduler jobs create http siigo-etl-env2 \
  --schedule="0 2 * * *" \
  --uri="https://REGION-PROJECT_ID.cloudfunctions.net/siigo-etl-env2" \
  --http-method=POST \
  --time-zone="America/Bogota"
```

## Variables de Entorno

### ENV1 (Principal)
```yaml
BQ_PROJECT: powerbi-445616
BQ_TABLES: invoices,products,credit-notes,customers,vouchers,journals
NUM_DAYS: '1'
RATE_LIMIT_PER_MINUTE: '95'
```

### ENV2 (Purchases)
```yaml
BQ_PROJECT: powerbi-445616
BQ_TABLES: purchases
NUM_DAYS: '1'
```

**Ver `config/env*.yaml` para configuración completa**

## Monitoreo
```bash
# Logs
gcloud functions logs read siigo-etl-env1 --limit=50

# Metrics
gcloud monitoring dashboards list
```

## Limitaciones Conocidas

1. **Purchases endpoint**: No filtra por fecha → trae todos los registros
2. **API Siigo**: 100 req/min (configurado conservador a 95)
3. **Timeout CF**: 540s máximo (9 min)

## Troubleshooting

### HTTP 429
Rate limiter debe manejar automáticamente. Si persiste:
- Reducir `RATE_LIMIT_PER_MINUTE`
- Aumentar `BATCH_PAUSE_DURATION`

### Timeout
Reducir `NUM_DAYS` o aumentar memoria CF (actualmente 256MB default)

### Schema Mismatch
BigQuery acepta campos nuevos automáticamente (`ALLOW_FIELD_ADDITION`)

## Development
```bash
# Local testing (requiere service account)
export GOOGLE_APPLICATION_CREDENTIALS="path/to/sa.json"
python main.py

# Unit tests
pytest tests/
```

## License

Proprietary - Groupit
