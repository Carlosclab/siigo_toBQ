import os
import time
import datetime
from dateutil.relativedelta import relativedelta
from threading import Lock

import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaUpdateOption


class RateLimiter:
    """
    Rate Limiter usando algoritmo Token Bucket para controlar velocidad de requests.
    Garantiza no exceder el límite de requests por minuto de la API.
    """
    def __init__(self, max_requests=95, time_window=60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # Limpia requests que ya salieron de la ventana de tiempo
            self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
            
            if len(self.requests) >= self.max_requests:
                # Calcula cuánto tiempo debe esperar
                sleep_time = self.time_window - (now - self.requests[0])
                if sleep_time > 0:
                    print(f"Rate limit preventivo: esperando {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    # Limpia lista después de esperar
                    self.requests = [req_time for req_time in self.requests if time.time() - req_time < self.time_window]
            
            self.requests.append(now)

    def get_current_usage(self):
        """Retorna el número actual de requests en la ventana de tiempo"""
        with self.lock:
            now = time.time()
            self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
            return len(self.requests)


# Instancia global del rate limiter
rate_limiter = None


def request_with_retries(method, url, headers=None, params=None, json_data=None, max_retries=3, timeout=60):
    """
    Ejecuta una petición HTTP con reintentos y control de rate limiting.
    Incluye manejo específico para errores 429 (Too Many Requests).
    """
    global rate_limiter
    
    attempts = 0
    while attempts < max_retries:
        attempts += 1
        
        # Aplicar rate limiting antes de cada request
        if rate_limiter:
            rate_limiter.wait_if_needed()
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params, timeout=timeout)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=json_data, timeout=timeout)
            else:
                raise ValueError(f"Método HTTP '{method}' no soportado.")
            
            response.raise_for_status()
            
            # Verificar headers de rate limiting si existen
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = int(response.headers['X-RateLimit-Remaining'])
                if remaining < 10:  # Si quedan pocas requests disponibles
                    print(f"Advertencia: Solo quedan {remaining} requests disponibles. Pausa preventiva...")
                    time.sleep(2)
            
            return response

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                # Manejo específico para Too Many Requests
                retry_after = int(e.response.headers.get('Retry-After', 60))
                print(f"Rate limit 429 detectado. Esperando {retry_after}s antes del siguiente intento...")
                time.sleep(retry_after)
                continue
            
            print(f"ERROR: Falla en petición HTTP (Intento {attempts}/{max_retries}): {e}")
            if e.response is not None:
                print(f"ERROR DETAILS: Status={e.response.status_code}, Body={e.response.text}")
            if attempts == max_retries:
                raise
            time.sleep(5)
            
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Falla de red (Intento {attempts}/{max_retries}): {e}")
            if attempts == max_retries:
                raise
            time.sleep(5)


def limpiar_valores_nulos_en_arreglos(x):
    """Limpia valores nulos en arrays, reemplazándolos por strings vacíos."""
    if isinstance(x, list):
        return [elem if elem is not None else "" for elem in x]
    return x


def obtener_token(config):
    """Obtiene token de autenticación de la API."""
    url_auth = config["url_auth"]
    user = config["user"]
    key = config["key"]
    headers = {"Content-Type": "application/json", "Partner-Id": "Powerbi"}
    body = {"username": user, "access_key": key}
    
    print(f"[{config['id']}] Obteniendo token de autenticación...")
    response = request_with_retries(method="POST", url=url_auth, headers=headers, json_data=body)
    token = response.json().get('access_token')
    if not token:
        raise ValueError(f"[{config['id']}] No se pudo obtener el token.")
    print(f"[{config['id']}] Token obtenido exitosamente.")
    return token


def cargar_a_bigquery(df, full_table_id, config):
    """Carga DataFrame a BigQuery con configuración de esquema flexible."""
    project_id = config["bq_project"]
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    print(f"[{config['id']}] Cargando {len(df)} registros a la tabla {full_table_id}...")
    try:
        load_job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        load_job.result(timeout=300)
    except Exception as e:
        print(f"ERROR: Fallo la carga a BigQuery para {full_table_id}: {e}")
        raise
    print(f"[{config['id']}] Carga a {full_table_id} completada exitosamente.")


def obtener_y_cargar_datos_por_endpoint(token, endpoint, fecha_inicio, query_param_name, config):
    """
    Obtiene datos de un endpoint con paginación y los carga a BigQuery.
    Incluye lógica especial para vouchers que extrae y consulta facturas individuales.
    Incluye pausas preventivas para distribuir la carga de requests.
    """
    global rate_limiter
    
    full_table_id = f"{config['bq_project']}.{config['bq_dataset']}.{endpoint}"
    final_url = f"{config['base_url'].strip('/')}/{endpoint.strip('/')}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Partner-Id": "Powerbi"
    }
    params = {query_param_name: fecha_inicio}
    
    print(f"--- Procesando Endpoint: {endpoint} ({query_param_name}) ---")
    all_data = []
    current_url = final_url
    is_first_request = True
    processed_requests = 0
    
    # Configuración de pausas preventivas
    batch_pause_interval = int(os.getenv("BATCH_PAUSE_INTERVAL", "20"))
    batch_pause_duration = int(os.getenv("BATCH_PAUSE_DURATION", "30"))
    
    while current_url:
        try:
            # Pausa preventiva cada cierto número de requests
            if processed_requests > 0 and processed_requests % batch_pause_interval == 0:
                current_usage = rate_limiter.get_current_usage() if rate_limiter else 0
                print(f"Pausa preventiva después de {processed_requests} requests. Rate limit actual: {current_usage}")
                time.sleep(batch_pause_duration)
            
            current_params = params if is_first_request else None
            response = request_with_retries("GET", current_url, headers=headers, params=current_params, timeout=120)
            json_response = response.json()
            is_first_request = False
            processed_requests += 1
            
            data = json_response.get('results', []) if isinstance(json_response, dict) else json_response
            next_url = json_response.get('_links', {}).get('next', {}).get('href') if isinstance(json_response, dict) else None

            if data:
                all_data.extend(data)
                print(f"Obtenidos {len(data)} registros. Total acumulado: {len(all_data)}")
            
            current_url = next_url
            
        except Exception as e:
            print(f"ERROR: Falla en paginación para {endpoint}: {e}. Se detiene la captura para este endpoint.")
            break
            
    if not all_data:
        print(f"[{config['id']}] No se encontraron datos para {endpoint}.")
        return all_data

    print(f"[{config['id']}] Total de registros obtenidos para {endpoint}: {len(all_data)}")

    try:
        df = pd.json_normalize(all_data)
        df.columns = [col.replace('.', '_') for col in df.columns]
        df = df.applymap(limpiar_valores_nulos_en_arreglos)
        cargar_a_bigquery(df, full_table_id, config=config)
        
        # Lógica especial para vouchers: extraer y consultar facturas individuales
        if endpoint == "vouchers" and query_param_name == "created_start":
            procesar_facturas_desde_vouchers(token, df, config)
        
    except Exception as e:
        print(f"ERROR: Falla en normalización o carga de datos para {endpoint}: {e}")
        raise
    
    return all_data


def procesar_facturas_desde_vouchers(token, vouchers_df, config):
    """
    Extrae nombres de facturas desde vouchers y las consulta individualmente.
    Aplicando rate limiting a cada consulta de factura.
    """
    global rate_limiter
    
    print(f"[{config['id']}] === Procesando facturas desde vouchers ===")
    array_invoices = []
    
    # Extraer nombres de facturas desde los items de vouchers
    if 'items' in vouchers_df.columns:
        for items in vouchers_df['items']:
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and 'due' in item:
                        due = item['due']
                        if isinstance(due, dict) and 'prefix' in due and 'consecutive' in due:
                            prefix = due.get('prefix')
                            consecutive = due.get('consecutive')
                            if prefix and consecutive:
                                invoice_name = f"{prefix}-{consecutive}"
                                array_invoices.append(invoice_name)
    
    # Eliminar duplicados
    array_invoices = list(set(array_invoices))
    print(f"[{config['id']}] Se encontraron {len(array_invoices)} facturas únicas en vouchers")
    
    if not array_invoices:
        print(f"[{config['id']}] No se encontraron facturas para procesar.")
        return
    
    # Configuración para consulta de facturas
    batch_pause_interval = int(os.getenv("BATCH_PAUSE_INTERVAL", "20"))
    batch_pause_duration = int(os.getenv("BATCH_PAUSE_DURATION", "30"))
    invoices_processed = 0
    
    # Procesar cada factura individualmente
    for invoice_name in array_invoices:
        try:
            # Pausa preventiva cada cierto número de facturas procesadas
            if invoices_processed > 0 and invoices_processed % batch_pause_interval == 0:
                current_usage = rate_limiter.get_current_usage() if rate_limiter else 0
                print(f"[{config['id']}] Pausa preventiva después de {invoices_processed} facturas. Rate limit: {current_usage}")
                time.sleep(batch_pause_duration)
            
            print(f"[{config['id']}] Consultando factura: {invoice_name}")
            obtener_y_cargar_datos_por_endpoint(
                token=token,
                endpoint="invoices",
                fecha_inicio=invoice_name,
                query_param_name="name",
                config=config
            )
            invoices_processed += 1
            
        except Exception as e:
            print(f"[{config['id']}] ERROR al procesar factura {invoice_name}: {e}")
            # Continúa con la siguiente factura en caso de error
            continue
    
    print(f"[{config['id']}] Finalizado procesamiento de facturas. Total procesadas: {invoices_processed}/{len(array_invoices)}")


def obtener_y_cargar_datos_individuales(token, endpoint, param_name, param_value, config):
    """
    Función auxiliar para obtener datos de un registro individual (ej: factura por nombre).
    Optimizada para consultas unitarias con rate limiting.
    """
    global rate_limiter
    
    # Aplicar rate limiting
    if rate_limiter:
        rate_limiter.wait_if_needed()
    
    full_table_id = f"{config['bq_project']}.{config['bq_dataset']}.{endpoint}"
    final_url = f"{config['base_url'].strip('/')}/{endpoint.strip('/')}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Partner-Id": "Powerbi"
    }
    params = {param_name: param_value}
    
    try:
        response = request_with_retries("GET", final_url, headers=headers, params=params, timeout=60)
        json_response = response.json()
        
        data = json_response.get('results', []) if isinstance(json_response, dict) else json_response
        
        if data:
            df = pd.json_normalize(data)
            df.columns = [col.replace('.', '_') for col in df.columns]
            df = df.applymap(limpiar_valores_nulos_en_arreglos)
            cargar_a_bigquery(df, full_table_id, config=config)
            print(f"[{config['id']}] Cargado: {endpoint} con {param_name}={param_value} ({len(df)} registros)")
        else:
            print(f"[{config['id']}] No se encontraron datos para {endpoint} con {param_name}={param_value}")
            
    except Exception as e:
        print(f"[{config['id']}] ERROR al obtener {endpoint} con {param_name}={param_value}: {e}")
        raise


def run_etl_pipeline(request):
    """
    Punto de entrada para la Cloud Function.
    Incluye configuración optimizada de rate limiting.
    """
    global rate_limiter
    
    print("======= Iniciando ejecución del ETL con Rate Limiting Optimizado =======")
    
    # --- Cargar configuración desde Variables de Entorno ---
    bq_project = os.getenv("BQ_PROJECT")
    endpoints_str = os.getenv("BQ_TABLES")
    base_url = os.getenv("URL")
    url_auth = os.getenv("URL_AUTH")
    num_sources = int(os.getenv("NUM_SOURCES", "1"))
    num_days = int(os.getenv("NUM_DAYS", "2"))
    
    # Configuración de Rate Limiting
    rate_limit_per_minute = int(os.getenv("RATE_LIMIT_PER_MINUTE", "95"))
    batch_pause_interval = int(os.getenv("BATCH_PAUSE_INTERVAL", "20"))
    batch_pause_duration = int(os.getenv("BATCH_PAUSE_DURATION", "30"))
    
    # Inicializar Rate Limiter global
    rate_limiter = RateLimiter(max_requests=rate_limit_per_minute, time_window=60)
    print(f"Rate Limiter configurado: {rate_limit_per_minute} requests/min")
    print(f"Pausas preventivas cada {batch_pause_interval} requests por {batch_pause_duration}s")

    if not all([bq_project, endpoints_str, base_url, url_auth]):
        print("ERROR: Faltan variables de entorno esenciales (BQ_PROJECT, BQ_TABLES, URL, URL_AUTH).")
        return "Configuración incompleta.", 500

    configurations = []
    for i in range(1, num_sources + 1):
        config = {
            "id": f"Source{i}",
            "user": os.getenv(f"USER{i}"),
            "key": os.getenv(f"KEY{i}"),
            "bq_dataset": os.getenv(f"BQ_DATASET{i}"),
            "url_auth": url_auth,
            "base_url": base_url,
            "bq_project": bq_project
        }
        if not all([config["user"], config["key"], config["bq_dataset"]]):
            print(f"ADVERTENCIA: Configuración para Source{i} incompleta. Saltando...")
            continue
        configurations.append(config)

    # --- Cálculo de Fecha con NUM_DAYS ---
    hoy = datetime.date.today()
    start_date = (hoy - datetime.timedelta(days=num_days)).isoformat()
    print(f"Ejecutando carga para los últimos {num_days} día(s). Fecha de inicio: {start_date}")
    
    total_start_time = time.time()
    
    for config in configurations:
        print(f"\n======= Procesando Configuración: {config['id']} =======")
        try:
            token = obtener_token(config)
        except Exception as e:
            print(f"ERROR: No se pudo obtener token para {config['id']}: {e}.")
            continue

        endpoints = [e.strip() for e in endpoints_str.split(',') if e.strip()]
        
        for endpoint in endpoints:
            endpoint_start_time = time.time()
            
            try:
                print(f"\n--- Procesando {endpoint} con created_start ---")
                if endpoint == "vouchers":
                    print(f"[{config['id']}] Procesando vouchers: se extraerán facturas automáticamente")
                
                obtener_y_cargar_datos_por_endpoint(
                    token=token, 
                    endpoint=endpoint, 
                    fecha_inicio=start_date, 
                    query_param_name="created_start", 
                    config=config
                )
            except Exception as e:
                print(f"ERROR CRÍTICO durante el procesamiento de {endpoint} con created_start: {e}")

            try:
                if endpoint == "vouchers":
                    print(f"[{config['id']}] Saltando updated_start para vouchers (las facturas ya fueron procesadas)")
                    continue
                    
                print(f"\n--- Procesando {endpoint} con updated_start ---")
                obtener_y_cargar_datos_por_endpoint(
                    token=token, 
                    endpoint=endpoint, 
                    fecha_inicio=start_date, 
                    query_param_name="updated_start", 
                    config=config
                )
            except Exception as e:
                print(f"ERROR CRÍTICO durante el procesamiento de {endpoint} con updated_start: {e}")
            
            endpoint_duration = time.time() - endpoint_start_time
            current_usage = rate_limiter.get_current_usage()
            print(f"Endpoint {endpoint} completado en {endpoint_duration:.2f}s. Rate limit actual: {current_usage}")
    
    total_duration = time.time() - total_start_time
    print(f"\n======= Proceso ETL finalizado en {total_duration:.2f}s =======")
    print(f"Rate limit final: {rate_limiter.get_current_usage()}/{rate_limit_per_minute} requests/min")
    
    return "Proceso finalizado con éxito", 200
