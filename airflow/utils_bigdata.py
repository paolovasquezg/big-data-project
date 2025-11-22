import requests
import gzip
import json
import os
import pandas as pd
from datetime import datetime
import locale
from google.cloud import storage
import time
import random
from pymongo import MongoClient
import certifi

# ... (resto del archivo igual hasta el final) ...

# ============================================================
# SUBIR A MONGODB
# ============================================================

def upload_to_mongodb(fecha_input: str):
    """
    Tarea 4: Subir datos procesados a MongoDB Atlas
    
    Inserta en 3 colecciones:
    - dias: Resumen del dÃ­a/hora procesada
    - registros: Los 209 registros de muestra
    - metricas: MÃ©tricas calculadas (acciÃ³n recurrente, actor, horas inactivas)
    """
    
    # Normalizar fecha
    fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    data_dir = BASE_DATA_DIR
    
    dias_path = os.path.join(data_dir, "gh_dias.json")
    muestras_path = os.path.join(data_dir, "gh_muestras.json")
    metrics_path = os.path.join(data_dir, "metrics.json")
    
    if not os.path.exists(dias_path) or not os.path.exists(muestras_path):
        print(f"âš ï¸ No se encontraron JSON globales en {data_dir}. Saltando subida a Mongo.")
        return

    # Leer archivos globales y filtrar
    with open(dias_path, "r", encoding="utf-8") as f:
        lista_dias = json.load(f)
        gh_dias = next((d for d in lista_dias if d["fecha"] == fecha_str), None)
        
    with open(muestras_path, "r", encoding="utf-8") as f:
        todas_muestras = json.load(f)
        registros_data = [m for m in todas_muestras if m.get("fecha") == fecha_str]

    if not gh_dias:
        print(f"âš ï¸ No hay datos procesados para {fecha_str} en gh_dias.json")
        return

    # ConexiÃ³n a MongoDB
    MONGO_URI = "mongodb+srv://paolovasquezg:1234@bigdata2025.lhkp5ye.mongodb.net/?retryWrites=true&w=majority&tls=true"
    
    print(f"\nðŸ’¾ Conectando a MongoDB...")
    
    try:
        client = MongoClient(
            MONGO_URI,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=5000
        )
        
        # Verificar conexiÃ³n
        client.admin.command('ping')
        print("âœ“ Conectado a MongoDB")
        
        # Seleccionar base de datos
        db = client['ProyectoBD']
        
        print(f"\nðŸ“Š Subiendo a 3 colecciones...")
        
        # ============================================
        # COLECCIÃ“N 1: DIAS (resumen)
        # ============================================
        col_dias = db['dias']
        
        # Upsert por fecha Ãºnicamente
        col_dias.update_one(
            {"fecha": fecha_str},
            {"$set": gh_dias},
            upsert=True
        )
        print(f"âœ“ ColecciÃ³n 'dias': 1 documento insertado/actualizado")
        
        
        # ============================================
        # COLECCIÃ“N 2: REGISTROS (muestras)
        # ============================================
        col_registros = db['registros']
        
        inserted_count = 0
        if len(registros_data) > 0:
            # Borrar registros previos de esa fecha para evitar duplicados en re-runs
            col_registros.delete_many({"fecha": fecha_str})
            
            result = col_registros.insert_many(registros_data, ordered=False)
            inserted_count = len(result.inserted_ids)
        
        print(f"âœ“ ColecciÃ³n 'registros': {inserted_count} documentos insertados (limpieza previa realizada)")
        
        
        # ============================================
        # COLECCIÃ“N 3: METRICAS (anÃ¡lisis)
        # ============================================
        col_metricas = db['metricas']
        
        doc_metricas = {}
        if os.path.exists(metrics_path):
             with open(metrics_path, "r", encoding="utf-8") as f:
                lista_metricas = json.load(f)
                doc_metricas = next((m for m in lista_metricas if m.get("fecha") == fecha_str), {})
        
        if not doc_metricas:
            # Fallback: calcular si no existe en archivo
            print("âš ï¸ MÃ©tricas no encontradas en archivo, calculando al vuelo...")
            doc_metricas = {
                "fecha": fecha_str,
                "dia": gh_dias.get("dia"),
                "accion_mas_recurrente": None,
                "horas_inactivas": [],
                "actor_mas_recurrente": None
            }

        # Upsert por fecha Ãºnicamente
        col_metricas.update_one(
            {"fecha": fecha_str},
            {"$set": doc_metricas},
            upsert=True
        )
        print(f"âœ“ ColecciÃ³n 'metricas': 1 documento insertado/actualizado")
        
        
        # ============================================
        # CREAR ÃNDICES
        # ============================================
        print("\nðŸ“‘ Creando/verificando Ã­ndices...")
        
        col_dias.create_index([("fecha", 1)], unique=True, background=True)
        
        col_registros.create_index([("fecha", 1)], background=True)
        col_registros.create_index([("type", 1)], background=True)
        col_registros.create_index([("actor_login", 1)], background=True)
        col_registros.create_index([("hora", 1)], background=True)
        
        col_metricas.create_index([("fecha", 1)], unique=True, background=True)
        
        print("âœ“ Ãndices creados/verificados")
        
        
        # ============================================
        # RESUMEN
        # ============================================
        print("\n" + "="*60)
        print("ðŸ“Š RESUMEN DE CARGA A MONGODB")
        print("="*60)
        print(f"ColecciÃ³n 'dias':      1 documento")
        print(f"ColecciÃ³n 'registros': {inserted_count} documentos")
        print(f"ColecciÃ³n 'metricas':  1 documento")
        print(f"Total insertado:       {inserted_count + 2} documentos")
        print("="*60)
        
        client.close()
        
    except Exception as e:
        print(f"âœ— Error al subir a MongoDB: {e}")
        # No hacemos raise para no fallar todo el DAG si solo falla Mongo (opcional)
        # raise e

# ============================================================
# CONFIGURACIÃ“N GENERAL
# ============================================================

# Horas a procesar (puedes cambiarlo libremente)
HORAS = range(15, 17)
SAMPLE_SIZE = 209

# Carpeta base dentro de Composer
BASE_DATA_DIR = "/home/airflow/gcs/data"

# Locale en espaÃ±ol (si se puede)
try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except Exception:
    try:
        locale.setlocale(locale.LC_TIME, 'es_ES')
    except Exception:
        pass


# ============================================================
# HELPERS
# ============================================================




def descargar_archivo(url: str, destino: str, max_retries: int = 2) -> bool:
    """
    Descarga robusta para GHArchive:
    - Timeout moderado (conexiÃ³n, lectura)
    - Pocos retries
    - Si es 404, no insiste
    """
    headers = {"User-Agent": "Composer-GHArchive/1.0"}

    for intento in range(1, max_retries + 1):
        try:
            print(f"ðŸ”½ Descargando ({intento}/{max_retries}): {url}")
            # timeout=(connect_timeout, read_timeout)
            r = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=(10, 60),
            )

            if r.status_code == 200:
                with open(destino, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                print(f"âœ… Descarga OK: {destino}")
                return True

            if r.status_code == 404:
                print(f"âš ï¸ 404 (no existe hour file): {url}")
                return False

            print(f"âš ï¸ Status {r.status_code} para {url}, reintentando...")

        except Exception as e:
            print(f"âŒ Error descargando {url} (intento {intento}): {e}")

        # backoff cortito
        time.sleep(2 * intento + random.random())

    print(f"ðŸ›‘ No se pudo descargar tras {max_retries} intentos: {url}")
    return False


def _procesar_evento(event: dict):
    """
    Extrae los campos que necesitamos de un evento bruto de GHArchive.
    Devuelve un diccionario listo para usarse en el sample.
    """
    tipo = event.get("type")
    actor = event.get("actor") or {}
    repo = event.get("repo") or {}
    payload = event.get("payload") or {}
    created_at = event.get("created_at")

    branch = None
    tamano_push = 0
    action = None
    n_commits = 0

    if tipo == "PushEvent":
        branch = (payload.get("ref") or "").split("/")[-1]
        tamano_push = payload.get("size", 0)
        commits = payload.get("commits") or []
        try:
            n_commits = len(commits)
        except Exception:
            n_commits = 0
    elif tipo in ["PullRequestEvent", "IssuesEvent"]:
        action = payload.get("action")

    row = {
        "created_at": created_at,
        "type": tipo,
        "actor_login": actor.get("login"),
        "actor_id": actor.get("id"),
        "repo_name": repo.get("name"),
        "repo_id": repo.get("id"),
        "branch": branch,
        "tamano_push": tamano_push,
        "action": action,
        "n_commits": n_commits,
    }
    return row


# ============================================================
# FUNCIÃ“N PRINCIPAL PARA PROCESAR UN DÃA
# ============================================================

def procesar_dia(fecha_input: str):
    """
    Procesa un dÃ­a YYYY-MM-DD completo (0-23 UTC):
    - Descarga las 24 horas del dÃ­a indicado.
    - Procesa cada archivo en streaming.
    - Elimina el archivo descargado para ahorrar espacio.
    - Genera gh_dias.json y gh_muestras.json
    """

    # Normalizar fecha
    fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    dia_nombre = fecha.strftime("%A").capitalize()

    # Usamos BASE_DATA_DIR directamente (estructura plana)
    data_dir = BASE_DATA_DIR
    os.makedirs(data_dir, exist_ok=True)

    print(f"\nðŸ“… Procesando dÃ­a completo {fecha_str} (0-23 UTC) en {data_dir}...\n")

    muestras_total = []
    archivos_ok = 0
    total_registros_dia = 0
    tipos_eventos_dia = set()

    # Procesamos siempre las 24 horas (0 a 23)
    for hora in range(24):
        # GHArchive URL: YYYY-MM-DD-{hora}.json.gz
        nombre_archivo = f"{fecha_str}-{hora}.json.gz"
        url = f"https://data.gharchive.org/{nombre_archivo}"
        file_path = os.path.join(data_dir, nombre_archivo)

        # Descarga solo si no existe
        if not os.path.exists(file_path):
            ok = descargar_archivo(url, file_path, max_retries=2)
            if not ok:
                continue

        # Procesar en streaming
        registros_hora = 0
        tipos_eventos_hora = set()
        sample_hora = []

        print(f"ðŸ•’ Procesando {nombre_archivo}...")

        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                for idx, line in enumerate(f, start=1):
                    try:
                        event = json.loads(line)
                    except Exception:
                        continue

                    registros_hora += 1
                    tipo = event.get("type")
                    if tipo:
                        tipos_eventos_hora.add(tipo)

                    row = _procesar_evento(event)

                    # Reservoir sampling para esta hora
                    if len(sample_hora) < SAMPLE_SIZE:
                        sample_hora.append(row)
                    else:
                        j = random.randint(0, registros_hora - 1)
                        if j < SAMPLE_SIZE:
                            sample_hora[j] = row

                    if idx % 50000 == 0:
                        print(f"   Â· {nombre_archivo}: {idx} lÃ­neas...")

        except Exception as e:
            print(f"âš ï¸ Error leyendo/procesando {file_path}: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
            continue

        # === LIMPIEZA DE DISCO ===
        try:
            os.remove(file_path)
            print(f"ðŸ—‘ï¸ Archivo eliminado: {file_path}")
        except Exception as e:
            print(f"âš ï¸ No se pudo eliminar {file_path}: {e}")
        # =========================

        if registros_hora == 0:
            print(f"âšª Sin registros en {nombre_archivo}")
            continue

        archivos_ok += 1
        total_registros_dia += registros_hora
        tipos_eventos_dia.update(tipos_eventos_hora)

        # Enriquecer muestras
        for m in sample_hora:
            m["fecha"] = fecha_str
            m["dia"] = dia_nombre
            m["hora_archivo"] = hora

        muestras_total.extend(sample_hora)

        print(
            f"âœ… {nombre_archivo}: {registros_hora} registros, "
            f"{len(sample_hora)} muestras."
        )

    if archivos_ok == 0:
        print(f"âšª Sin datos vÃ¡lidos procesados para {fecha_str}")
        return fecha_str

    print(
        f"\nâœ… Resumen {fecha_str}: {total_registros_dia} registros totales "
        f"({archivos_ok} archivos procesados)"
    )
    print(f"ðŸ“Œ Total muestras generadas: {len(muestras_total)}\n")

    # ======== ACTUALIZAR gh_dias.json (ACUMULATIVO) ========
    dias_path = os.path.join(data_dir, "gh_dias.json")
    lista_dias = []
    
    if os.path.exists(dias_path):
        try:
            with open(dias_path, "r", encoding="utf-8") as f:
                content = json.load(f)
                if isinstance(content, list):
                    lista_dias = content
                else:
                    # Si era dict (formato antiguo), lo convertimos a lista
                    lista_dias = [content]
        except Exception:
            lista_dias = []

    # Eliminar si ya existe registro de este dÃ­a (para re-proceso)
    lista_dias = [d for d in lista_dias if d.get("fecha") != fecha_str]

    doc_dia = {
        "fecha": fecha_str,
        "dia": dia_nombre,
        "cantidad_archivos": archivos_ok,
        "cantidad_regs": total_registros_dia,
        "cantidad_regs_sample": len(muestras_total),
        "tipos_eventos": sorted(list(tipos_eventos_dia)),
    }
    lista_dias.append(doc_dia)
    # Ordenar por fecha
    lista_dias.sort(key=lambda x: x["fecha"])

    with open(dias_path, "w", encoding="utf-8") as f:
        json.dump(lista_dias, f, ensure_ascii=False, indent=4)


    # ======== ACTUALIZAR gh_muestras.json (ACUMULATIVO) ========
    muestras_path = os.path.join(data_dir, "gh_muestras.json")
    lista_muestras = []

    if os.path.exists(muestras_path):
        try:
            with open(muestras_path, "r", encoding="utf-8") as f:
                lista_muestras = json.load(f)
        except Exception:
            lista_muestras = []

    # Convertir nuevas muestras a DF para procesar columnas
    muestras_df = pd.DataFrame(muestras_total)

    if not muestras_df.empty:
        muestras_df["created_at"] = pd.to_datetime(
            muestras_df["created_at"], errors="coerce"
        )
        
        # Formato de hora: HH:00-HH+1:00
        muestras_df["hora"] = muestras_df["created_at"].dt.hour.apply(
            lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00"
        )

        # Seleccionar columnas deseadas
        cols_deseadas = [
            "fecha", "dia", "hora", 
            "created_at", "type", "actor_login", "actor_id", 
            "repo_name", "repo_id", "branch", "tamano_push", 
            "action", "n_commits"
        ]
        cols_finales = [c for c in cols_deseadas if c in muestras_df.columns]
        muestras_df = muestras_df[cols_finales]
        muestras_df["created_at"] = muestras_df["created_at"].astype(str)
        
        nuevas_muestras = muestras_df.to_dict("records")
    else:
        nuevas_muestras = []

    # Eliminar muestras anteriores de esta fecha
    lista_muestras = [m for m in lista_muestras if m.get("fecha") != fecha_str]
    
    # Agregar nuevas
    lista_muestras.extend(nuevas_muestras)
    
    # (Opcional) Ordenar por fecha/hora si se desea, pero es costoso.
    # Lo dejamos append al final.

    with open(muestras_path, "w", encoding="utf-8") as f:
        json.dump(lista_muestras, f, ensure_ascii=False, indent=4)

    print(f"ðŸ’¾ Actualizados {dias_path} y {muestras_path} (Acumulativos)\n")

    return fecha_str


# ============================================================
# CÃLCULO DE MÃ‰TRICAS (POR FECHA)
# ============================================================

def procesar_metricas(fecha_input: str):

    fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    data_dir = BASE_DATA_DIR

    dias_path = os.path.join(data_dir, "gh_dias.json")
    muestras_path = os.path.join(data_dir, "gh_muestras.json")

    if not os.path.exists(dias_path) or not os.path.exists(muestras_path):
        print(f"âš ï¸ No se encontraron JSON globales en {data_dir}")
        return

    # Leer dÃ­a especÃ­fico de gh_dias.json
    with open(dias_path, "r", encoding="utf-8") as f:
        lista_dias = json.load(f)
        # Buscar el dÃ­a
        gh_dia = next((d for d in lista_dias if d["fecha"] == fecha_str), None)

    if not gh_dia:
        print(f"âš ï¸ No se encontrÃ³ informaciÃ³n para {fecha_str} en gh_dias.json")
        return

    # Leer muestras y filtrar por fecha
    # (Esto carga TODO en memoria, ojo con RAM si crece mucho)
    with open(muestras_path, "r", encoding="utf-8") as f:
        todas_muestras = json.load(f)
        gh_muestras = [m for m in todas_muestras if m.get("fecha") == fecha_str]

    dia_nombre = gh_dia["dia"]

    df = pd.DataFrame(gh_muestras)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

        recurrente = df["type"].value_counts().idxmax() if len(df) else None
        actor_recurrente = (
            df["actor_login"].value_counts().idxmax() if len(df) else None
        )
        horas_presentes = df["created_at"].dt.hour.unique().tolist()
        horas_sin = sorted(set(range(24)) - set(horas_presentes))
    else:
        recurrente = None
        actor_recurrente = None
        horas_presentes = []
        horas_sin = list(range(24))

    salida = {
        "fecha": fecha_str,
        "dia": dia_nombre,
        "accion_mas_recurrente": recurrente,
        "horas_inactivas": horas_sin,
        "actor_mas_recurrente": actor_recurrente,
    }

    # ======== ACTUALIZAR metrics.json (ACUMULATIVO) ========
    metrics_path = os.path.join(data_dir, "metrics.json")
    lista_metricas = []

    if os.path.exists(metrics_path):
        try:
            with open(metrics_path, "r", encoding="utf-8") as f:
                lista_metricas = json.load(f)
        except Exception:
            lista_metricas = []

    # Eliminar mÃ©trica anterior de esta fecha si existe
    lista_metricas = [m for m in lista_metricas if m.get("fecha") != fecha_str]
    
    lista_metricas.append(salida)
    lista_metricas.sort(key=lambda x: x["fecha"])

    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(lista_metricas, f, ensure_ascii=False, indent=4)

    print(f"ðŸ“Š metrics.json actualizado con datos de {fecha_str}")


# ============================================================
# SUBIR A CLOUD STORAGE
# ============================================================

def upload_outputs_to_gcs(fecha: str, bucket_name: str):

    # Subimos los archivos globales acumulativos
    # Nota: Esto sobreescribe el archivo en GCS con la versiÃ³n mÃ¡s reciente (que incluye todo el historial)
    data_dir = BASE_DATA_DIR

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    archivos = [
        "gh_dias.json",
        "gh_muestras.json",
        "metrics.json",
    ]

    for file in archivos:
        local_path = os.path.join(data_dir, file)
        # En GCS los guardamos en la raÃ­z o en una carpeta 'data' pero sin fecha, 
        # ya que contienen todas las fechas.
        blob_path = f"data/{file}"

        if os.path.exists(local_path):
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            print(f"â¬†ï¸ Subido a GCS: {blob_path}")
        else:
            print(f"âš ï¸ No se encontrÃ³ {local_path} (no se sube)")



# ============================================================
# CALCULAR KPIS GLOBALES (AGGREGATIONS MONGO)
# ============================================================

def calcular_kpis_global():
    """
    Calcula KPIs globales (no por fecha), igual que el notebook mongo_crud.ipynb
    Los KPIs se basan en las colecciones:
    - dias
    - registros
    - metricas
    Y se guardan en la colección 'kpis' (un solo documento).
    """

    print("\n📊 Calculando KPIs globales desde MongoDB...")

    # ======== Conexión a MongoDB ========
    try:
        MONGO_URI = "mongodb+srv://paolovasquezg:1234@bigdata2025.lhkp5ye.mongodb.net/?retryWrites=true&w=majority&tls=true"
        client = MongoClient(
            MONGO_URI,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=5000
        )
        client.admin.command("ping")
        print("✓ Conectado a MongoDB para agregaciones")

    except Exception as e:
        print(f"✖ Error conectando a MongoDB: {e}")
        return

    db = client["ProyectoBD"]
    dias = db["dias"]
    registros = db["registros"]
    metricas = db["metricas"]
    kpis = db["kpis"]

    # Limpiar documento previo (si existe)
    try:
        kpis.delete_many({})
    except:
        pass

    # ================================
    # KPI 1: promedio de actividad por día
    # ================================
    try:
        actividad_dia_pipeline = [
            {"$group": {"_id": None, "prom_actividad_por_dia": {"$avg": "$cantidad_regs"}}}
        ]
        actividad_dia = list(dias.aggregate(actividad_dia_pipeline))[0]["prom_actividad_por_dia"]
    except:
        actividad_dia = None

    # ================================
    # KPI 2: promedio de commits por día
    # ================================
    try:
        commits_dia_pipeline = [
            {"$group": {"_id": "$fecha", "sum_commits": {"$sum": "$n_commits"}}},
            {"$group": {"_id": None, "prom_commits_por_dia": {"$avg": "$sum_commits"}}},
        ]
        commits_dia = list(registros.aggregate(commits_dia_pipeline))[0]["prom_commits_por_dia"]
    except:
        commits_dia = None

    # ================================
    # KPI 3: promedio de commits por hora
    # ================================
    try:
        commits_hora_pipeline = [
            {"$group": {"_id": "$hora", "prom_commits_por_hora": {"$avg": "$n_commits"}}},
            {"$group": {"_id": None, "promedio": {"$avg": "$prom_commits_por_hora"}}}
        ]
        commits_hora = list(registros.aggregate(commits_hora_pipeline))[0]["promedio"]
    except:
        commits_hora = None

    # ================================
    # KPI 4: promedio de tamaño push por hora
    # ================================
    try:
        tamano_push_hora_pipeline = [
            {"$group": {"_id": "$hora", "prom": {"$avg": "$tamano_push"}}},
            {"$group": {"_id": None, "prom_tamano_push_por_hora": {"$avg": "$prom"}}}
        ]
        tamano_push_hora = list(registros.aggregate(tamano_push_hora_pipeline))[0]["prom_tamano_push_por_hora"]
    except:
        tamano_push_hora = None

    # ================================
    # KPI 5: actor más recurrente
    # ================================
    try:
        actor_rec_pipeline = [
            {"$group": {"_id": "$actor_login", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        actor_mas_rec = list(registros.aggregate(actor_rec_pipeline))[0]["_id"]
    except:
        actor_mas_rec = None

    # ================================
    # KPI 6: evento más recurrente
    # ================================
    try:
        evento_rec_pipeline = [
            {"$group": {"_id": "$type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        evento_mas_rec = list(registros.aggregate(evento_rec_pipeline))[0]["_id"]
    except:
        evento_mas_rec = None

    # ================================
    # KPI 7: promedio tamaño push por día
    # ================================
    try:
        tamano_push_dia_pipeline = [
            {"$group": {"_id": "$fecha", "sum_push": {"$sum": "$tamano_push"}}},
            {"$group": {"_id": None, "prom_tamano_push_dia": {"$avg": "$sum_push"}}}
        ]
        tamano_push_dia = list(registros.aggregate(tamano_push_dia_pipeline))[0]["prom_tamano_push_dia"]
    except:
        tamano_push_dia = None

    # ================================
    # COMPILAR DOCUMENTO FINAL (como notebook)
    # ================================
    kpis_insert = {
        "prom_actividad_por_dia": actividad_dia,
        "prom_commits_por_hora": commits_hora,
        "prom_tamano_push_por_hora": tamano_push_hora,
        "actor_mas_recurrente": actor_mas_rec,
        "evento_mas_recurrente": evento_mas_rec,
        "prom_tamano_push_dia": tamano_push_dia,
        "prom_commits_por_dia": commits_dia
    }

    kpis.insert_one(kpis_insert)

    print("✓ KPIs globales insertados en colección 'kpis'")
    client.close()

    return "kpis_globales"
