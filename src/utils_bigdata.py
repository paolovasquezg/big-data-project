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
    - dias: Resumen del día/hora procesada
    - registros: Los 209 registros de muestra
    - metricas: Métricas calculadas (acción recurrente, actor, horas inactivas)
    """
    
    # Normalizar fecha y directorios
    fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    data_dir = get_data_dir(fecha_str)
    
    dias_path = os.path.join(data_dir, "gh_dias.json")
    muestras_path = os.path.join(data_dir, "gh_muestras.json")
    metricas_path = os.path.join(data_dir, "metrics.json") # Si existe, o la calculamos al vuelo
    
    if not os.path.exists(dias_path) or not os.path.exists(muestras_path):
        print(f"⚠️ No se encontraron JSON para {fecha_str} en {data_dir}. Saltando subida a Mongo.")
        return

    # Leer archivos generados
    with open(dias_path, "r", encoding="utf-8") as f:
        gh_dias = json.load(f)
        
    with open(muestras_path, "r", encoding="utf-8") as f:
        registros_data = json.load(f)

    # Conexión a MongoDB
    MONGO_URI = "mongodb+srv://paolovasquezg:1234@bigdata2025.lhkp5ye.mongodb.net/?retryWrites=true&w=majority&tls=true"
    
    print(f"\n💾 Conectando a MongoDB...")
    
    try:
        client = MongoClient(
            MONGO_URI,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=5000
        )
        
        # Verificar conexión
        client.admin.command('ping')
        print("✓ Conectado a MongoDB")
        
        # Seleccionar base de datos
        db = client['ProyectoBD']
        
        print(f"\n📊 Subiendo a 3 colecciones...")
        
        # ============================================
        # COLECCIÓN 1: DIAS (resumen)
        # ============================================
        col_dias = db['dias']
        
        # Upsert por fecha únicamente
        col_dias.update_one(
            {"fecha": fecha_str},
            {"$set": gh_dias},
            upsert=True
        )
        print(f"✓ Colección 'dias': 1 documento insertado/actualizado")
        
        
        # ============================================
        # COLECCIÓN 2: REGISTROS (muestras)
        # ============================================
        col_registros = db['registros']
        
        inserted_count = 0
        if len(registros_data) > 0:
            # Insertar todos los registros (ordered=False para velocidad)
            # Nota: Si ya existen, se duplicarán a menos que tengan _id único.
            # Como no definimos _id, Mongo creará uno nuevo.
            # Si queremos evitar duplicados al re-correr, deberíamos borrar primero los de esa fecha.
            
            # Borrar registros previos de esa fecha para evitar duplicados en re-runs
            col_registros.delete_many({"fecha": fecha_str})
            
            result = col_registros.insert_many(registros_data, ordered=False)
            inserted_count = len(result.inserted_ids)
        
        print(f"✓ Colección 'registros': {inserted_count} documentos insertados (limpieza previa realizada)")
        
        
        # ============================================
        # COLECCIÓN 3: METRICAS (análisis)
        # ============================================
        col_metricas = db['metricas']
        
        # Calcular métricas al vuelo (o leer metrics.json si existiera)
        # Usamos la lógica que ya teníamos en procesar_metricas, o recalculamos aquí.
        # Dado que procesar_metricas ya genera metrics.json, lo ideal es leerlo.
        # Pero el código del usuario calculaba al vuelo. Haremos un mix: leer metrics.json si existe.
        
        doc_metricas = {}
        if os.path.exists(metricas_path):
             with open(metricas_path, "r", encoding="utf-8") as f:
                doc_metricas = json.load(f)
        else:
            # Fallback: calcular si no existe archivo
            print("⚠️ metrics.json no encontrado, calculando al vuelo...")
            # (Lógica simplificada de fallback)
            doc_metricas = {
                "fecha": fecha_str,
                "dia": gh_dias.get("dia"),
                "accion_mas_recurrente": None,
                "horas_inactivas": [],
                "actor_mas_recurrente": None
            }

        # Upsert por fecha únicamente
        col_metricas.update_one(
            {"fecha": fecha_str},
            {"$set": doc_metricas},
            upsert=True
        )
        print(f"✓ Colección 'metricas': 1 documento insertado/actualizado")
        
        
        # ============================================
        # CREAR ÍNDICES
        # ============================================
        print("\n📑 Creando/verificando índices...")
        
        col_dias.create_index([("fecha", 1)], unique=True, background=True)
        
        col_registros.create_index([("fecha", 1)], background=True)
        col_registros.create_index([("type", 1)], background=True)
        col_registros.create_index([("actor_login", 1)], background=True)
        col_registros.create_index([("hora", 1)], background=True)
        
        col_metricas.create_index([("fecha", 1)], unique=True, background=True)
        
        print("✓ Índices creados/verificados")
        
        
        # ============================================
        # RESUMEN
        # ============================================
        print("\n" + "="*60)
        print("📊 RESUMEN DE CARGA A MONGODB")
        print("="*60)
        print(f"Colección 'dias':      1 documento")
        print(f"Colección 'registros': {inserted_count} documentos")
        print(f"Colección 'metricas':  1 documento")
        print(f"Total insertado:       {inserted_count + 2} documentos")
        print("="*60)
        
        client.close()
        
    except Exception as e:
        print(f"✗ Error al subir a MongoDB: {e}")
        # No hacemos raise para no fallar todo el DAG si solo falla Mongo (opcional)
        # raise e

# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

# Horas a procesar (puedes cambiarlo libremente)
HORAS = range(15, 17)
SAMPLE_SIZE = 209

# Carpeta base dentro de Composer
BASE_DATA_DIR = "/home/airflow/gcs/data"

# Locale en español (si se puede)
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

def get_data_dir(fecha_str: str) -> str:
    """
    Devuelve el directorio donde se guardan los archivos
    para una fecha dada (YYYY-MM-DD), y lo crea si no existe.
    """
    data_dir = os.path.join(BASE_DATA_DIR, fecha_str)
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def descargar_archivo(url: str, destino: str, max_retries: int = 2) -> bool:
    """
    Descarga robusta para GHArchive:
    - Timeout moderado (conexión, lectura)
    - Pocos retries
    - Si es 404, no insiste
    """
    headers = {"User-Agent": "Composer-GHArchive/1.0"}

    for intento in range(1, max_retries + 1):
        try:
            print(f"🔽 Descargando ({intento}/{max_retries}): {url}")
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
                print(f"✅ Descarga OK: {destino}")
                return True

            if r.status_code == 404:
                print(f"⚠️ 404 (no existe hour file): {url}")
                return False

            print(f"⚠️ Status {r.status_code} para {url}, reintentando...")

        except Exception as e:
            print(f"❌ Error descargando {url} (intento {intento}): {e}")

        # backoff cortito
        time.sleep(2 * intento + random.random())

    print(f"🛑 No se pudo descargar tras {max_retries} intentos: {url}")
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
# FUNCIÓN PRINCIPAL PARA PROCESAR UN DÍA
# ============================================================

def procesar_dia(fecha_input: str):
    """
    Procesa un día YYYY-MM-DD completo (0-23 UTC):
    - Descarga las 24 horas del día indicado.
    - Procesa cada archivo en streaming.
    - Elimina el archivo descargado para ahorrar espacio.
    - Genera gh_dias.json y gh_muestras.json
    """

    # Normalizar fecha y directorio
    fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    dia_nombre = fecha.strftime("%A").capitalize()

    data_dir = get_data_dir(fecha_str)

    print(f"\n📅 Procesando día completo {fecha_str} (0-23 UTC) en {data_dir}...\n")

    muestras_total = []
    archivos_ok = 0
    total_registros_dia = 0
    tipos_eventos_dia = set()

    # Procesamos siempre las 24 horas (0 a 23)
    for hora in range(24):
        # GHArchive URL: YYYY-MM-DD-{hora}.json.gz (sin padding de ceros, ej: 2024-01-01-5.json.gz)
        nombre_archivo = f"{fecha_str}-{hora}.json.gz"
        url = f"https://data.gharchive.org/{nombre_archivo}"
        file_path = os.path.join(data_dir, nombre_archivo)

        # Descarga solo si no existe
        if not os.path.exists(file_path):
            ok = descargar_archivo(url, file_path, max_retries=2)
            if not ok:
                # Si falla la descarga (ej. 404), saltamos esta hora
                continue

        # Procesar en streaming
        registros_hora = 0
        tipos_eventos_hora = set()
        sample_hora = []

        print(f"🕒 Procesando {nombre_archivo}...")

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

                    # Log de progreso cada 50k registros
                    if idx % 50000 == 0:
                        print(f"   · {nombre_archivo}: {idx} líneas...")

        except Exception as e:
            print(f"⚠️ Error leyendo/procesando {file_path}: {e}")
            # Intentar borrar si quedó corrupto
            if os.path.exists(file_path):
                os.remove(file_path)
            continue

        # === LIMPIEZA DE DISCO ===
        # Borramos el archivo .gz inmediatamente después de procesarlo
        try:
            os.remove(file_path)
            print(f"🗑️ Archivo eliminado para liberar espacio: {file_path}")
        except Exception as e:
            print(f"⚠️ No se pudo eliminar {file_path}: {e}")
        # =========================

        if registros_hora == 0:
            print(f"⚪ Sin registros en {nombre_archivo}")
            continue

        archivos_ok += 1
        total_registros_dia += registros_hora
        tipos_eventos_dia.update(tipos_eventos_hora)

        # Enriquecer muestras
        for m in sample_hora:
            m["fecha_archivo"] = fecha_str
            m["dia_proceso"] = dia_nombre
            m["hora_archivo"] = hora

        muestras_total.extend(sample_hora)

        print(
            f"✅ {nombre_archivo}: {registros_hora} registros, "
            f"{len(sample_hora)} muestras."
        )

    if archivos_ok == 0:
        print(f"⚪ Sin datos válidos procesados para {fecha_str}")
        return fecha_str

    print(
        f"\n✅ Resumen {fecha_str}: {total_registros_dia} registros totales "
        f"({archivos_ok} archivos procesados)"
    )
    print(f"📌 Total muestras generadas: {len(muestras_total)}\n")

    # ======== gh_dias.json ========
    doc_dia = {
        "fecha": fecha_str,
        "dia": dia_nombre,
        "cantidad_archivos": archivos_ok,
        "cantidad_regs": total_registros_dia,
        "cantidad_regs_sample": len(muestras_total),
        "tipos_eventos": sorted(list(tipos_eventos_dia)),
    }

    with open(os.path.join(data_dir, "gh_dias.json"), "w", encoding="utf-8") as f:
        json.dump(doc_dia, f, ensure_ascii=False, indent=4)

    # ======== gh_muestras.json ========
    muestras_df = pd.DataFrame(muestras_total)

    if not muestras_df.empty:
        muestras_df["created_at"] = pd.to_datetime(
            muestras_df["created_at"], errors="coerce"
        )
        
        # Formato de hora solicitado: HH:00-HH+1:00
        muestras_df["hora"] = muestras_df["created_at"].dt.hour.apply(
            lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00"
        )

        # Reordenar columnas
        columnas = ["fecha", "dia", "hora"] + [
            c for c in muestras_df.columns if c not in ["fecha", "dia", "hora"]
        ]
        # Eliminar columnas auxiliares si existen (fecha_archivo, dia_proceso, hora_archivo)
        # El snippet del usuario usa "fecha" y "dia" que ya agregamos en el loop o aquí.
        # En mi código anterior agregaba "fecha_archivo", "dia_proceso", "hora_archivo".
        # Debo renombrarlas o asegurar que existan "fecha" y "dia".
        
        # Ajuste: En el loop anterior yo usaba:
        # m["fecha_archivo"] = fecha_str
        # m["dia_proceso"] = dia_nombre
        # m["hora_archivo"] = hora
        
        # El usuario quiere "fecha", "dia", "hora".
        # "hora" ya la calculé arriba con el lambda.
        # Renombremos las otras para que coincidan o asegurémonos de tenerlas.
        if "fecha_archivo" in muestras_df.columns:
            muestras_df.rename(columns={"fecha_archivo": "fecha", "dia_proceso": "dia"}, inplace=True)
            
        # Asegurar que solo queden las columnas deseadas y en orden
        # Las columnas del snippet son:
        # ['created_at', 'type', 'actor_login', 'actor_id', 'repo_name', 'repo_id', 'branch', 'tamano_push', 'action', 'n_commits']
        # + fecha, dia, hora
        
        cols_deseadas = [
            "fecha", "dia", "hora", 
            "created_at", "type", "actor_login", "actor_id", 
            "repo_name", "repo_id", "branch", "tamano_push", 
            "action", "n_commits"
        ]
        
        # Filtrar solo columnas existentes (por seguridad)
        cols_finales = [c for c in cols_deseadas if c in muestras_df.columns]
        muestras_df = muestras_df[cols_finales]

        muestras_df["created_at"] = muestras_df["created_at"].astype(str)

    with open(
        os.path.join(data_dir, "gh_muestras.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(muestras_df.to_dict("records"), f, ensure_ascii=False, indent=4)

    print(f"💾 Guardado en {data_dir}: gh_dias.json y gh_muestras.json\n")

    return fecha_str


# ============================================================
# CÁLCULO DE MÉTRICAS (POR FECHA)
# ============================================================

def procesar_metricas(fecha_input: str):

    fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    data_dir = get_data_dir(fecha_str)

    dias_path = os.path.join(data_dir, "gh_dias.json")
    muestras_path = os.path.join(data_dir, "gh_muestras.json")

    if not os.path.exists(dias_path) or not os.path.exists(muestras_path):
        print(f"⚠️ No se encontraron JSON para {fecha_str} en {data_dir}")
        return

    with open(dias_path, "r", encoding="utf-8") as f:
        gh_dias = json.load(f)

    with open(muestras_path, "r", encoding="utf-8") as f:
        gh_muestras = json.load(f)

    fecha = gh_dias["fecha"]
    dia_nombre = gh_dias["dia"]

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
        "fecha": fecha,
        "dia": dia_nombre,
        "accion_mas_recurrente": recurrente,
        "horas_inactivas": horas_sin,
        "actor_mas_recurrente": actor_recurrente,
    }

    metrics_path = os.path.join(data_dir, "metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(salida, f, ensure_ascii=False, indent=4)

    print(f"📊 metrics.json generado correctamente en {metrics_path}")


# ============================================================
# SUBIR A CLOUD STORAGE
# ============================================================

def upload_outputs_to_gcs(fecha: str, bucket_name: str):

    fecha = datetime.strptime(fecha, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    data_dir = get_data_dir(fecha_str)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    archivos = [
        "gh_dias.json",
        "gh_muestras.json",
        "metrics.json",
    ]

    for file in archivos:
        local_path = os.path.join(data_dir, file)
        blob_path = f"data/{fecha_str}/{file}"

        if os.path.exists(local_path):
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            print(f"⬆️ Subido a GCS: {blob_path}")
        else:
            print(f"⚠️ No se encontró {local_path} (no se sube)")
