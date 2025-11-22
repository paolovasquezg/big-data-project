import requests
import gzip
import json
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import locale

# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

HORAS = range(0,2)
SAMPLE_SIZE = 209

# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def safe_eval(x):
    if isinstance(x, dict):
        return x
    try:
        return eval(x)
    except Exception:
        return {}

def contar_commits(payload):
    payload_dict = safe_eval(payload)
    if isinstance(payload_dict, dict) and 'commits' in payload_dict:
        return len(payload_dict['commits'])
    return 0

def extraer_detalles(payload, tipo):
    payload_dict = safe_eval(payload)
    detalles = {}
    if tipo == "PushEvent":
        detalles["branch"] = payload_dict.get("ref", "").split("/")[-1]
        detalles["tamano_push"] = payload_dict.get("size", 0)
        
    elif tipo in ["PullRequestEvent", "IssuesEvent"]:
        detalles["action"] = payload_dict.get("action")
    return detalles

try:
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
except:
    locale.setlocale(locale.LC_TIME, 'es_ES')

# ============================================================
# FUNCIÓN PRINCIPAL PARA PROCESAR UN DÍA
# ============================================================

def procesar_dia(fecha_input):

    fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
    fecha_str = fecha.strftime("%Y-%m-%d")
    dia_nombre = fecha.strftime("%A").capitalize()

    print(f"\n📅 Procesando {fecha_str}...\n")

    dfs_horas = []
    muestras_total = []   # <--- NUEVO: acumulador de muestras por hora
    archivos_ok = 0

    for hora in HORAS:
        # En la URL de GH Archive las horas 0-9 aparecen sin cero a la izquierda (p.ej. '-0.json.gz')
        # pero al guardar localmente preferimos usar dos dígitos para ordenar los ficheros ('-00.json.gz').
        url = f"https://data.gharchive.org/{fecha_str}-{hora}.json.gz"
        file_path = f"{fecha_str}-{hora:02d}.json.gz"

        if not os.path.exists(file_path):
            try:
                r = requests.get(url, stream=True, timeout=60)
                if r.status_code == 200:
                    with open(file_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                else:
                    continue
            except Exception as e:
                continue

        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                data = [json.loads(line) for line in f]

            if len(data) > 0:
                df = pd.DataFrame(data)

                # ----------------------------------------------------------
                # PROCESAMIENTO POR HORA (MISMAS COLUMNAS FINALES)
                # ----------------------------------------------------------
                df_m = df[['type', 'actor', 'repo', 'payload', 'created_at']].copy()

                df_m['actor_login'] = df_m['actor'].apply(lambda x: safe_eval(x).get('login'))
                df_m['actor_id'] = df_m['actor'].apply(lambda x: safe_eval(x).get('id'))
                df_m['repo_name'] = df_m['repo'].apply(lambda x: safe_eval(x).get('name'))
                df_m['repo_id'] = df_m['repo'].apply(lambda x: safe_eval(x).get('id'))

                detalles = df_m.apply(lambda x: extraer_detalles(x['payload'], x['type']), axis=1)
                df_m['branch'] = detalles.apply(lambda x: x.get("branch"))
                df_m['tamano_push'] = detalles.apply(lambda x: x.get("tamano_push"))
                df_m['tamano_push'] = df_m['tamano_push'].fillna(0)  # <--- esta línea
                df_m['action'] = detalles.apply(lambda x: x.get("action"))
                df_m['n_commits'] = df_m.apply(
                    lambda x: contar_commits(x['payload']) if x['type'] == 'PushEvent' else 0,
                    axis=1
                )

                # === SOLO LAS COLUMNAS QUE VAN A gh_muestras.json ===
                df_hora = df_m[
                    ['created_at', 'type', 'actor_login', 'actor_id',
                    'repo_name', 'repo_id', 'branch', 'tamano_push',
                    'action', 'n_commits']
                ].copy()

                # MUESTRA DE 209 POR HORA
                df_sample = df_hora.sample(SAMPLE_SIZE, random_state=42) \
                    if len(df_hora) > SAMPLE_SIZE else df_hora

                df_sample["fecha"] = fecha_str
                df_sample["dia"] = dia_nombre
                df_sample["hora"] = hora   # <--- mantiene la hora original numérica

                muestras_total.extend(df_sample.to_dict("records"))


                # ----------------------------------
                dfs_horas.append(df)
                archivos_ok += 1

        except Exception as e:
            continue

    if len(dfs_horas) == 0:
        return

    df_dia = pd.concat(dfs_horas, ignore_index=True)


    # ---------------------------------------------------------
    # PROCESAMIENTO
    # ---------------------------------------------------------
    df_dia = df_dia[['type', 'actor', 'repo', 'payload', 'created_at']]

    df_dia['actor_login'] = df_dia['actor'].apply(lambda x: safe_eval(x).get('login'))
    df_dia['actor_id'] = df_dia['actor'].apply(lambda x: safe_eval(x).get('id'))
    df_dia['repo_name'] = df_dia['repo'].apply(lambda x: safe_eval(x).get('name'))
    df_dia['repo_id'] = df_dia['repo'].apply(lambda x: safe_eval(x).get('id'))

    detalles = df_dia.apply(lambda x: extraer_detalles(x['payload'], x['type']), axis=1)
    df_dia['branch'] = detalles.apply(lambda x: x.get("branch"))
    df_dia['tamano_push'] = detalles.apply(lambda x: x.get("tamano_push"))
    df_dia['tamano_push'] = df_dia['tamano_push'].fillna(0)

    df_dia['action'] = detalles.apply(lambda x: x.get("action"))
    df_dia['n_commits'] = df_dia.apply(
        lambda x: contar_commits(x['payload']) if x['type'] == 'PushEvent' else 0,
        axis=1
    )

    df_filtrado = df_dia[
        ['created_at', 'type', 'actor_login', 'actor_id',
         'repo_name', 'repo_id', 'branch', 'tamano_push',
         'action', 'n_commits']
    ]

    # ---------------------------------------------------------
    # SALIDA: gh_dias.json (OBJETO, NO LISTA)
    # ---------------------------------------------------------
    doc_dia = {
        "fecha": fecha_str,
        "dia": dia_nombre,
        "cantidad_archivos": archivos_ok,
        "cantidad_regs": len(df_filtrado),
        "cantidad_regs_sample": len(muestras_total),   # <--- NUEVO
        "tipos_eventos": sorted(df_filtrado['type'].unique().tolist())
    }

    with open("gh_dias.json", "w", encoding="utf-8") as f:
        json.dump(doc_dia, f, ensure_ascii=False, indent=4)

    # ---------------------------------------------------------
    # SALIDA FINAL: gh_muestras.json (209 por cada hora)
    # ---------------------------------------------------------

    muestras_df = pd.DataFrame(muestras_total)

    # Convertir created_at a datetime
    muestras_df["created_at"] = pd.to_datetime(muestras_df["created_at"], errors="coerce")

    # NUEVO: mostrar la hora en rango HH:00-HH+1:00
    muestras_df["hora"] = muestras_df["created_at"].dt.hour.apply(
        lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00"
    )

    # Reordenar columnas
    columnas = ["fecha", "dia", "hora"] + [
        c for c in muestras_df.columns if c not in ["fecha", "dia", "hora"]
    ]
    muestras_df = muestras_df[columnas]



    # Asegurar que created_at sea string
    muestras_df["created_at"] = muestras_df["created_at"].astype(str)

    # Asegurar que no quede ningún Timestamp en ninguna columna
    for col in muestras_df.columns:
        muestras_df[col] = muestras_df[col].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") else x)

    with open("gh_muestras.json", "w", encoding="utf-8") as f:
        json.dump(muestras_df.to_dict("records"), f, ensure_ascii=False, indent=4)

    return fecha_str

# ============================================================
# CÁLCULO DE MÉTRICAS
# ============================================================

def procesar_metricas():
    with open("gh_dias.json", "r", encoding="utf-8") as f:
        gh_dias = json.load(f)

    with open("gh_muestras.json", "r", encoding="utf-8") as f:
        gh_muestras = json.load(f)

    fecha = gh_dias["fecha"]
    dia_nombre = gh_dias["dia"]

    df = pd.DataFrame(gh_muestras)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    recurrente = df["type"].value_counts().idxmax() if len(df) else None
    actor_recurrente = df["actor_login"].value_counts().idxmax() if len(df) else None

    horas_presentes = df["created_at"].dt.hour.unique().tolist()
    horas_sin = sorted(set(range(24)) - set(horas_presentes))

    salida = {
        "fecha": fecha,
        "dia": dia_nombre,
        "accion_mas_recurrente": recurrente,
        "horas_inactivas": horas_sin,
        "actor_mas_recurrente": actor_recurrente
    }

    with open("metrics.json", "w", encoding="utf-8") as f:
        json.dump(salida, f, ensure_ascii=False, indent=4)


# ============================================================
# EJECUCIÓN
# ============================================================

if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit()

    fecha_input = sys.argv[1]

    procesar_dia(fecha_input)
    procesar_metricas()
