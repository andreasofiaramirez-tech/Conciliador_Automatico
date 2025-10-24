# ==============================================================================
# --- 1. IMPORTACIÓN DE LIBRERÍAS ---
# ==============================================================================
# Aquí importamos todas las herramientas que necesitamos para que la aplicación funcione.
import streamlit as st
import pandas as pd
import numpy as np
import re
import xlsxwriter
from itertools import combinations
from io import BytesIO  # Para manejar archivos (como el Excel) en la memoria RAM en lugar de en el disco

# ==============================================================================
# --- 2. CONFIGURACIÓN INICIAL DE LA APLICACIÓN ---
# ==============================================================================
# Configura el título que aparece en la pestaña del navegador, el ícono y el layout de la página.
st.set_page_config(page_title="Conciliador Automático", page_icon="🤖", layout="wide")

# --- Inicialización del Estado de la Sesión (La "Memoria" de la App) ---
# Este bloque es CRUCIAL. Se ejecuta una sola vez al inicio de la sesión del usuario.
# Se asegura de que todas las variables de "memoria" existan desde el principio para evitar errores al refrescar la página.
if 'password_correct' not in st.session_state:
    st.session_state.password_correct = False # Controla si el usuario ha introducido la contraseña correcta.
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False # Una "bandera" que nos dice si la conciliación ya se hizo.
    st.session_state.log_messages = [] # Una lista para guardar los mensajes del proceso.
    st.session_state.csv_output = None # Guardará el archivo CSV de salida en memoria.
    st.session_state.excel_output = None # Guardará el archivo Excel de salida en memoria.
    st.session_state.df_saldos_abiertos = pd.DataFrame() # Guardará el DataFrame de saldos pendientes.
    st.session_state.df_conciliados = pd.DataFrame() # Guardará el DataFrame de movimientos conciliados.

# ==============================================================================
# --- 3. BLOQUE DE FUNCIONES DE LÓGICA DE DATOS ---
# ==============================================================================
# En esta sección definimos todas las funciones que realizan el trabajo pesado de procesar los datos.

# --- (A) Funciones Generales y de Ayuda ---
TOLERANCIAS_MAX_BS = 2.00 # Constante para el margen de error en Bolívares.

def mapear_columnas(df, log_messages):
    """
    Actúa como un "detective". Lee los nombres de las columnas del archivo del usuario
    y los renombra a un formato estándar que el resto del programa pueda entender.
    Ignora las columnas que no reconoce.
    """
    DEBITO_SYNONYMS = ['debito', 'debitos', 'débito', 'débitos']
    CREDITO_SYNONYMS = ['credito', 'creditos', 'crédito', 'créditos']
    BS_SYNONYMS = ['ves', 'bolivar', 'bolívar', 'local']
    USD_SYNONYMS = ['dolar', 'dólar', 'dólares', 'usd', 'dolares']

    REQUIRED_COLUMNS = {
        'Débito Bolivar': (DEBITO_SYNONYMS, BS_SYNONYMS),
        'Crédito Bolivar': (CREDITO_SYNONYMS, BS_SYNONYMS),
        'Débito Dolar': (DEBITO_SYNONYMS, USD_SYNONYMS),
        'Crédito Dolar': (CREDITO_SYNONYMS, USD_SYNONYMS),
    }

    column_mapping = {}
    current_cols = [col.strip() for col in df.columns] 

    for req_col, (type_synonyms, curr_synonyms) in REQUIRED_COLUMNS.items():
        found = False
        for input_col in current_cols:
            normalized_input = re.sub(r'[^\w]', '', input_col.lower())
            type_match = any(syn in normalized_input for syn in type_synonyms)
            curr_match = any(syn in normalized_input for syn in curr_synonyms)
            if type_match and curr_match and input_col not in column_mapping.values():
                column_mapping[input_col] = req_col
                found = True
                break
        if not found and req_col not in df.columns:
            log_messages.append(f"⚠️ ADVERTENCIA: No se encontró columna para '{req_col}'. Se creará vacía.")
            df[req_col] = 0.0 

    df.rename(columns=column_mapping, inplace=True)
    return df

def cargar_y_limpiar_datos(uploaded_actual, uploaded_anterior, log_messages):
    """
    Orquesta la carga y limpieza de los dos archivos EXCEL.
    - Limpia cada archivo por separado para asegurar que los formatos sean idénticos.
    - Une los dos archivos.
    - Elimina duplicados (clave para no repetir saldos anteriores).
    - Añade columnas de cálculo necesarias para la conciliación.
    """
    def intentar_cargar_y_limpiar_excel(archivo_buffer):
        try:
            archivo_buffer.seek(0)
            # Usamos pd.read_excel, asumiendo que los datos están en la primera hoja.
            # Forzamos que 'Asiento' se lea como texto para evitar problemas de formato.
            df = pd.read_excel(archivo_buffer, engine='openpyxl', dtype={'Asiento': str})
        except Exception as e:
            log_messages.append(f"❌ Error al leer el archivo Excel: {e}")
            return None

        df.columns = df.columns.str.strip()
        df = mapear_columnas(df, log_messages)
        
        columnas_montos = ['Débito Bolivar', 'Crédito Bolivar', 'Débito Dolar', 'Crédito Dolar']
        df = df.copy()
        df['Asiento'] = df['Asiento'].astype(str).str.strip()
        df['Referencia'] = df['Referencia'].astype(str).str.strip()
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce') # Excel maneja mejor las fechas, pero la conversión asegura consistencia.

        for col in columnas_montos:
            if col in df.columns:
                # Si la columna ya es numérica (leída correctamente por Excel), solo la redondeamos y continuamos.
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].round(2)
                    continue
                
                # Si la columna es texto, aplicamos la lógica de limpieza segura.
                # Convertimos todo a string para un tratamiento uniforme.
                temp_serie = df[col].astype(str).str.strip()
                
                # Lógica segura: primero quitar separadores de miles (.), luego cambiar coma decimal (,) por punto.
                temp_serie = temp_serie.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                
                # Convertimos a numérico. El 'errors='coerce'' maneja cualquier valor que no se pueda convertir.
                df[col] = pd.to_numeric(temp_serie, errors='coerce').fillna(0.0).round(2)
        
        return df

    # 1. Cargar y limpiar cada archivo POR SEPARADO
    log_messages.append("Iniciando lectura de archivos Excel...")
    df_actual = intentar_cargar_y_limpiar_excel(uploaded_actual)
    df_anterior = intentar_cargar_y_limpiar_excel(uploaded_anterior)

    if df_actual is None or df_anterior is None:
        st.error("❌ ¡Error Fatal! No se pudo procesar uno o ambos archivos Excel. Verifique que no estén corruptos y que los datos estén en la primera hoja.")
        return None

    # 2. Unir los archivos YA LIMPIOS Y REDONDEADOS
    df_full = pd.concat([df_anterior, df_actual], ignore_index=True)

    # 3. Eliminar duplicados de la unión
    key_cols = ['Asiento', 'Referencia', 'Fecha', 'Débito Bolivar', 'Crédito Bolivar', 'Débito Dolar', 'Crédito Dolar']
    cols_for_dedup = [col for col in key_cols if col in df_full.columns]
    initial_len = len(df_full)
    df_full.drop_duplicates(subset=cols_for_dedup, keep='first', inplace=True)
    dedup_len = len(df_full)
    if initial_len != dedup_len:
        log_messages.append(f"ℹ️ ¡DUPLICADOS ENCONTRADOS! Se eliminaron {initial_len - dedup_len} movimientos idénticos que estaban en ambos archivos.")

    # 4. Añadir columnas finales para la conciliación
    df_full['Monto_BS'] = (df_full['Débito Bolivar'] - df_full['Crédito Bolivar']).round(2)
    df_full['Monto_USD'] = (df_full['Débito Dolar'] - df_full['Crédito Dolar']).round(2)
    df_full['Conciliado'] = False
    df_full['Grupo_Conciliado'] = np.nan
    df_full['Referencia_Normalizada_Literal'] = np.nan

    log_messages.append(f"✅ Datos de Excel cargados y limpiados. Total movimientos a procesar: {len(df_full)}")
    return df_full

# --- (B) Funciones Específicas de la Estrategia "Fondos en Tránsito" ---
def normalizar_referencia_fondos_en_transito(df):
    """
    Clasifica cada movimiento de "Fondos en Tránsito" según las palabras clave en su referencia.
    Crea las columnas 'Clave_Normalizada' y 'Clave_Grupo' que son el cerebro de la conciliación.
    """
    df_copy = df.copy()
    def clasificar(referencia_str):
        if pd.isna(referencia_str): return 'OTRO', 'OTRO', ''
        ref = str(referencia_str).upper().strip()
        ref_lit_norm = re.sub(r'[^A-Z0-9]', '', ref)
        if any(keyword in ref for keyword in ['DIFERENCIA EN CAMBIO', 'DIF. CAMBIO', 'DIFERENCIAL', 'DIFERENCIAS DE CAMBIO', 'DIFERENCIAS DE SALDOS', 'DIFERENCIA DE SALDO', 'DIF. SALDO']): return 'DIF_CAMBIO', 'GRUPO_DIF_CAMBIO', ref_lit_norm
        if 'AJUSTE' in ref: return 'AJUSTE_GENERAL', 'GRUPO_AJUSTE', ref_lit_norm
        if 'REINTEGRO' in ref or 'SILLACA' in ref: return 'REINTEGRO_SILLACA', 'GRUPO_SILLACA', ref_lit_norm
        if 'REMESA' in ref: return 'REMESA_GENERAL', 'GRUPO_REMESA', ref_lit_norm
        if 'NOTA DE DEBITO' in ref or 'NOTA DE CREDITO' in ref: return 'NOTA_GENERAL', 'GRUPO_NOTA', ref_lit_norm
        if 'BANCO A BANCO' in ref: return 'BANCO_A_BANCO', 'GRUPO_BANCO', ref_lit_norm
        return 'OTRO', 'OTRO', ref_lit_norm
    df_copy[['Clave_Normalizada', 'Clave_Grupo', 'Referencia_Normalizada_Literal']] = df_copy['Referencia'].apply(clasificar).apply(pd.Series)
    return df_copy

def conciliar_diferencia_cambio(df, log_messages):
    df_a_conciliar = df[(df['Clave_Grupo'] == 'GRUPO_DIF_CAMBIO') & (~df['Conciliado'])]
    total_conciliados = len(df_a_conciliar)
    if total_conciliados > 0:
        indices = df_a_conciliar.index
        df.loc[indices, 'Conciliado'] = True
        df.loc[indices, 'Grupo_Conciliado'] = 'AUTOMATICO_DIF_CAMBIO_SALDO'
        log_messages.append(f"✔️ Fase Auto: {total_conciliados} conciliados por ser 'Diferencia en Cambio/Saldo'.")
    return total_conciliados

def conciliar_ajuste_automatico(df, log_messages):
    df_a_conciliar = df[(df['Clave_Grupo'] == 'GRUPO_AJUSTE') & (~df['Conciliado'])]
    total_conciliados = len(df_a_conciliar)
    if total_conciliados > 0:
        indices = df_a_conciliar.index
        df.loc[indices, 'Conciliado'] = True
        df.loc[indices, 'Grupo_Conciliado'] = 'AUTOMATICO_AJUSTE'
        log_messages.append(f"✔️ Fase Auto: {total_conciliados} conciliados por ser 'AJUSTE'.")
    return total_conciliados

def conciliar_pares_exactos_cero(df, clave_grupo, fase_name, log_messages):
    TOLERANCIA_CERO = 0.0
    df_pendientes = df[(df['Clave_Grupo'] == clave_grupo) & (~df['Conciliado'])].copy()
    if df_pendientes.empty: return 0
    log_messages.append(f"\n--- {fase_name} ---")
    grupos = df_pendientes.groupby('Referencia_Normalizada_Literal')
    total_conciliados = 0
    for ref_norm, grupo in grupos:
        if len(grupo) < 2: continue
        debitos_indices = grupo[grupo['Monto_BS'] > 0].index
        creditos_indices = grupo[grupo['Monto_BS'] < 0].index
        debitos_usados = set()
        creditos_usados = set()
        for idx_d in debitos_indices:
            if idx_d in debitos_usados: continue
            monto_d = df.loc[idx_d, 'Monto_BS']
            for idx_c in creditos_indices:
                if idx_c in creditos_usados: continue
                monto_c = df.loc[idx_c, 'Monto_BS']
                if abs(monto_d + monto_c) <= TOLERANCIA_CERO:
                    asiento_d, asiento_c = df.loc[idx_d, 'Asiento'], df.loc[idx_c, 'Asiento']
                    df.loc[[idx_d, idx_c], 'Conciliado'] = True
                    df.loc[idx_d, 'Grupo_Conciliado'] = f'PAR_REF_EXACTO_{ref_norm}_{asiento_c}'
                    df.loc[idx_c, 'Grupo_Conciliado'] = f'PAR_REF_EXACTO_{ref_norm}_{asiento_d}'
                    total_conciliados += 2
                    debitos_usados.add(idx_d)
                    creditos_usados.add(idx_c)
                    break 
    if total_conciliados > 0: log_messages.append(f"✔️ {fase_name}: {total_conciliados} movimientos conciliados.")
    return total_conciliados

def conciliar_pares_exactos_por_referencia(df, clave_grupo, fase_name, log_messages):
    df_pendientes = df[(df['Clave_Grupo'] == clave_grupo) & (~df['Conciliado'])].copy()
    if df_pendientes.empty: return 0
    log_messages.append(f"\n--- {fase_name} ---")
    grupos = df_pendientes.groupby('Referencia_Normalizada_Literal')
    total_conciliados = 0
    for ref_norm, grupo in grupos:
        if len(grupo) < 2: continue
        debitos_indices = grupo[grupo['Monto_BS'] > 0].index.tolist()
        creditos_indices = grupo[grupo['Monto_BS'] < 0].index.tolist()
        debitos_usados = set()
        creditos_usados = set()
        for idx_d in debitos_indices:
            if idx_d in debitos_usados: continue
            monto_d = df.loc[idx_d, 'Monto_BS']
            mejor_match_idx, mejor_match_diff = None, TOLERANCIAS_MAX_BS + 1
            for idx_c in creditos_indices:
                if idx_c in creditos_usados: continue
                diferencia = abs(monto_d + df.loc[idx_c, 'Monto_BS'])
                if diferencia < mejor_match_diff:
                    mejor_match_diff, mejor_match_idx = diferencia, idx_c
            if mejor_match_idx is not None and mejor_match_diff <= TOLERANCIAS_MAX_BS:
                asiento_d, asiento_c = df.loc[idx_d, 'Asiento'], df.loc[mejor_match_idx, 'Asiento']
                df.loc[[idx_d, mejor_match_idx], 'Conciliado'] = True
                df.loc[idx_d, 'Grupo_Conciliado'] = f'PAR_REF_{ref_norm}_{asiento_c}'
                df.loc[mejor_match_idx, 'Grupo_Conciliado'] = f'PAR_REF_{ref_norm}_{asiento_d}'
                total_conciliados += 2
                debitos_usados.add(idx_d)
                creditos_usados.add(mejor_match_idx)
    if total_conciliados > 0: log_messages.append(f"✔️ {fase_name}: {total_conciliados} movimientos conciliados.")
    return total_conciliados

def cruzar_pares_simples(df, clave_normalizada, fase_name, log_messages):
    df_pendientes = df[~df['Conciliado']].copy()
    df_pendientes['Monto_BS_Abs_Redondeado'] = (df_pendientes['Monto_BS'].abs().round(0))
    df_a_cruzar = df_pendientes[df_pendientes['Clave_Normalizada'] == clave_normalizada]
    if df_a_cruzar.empty: return 0
    log_messages.append(f"\n--- {fase_name} ---")
    grupos = df_a_cruzar.groupby('Monto_BS_Abs_Redondeado')
    total_conciliados = 0
    for _, grupo in grupos:
        debitos_indices = grupo[grupo['Monto_BS'] > 0].index.tolist()
        creditos_indices = grupo[grupo['Monto_BS'] < 0].index.tolist()
        debitos_usados = set()
        creditos_usados = set()
        for idx_d in debitos_indices:
            if idx_d in debitos_usados: continue
            monto_d = df.loc[idx_d, 'Monto_BS']
            mejor_match_idx, mejor_match_diff = None, TOLERANCIAS_MAX_BS + 1
            for idx_c in creditos_indices:
                if idx_c in creditos_usados: continue
                diferencia = abs(monto_d + df.loc[idx_c, 'Monto_BS'])
                if diferencia < mejor_match_diff:
                    mejor_match_diff, mejor_match_idx = diferencia, idx_c
            if mejor_match_idx is not None and mejor_match_diff <= TOLERANCIAS_MAX_BS:
                asiento_d, asiento_c = df.loc[idx_d, 'Asiento'], df.loc[mejor_match_idx, 'Asiento']
                df.loc[[idx_d, mejor_match_idx], 'Conciliado'] = True
                df.loc[idx_d, 'Grupo_Conciliado'] = f'PAR_BS_{asiento_c}'
                df.loc[mejor_match_idx, 'Grupo_Conciliado'] = f'PAR_BS_{asiento_d}'
                total_conciliados += 2
                debitos_usados.add(idx_d)
                creditos_usados.add(mejor_match_idx)
    if 'Monto_BS_Abs_Redondeado' in df.columns: df.drop(columns=['Monto_BS_Abs_Redondeado'], inplace=True, errors='ignore')
    if total_conciliados > 0: log_messages.append(f"✔️ {fase_name}: {total_conciliados} movimientos conciliados.")
    return total_conciliados

def cruzar_grupos_por_criterio(df, clave_normalizada, agrupacion_col, grupo_prefix, fase_name, log_messages):
    df_pendientes = df[(df['Clave_Normalizada'] == clave_normalizada) & (~df['Conciliado'])].copy()
    if df_pendientes.empty: return 0
    log_messages.append(f"\n--- {fase_name} ---")
    indices_conciliados = set()
    if agrupacion_col == 'Fecha': grupos = df_pendientes.groupby(df_pendientes['Fecha'].dt.date.fillna('NaT'))
    else: grupos = df_pendientes.groupby(agrupacion_col)
    for criterio, grupo in grupos:
        if len(grupo) > 1 and abs(grupo['Monto_BS'].sum()) <= TOLERANCIAS_MAX_BS:
            grupo_id = f"GRUPO_{grupo_prefix}_{criterio}"
            indices_a_conciliar = grupo.index
            df.loc[indices_a_conciliar, 'Conciliado'] = True
            df.loc[indices_a_conciliar, 'Grupo_Conciliado'] = grupo_id
            indices_conciliados.update(indices_a_conciliar)
    total_conciliados = len(indices_conciliados)
    if total_conciliados > 0: log_messages.append(f"✔️ {fase_name}: {total_conciliados} movimientos conciliados.")
    return total_conciliados

def conciliar_lote_por_grupo(df, clave_grupo, fase_name, log_messages):
    log_messages.append(f"\n--- {fase_name} ---")
    df_pendientes = df[(~df['Conciliado']) & (df['Clave_Grupo'] == clave_grupo)].copy()
    if df_pendientes.empty or len(df_pendientes) < 2: return 0
    if abs(df_pendientes['Monto_BS'].sum()) <= TOLERANCIAS_MAX_BS:
        fecha_max = df_pendientes['Fecha'].max().strftime('%Y-%m-%d')
        grupo_id = f"LOTE_{clave_grupo.replace('GRUPO_', '')}_{fecha_max}"
        indices_a_conciliar = df_pendientes.index
        df.loc[indices_a_conciliar, 'Conciliado'] = True
        df.loc[indices_a_conciliar, 'Grupo_Conciliado'] = grupo_id
        total_conciliados = len(indices_a_conciliar)
        log_messages.append(f"✔️ {fase_name}: {total_conciliados} movimientos conciliados como lote.")
        return total_conciliados
    return 0

def conciliar_grupos_globales_por_referencia(df, log_messages):
    log_messages.append(f"\n--- FASE GLOBAL N-a-N (Cruce por Referencia Literal) ---")
    df_pendientes = df[~df['Conciliado']].copy()
    df_pendientes = df_pendientes[df_pendientes['Referencia_Normalizada_Literal'].notna() & (df_pendientes['Referencia_Normalizada_Literal'] != '') & (df_pendientes['Referencia_Normalizada_Literal'] != 'OTRO')]
    if df_pendientes.empty: return 0
    grupos = df_pendientes.groupby('Referencia_Normalizada_Literal')
    total_conciliados = 0
    for ref_norm, grupo in grupos:
        if len(grupo) > 1 and abs(grupo['Monto_BS'].sum()) <= TOLERANCIAS_MAX_BS:
            grupo_id = f"GRUPO_REF_GLOBAL_{ref_norm}"
            indices_a_conciliar = grupo.index
            df.loc[indices_a_conciliar, 'Conciliado'] = True
            df.loc[indices_a_conciliar, 'Grupo_Conciliado'] = grupo_id
            total_conciliados += len(indices_a_conciliar)
    if total_conciliados > 0: log_messages.append(f"✔️ Fase Global N-a-N: {total_conciliados} movimientos conciliados.")
    return total_conciliados

def conciliar_pares_globales_remanentes(df, log_messages):
    log_messages.append(f"\n--- FASE GLOBAL 1-a-1 (Cruce de pares remanentes) ---")
    df_pendientes = df[~df['Conciliado']].copy()
    if df_pendientes.empty or len(df_pendientes) < 2: return 0
    debitos = df_pendientes[df_pendientes['Monto_BS'] > 0].index.tolist()
    creditos = df_pendientes[df_pendientes['Monto_BS'] < 0].index.tolist()
    total_conciliados = 0
    creditos_usados = set()
    for idx_d in debitos:
        monto_d = df.loc[idx_d, 'Monto_BS']
        mejor_match_idx, mejor_match_diff = None, TOLERANCIAS_MAX_BS + 1
        for idx_c in creditos:
            if idx_c in creditos_usados: continue
            diferencia = abs(monto_d + df.loc[idx_c, 'Monto_BS'])
            if diferencia < mejor_match_diff:
                mejor_match_diff, mejor_match_idx = diferencia, idx_c
        if mejor_match_idx is not None and mejor_match_diff <= TOLERANCIAS_MAX_BS:
            asiento_d, asiento_c = df.loc[idx_d, 'Asiento'], df.loc[mejor_match_idx, 'Asiento']
            df.loc[[idx_d, mejor_match_idx], 'Conciliado'] = True
            df.loc[idx_d, 'Grupo_Conciliado'] = f'PAR_GLOBAL_{asiento_c}'
            df.loc[mejor_match_idx, 'Grupo_Conciliado'] = f'PAR_GLOBAL_{asiento_d}'
            creditos_usados.add(mejor_match_idx)
            total_conciliados += 2
    if total_conciliados > 0: log_messages.append(f"✔️ Fase Global 1-a-1: {total_conciliados} movimientos conciliados.")
    return total_conciliados

def conciliar_grupos_complejos_remanentes(df, log_messages):
    log_messages.append("\n--- FASE GRUPOS COMPLEJOS (Buscando subconjuntos 1-a-N y N-a-1) ---")
    total_conciliados_fase = 0
    while True:
        continuar_ciclo = False
        df_pendientes = df.loc[~df['Conciliado']]
        if len(df_pendientes) < 3: break
        debitos = df_pendientes[df_pendientes['Monto_BS'] > 0]
        creditos = df_pendientes[df_pendientes['Monto_BS'] < 0]
        if len(debitos) == 0 or len(creditos) == 0: break
        for c_idx, c_row in creditos.iterrows():
            target = abs(c_row['Monto_BS'])
            for i in range(2, min(len(debitos) + 1, 10)): # Limitar combinaciones por rendimiento
                for combo_d_indices in combinations(debitos.index, i):
                    if abs(debitos.loc[list(combo_d_indices), 'Monto_BS'].sum() - target) <= TOLERANCIAS_MAX_BS:
                        indices_a_conciliar = list(combo_d_indices) + [c_idx]
                        grupo_id = f"GRUPO_COMPLEJO_N-1_{c_row['Asiento']}"
                        df.loc[indices_a_conciliar, ['Conciliado', 'Grupo_Conciliado']] = [True, grupo_id]
                        total_conciliados_fase += len(indices_a_conciliar)
                        continuar_ciclo = True
                        break
                if continuar_ciclo: break
            if continuar_ciclo: break
        if continuar_ciclo: continue
        for d_idx, d_row in debitos.iterrows():
            target = d_row['Monto_BS']
            for i in range(2, min(len(creditos) + 1, 10)): # Limitar combinaciones
                for combo_c_indices in combinations(creditos.index, i):
                    if abs(abs(creditos.loc[list(combo_c_indices), 'Monto_BS'].sum()) - target) <= TOLERANCIAS_MAX_BS:
                        indices_a_conciliar = list(combo_c_indices) + [d_idx]
                        grupo_id = f"GRUPO_COMPLEJO_1-N_{d_row['Asiento']}"
                        df.loc[indices_a_conciliar, ['Conciliado', 'Grupo_Conciliado']] = [True, grupo_id]
                        total_conciliados_fase += len(indices_a_conciliar)
                        continuar_ciclo = True
                        break
                if continuar_ciclo: break
            if continuar_ciclo: break
        if not continuar_ciclo: break
    if total_conciliados_fase > 0: log_messages.append(f"✔️ Fase Grupos Complejos: {total_conciliados_fase} movimientos conciliados.")
    return total_conciliados_fase

def conciliar_gran_total_final(df, log_messages):
    log_messages.append(f"\n--- FASE FINAL (Revisión Gran Total) ---")
    df_pendientes = df[~df['Conciliado']].copy()
    if df_pendientes.empty or len(df_pendientes) < 2: return 0
    suma_gran_total_bs = df_pendientes['Monto_BS'].sum()
    if abs(suma_gran_total_bs) <= TOLERANCIAS_MAX_BS:
        df.loc[df_pendientes.index, ['Conciliado', 'Grupo_Conciliado']] = [True, "LOTE_GRAN_TOTAL_FINAL"]
        total_conciliados = len(df_pendientes.index)
        log_messages.append(f"✔️ Fase Final: ¡Éxito! {total_conciliados} remanentes sumaron {suma_gran_total_bs:.2f} Bs y fueron conciliados.")
        return total_conciliados
    else:
        log_messages.append(f"ℹ️ Fase Final: No se concilió. Suma de remanentes es {suma_gran_total_bs:.2f} Bs.")
        return 0

# --- (C) Funciones Principales de Cada Estrategia ---
def run_conciliation_fondos_en_transito (df, log_messages):
    """
    Esta es la función "maestra" para la cuenta Fondos en Tránsito.
    Llama a todas las funciones de conciliación en el orden estratégico correcto,
    exactamente como lo hacía tu script original.
    """
    df = normalizar_referencia_fondos_en_transito(df)
    log_messages.append("\n--- INICIANDO LÓGICA DE FONDOS EN TRÁNSITO ---")
    
    # A. Fases Automáticas
    conciliar_diferencia_cambio(df, log_messages)
    conciliar_ajuste_automatico(df, log_messages)
    
    # B. FASES POR GRUPO (Buscando coincidencias dentro de la misma categoría)
    conciliar_pares_exactos_cero(df, 'GRUPO_SILLACA', 'FASE SILLACA 1/7 (Cruce CERO)', log_messages)
    conciliar_pares_exactos_por_referencia(df, 'GRUPO_SILLACA', 'FASE SILLACA 2/7 (Pares por Referencia)', log_messages)
    cruzar_pares_simples(df, 'REINTEGRO_SILLACA', 'FASE SILLACA 3/7 (Pares por Monto)', log_messages)
    cruzar_grupos_por_criterio(df, 'REINTEGRO_SILLACA', 'Asiento', 'SILLACA_ASIENTO', 'FASE SILLACA 4/7 (Grupos por Asiento)', log_messages)
    cruzar_grupos_por_criterio(df, 'REINTEGRO_SILLACA', 'Referencia_Normalizada_Literal', 'SILLACA_REF', 'FASE SILLACA 5/7 (Grupos por Ref. Literal)', log_messages)
    cruzar_grupos_por_criterio(df, 'REINTEGRO_SILLACA', 'Fecha', 'SILLACA_FECHA', 'FASE SILLACA 6/7 (Grupos por Fecha)', log_messages)
    conciliar_lote_por_grupo(df, 'GRUPO_SILLACA', 'FASE SILLACA 7/7 (CRUCE POR LOTE)', log_messages)

    conciliar_pares_exactos_cero(df, 'GRUPO_NOTA', 'FASE NOTAS 1/6 (Cruce CERO)', log_messages)
    conciliar_pares_exactos_por_referencia(df, 'GRUPO_NOTA', 'FASE NOTAS 2/6 (Pares por Referencia)', log_messages)
    cruzar_pares_simples(df, 'NOTA_GENERAL', 'FASE NOTAS 3/6 (Pares por Monto)', log_messages)
    cruzar_grupos_por_criterio(df, 'NOTA_GENERAL', 'Referencia_Normalizada_Literal', 'NOTA_REF', 'FASE NOTAS 4/6 (Grupos por Ref. Literal)', log_messages)
    cruzar_grupos_por_criterio(df, 'NOTA_GENERAL', 'Fecha', 'NOTA_FECHA', 'FASE NOTAS 5/6 (Grupos por Fecha)', log_messages)
    conciliar_lote_por_grupo(df, 'GRUPO_NOTA', 'FASE NOTAS 6/6 (CRUCE POR LOTE)', log_messages)

    conciliar_pares_exactos_cero(df, 'GRUPO_BANCO', 'FASE BANCO 1/5 (Cruce CERO)', log_messages)
    conciliar_pares_exactos_por_referencia(df, 'GRUPO_BANCO', 'FASE BANCO 2/5 (Pares por Referencia)', log_messages)
    cruzar_pares_simples(df, 'BANCO_A_BANCO', 'FASE BANCO 3/5 (Pares por Monto)', log_messages)
    cruzar_grupos_por_criterio(df, 'BANCO_A_BANCO', 'Referencia_Normalizada_Literal', 'BANCO_REF', 'FASE BANCO 4/5 (Grupos por Ref. Literal)', log_messages)
    cruzar_grupos_por_criterio(df, 'BANCO_A_BANCO', 'Fecha', 'BANCO_FECHA', 'FASE BANCO 5/5 (Grupos por Fecha)', log_messages)

    conciliar_pares_exactos_cero(df, 'GRUPO_REMESA', 'FASE REMESA 1/3 (Cruce CERO)', log_messages)
    cruzar_pares_simples(df, 'REMESA_GENERAL', 'FASE REMESA 2/3 (Pares por Monto)', log_messages)
    cruzar_grupos_por_criterio(df, 'REMESA_GENERAL', 'Referencia_Normalizada_Literal', 'REMESA_REF', 'FASE REMESA 3/3 (Grupos por Ref. Literal)', log_messages)

    # C. Fases Globales
    conciliar_grupos_globales_por_referencia(df, log_messages)
    conciliar_pares_globales_remanentes(df, log_messages)
    
    # D. Fase Inteligente de Subgrupos
    conciliar_grupos_complejos_remanentes(df, log_messages)

    # E. Fase Final
    conciliar_gran_total_final(df, log_messages)
    
    log_messages.append("\n--- PROCESO DE CONCILIACIÓN FINALIZADO ---")
    return df

def run_conciliation_devoluciones_proveedores(df, log_messages):
    """
    Contendrá la secuencia de llamadas de conciliación para Devoluciones a Proveedores.
    Por ahora, es un marcador de posición.
    """
    log_messages.append("\n--- INICIANDO LÓGICA DE DEVOLUCIONES A PROVEEDORES (AÚN NO IMPLEMENTADA) ---")
    df['Conciliado'] = False
    log_messages.append("Lógica para esta cuenta aún no implementada. Todos los movimientos quedan como pendientes.")
    return df

# ==============================================================================
# --- 4. DICCIONARIO CENTRAL DE ESTRATEGIAS ---
# ==============================================================================
# Este es el "cerebro" de la aplicación. Conecta la selección del usuario
# con la lógica de programación correcta que debe ejecutarse.
# Se define DESPUÉS de que todas las funciones han sido "aprendidas" por Python.
ESTRATEGIAS = {
    "111.04.1001 - Fondos en Tránsito": {
        "id": "fondos_transito",
        "funcion_principal": run_conciliation_fondos_en_transito,
        "label_actual": "Movimientos del mes (Fondos en Tránsito)",
        "label_anterior": "Saldos anteriores (Fondos en Tránsito)",
        "columnas_reporte": ['Asiento', 'Referencia', 'Fecha', 'Monto Dólar', 'Tasa', 'Bs.'],
        "nombre_hoja_excel": "111.04.1001"
    },
    "212.07.6009 - Devoluciones a Proveedores": {
        "id": "devoluciones_proveedores",
        "funcion_principal": run_conciliation_devoluciones_proveedores,
        "label_actual": "Reporte de Devoluciones (Proveedores)",
        "label_anterior": "Partidas pendientes (Proveedores)",
        "columnas_reporte": ['Asiento', 'Proveedor', 'Fecha', 'Monto Original', 'Monto Pagado', 'Saldo'], # Columnas de ejemplo
        "nombre_hoja_excel": "212.07.6009"
    }
}

# ==============================================================================
# --- 5. FUNCIÓN DE SEGURIDAD ---
# ==============================================================================
# Esta función actúa como el "guardia de seguridad" de la aplicación.

def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
         # Esta función se llama cada vez que el usuario escribe en el campo de contraseña.
        if st.session_state.get("password") == st.secrets.get("password"):
            st.session_state.password_correct = True
            if "password" in st.session_state:
                del st.session_state["password"] # Borramos la contraseña de la memoria por seguridad.
        else:
            st.session_state.password_correct = False
    
    # Si el usuario aún no se ha autenticado, mostramos el campo de contraseña
    if not st.session_state.password_correct:
        st.text_input("Contraseña", type="password", on_change=password_entered, key="password")
        # Si ya intentó y falló, mostramos un error.
        if "password" in st.session_state and not st.session_state.password_correct:
             st.error("😕 Contraseña incorrecta.")
        st.markdown("---")
        return False # El guardia no deja pasar.
    else:
        return True # El guardia deja pasar.

    
# ==============================================================================
# --- 6. FLUJO PRINCIPAL DE LA APLICACIÓN (INTERFAZ GRÁFICA) ---
# ==============================================================================
# A partir de aquí se construye lo que el usuario ve en la pantalla.
st.title('🤖 Herramienta de Conciliación Automática')

# Primero, llamamos al guardia de seguridad.
if check_password():
    
    # --- INICIO DEL ÁREA PROTEGIDA ---
    # Todo lo que está aquí dentro solo se mostrará si la contraseña es correcta.
    
    st.markdown("""
    Esta aplicación automatiza el proceso de conciliación de cuentas contables (ESPECIFICACIONES).
    """)
    
     # Creamos las listas de opciones para los menús desplegables.
    CASA_OPTIONS = ["FEBECA, C.A", "MAYOR BEVAL, C.A", "PRISMA, C.A", "FEBECA, C.A (QUINCALLA)"]
    CUENTA_OPTIONS = list(ESTRATEGIAS.keys())

    # Mostramos los menús desplegables en pantalla.
    casa_seleccionada = st.selectbox("**1. Seleccione la Empresa (Casa):**", CASA_OPTIONS)
    cuenta_seleccionada = st.selectbox("**2. Seleccione la Cuenta Contable:**", CUENTA_OPTIONS)
    
    # Obtenemos la estrategia seleccionada del diccionario para usar sus parámetros
    estrategia_actual = ESTRATEGIAS[cuenta_seleccionada]
     
    # Mostramos los cargadores de archivos, usando las etiquetas dinámicas de la estrategia.
    st.markdown("""
    **3. Cargue los Archivos de Excel (.xlsx):**
    *Asegúrese de que los datos se encuentren en la **primera hoja** del archivo y que los **encabezados estén en la primera fila**.*
    """)
    col1, col2 = st.columns(2)
    with col1:
        # CAMBIO: El tipo de archivo ahora es 'xlsx'.
        uploaded_actual = st.file_uploader(estrategia_actual["label_actual"], type="xlsx", key=f"actual_{estrategia_actual['id']}")
    with col2:
        # CAMBIO: El tipo de archivo ahora es 'xlsx'.
        uploaded_anterior = st.file_uploader(estrategia_actual["label_anterior"], type="xlsx", key=f"anterior_{estrategia_actual['id']}")


    # --- Lógica del Botón y Procesamiento ---
    # Este bloque solo se activa si ambos archivos han sido cargados.
    if uploaded_actual and uploaded_anterior:
        # Si el usuario hace clic en el botón...
        if st.button("▶️ Iniciar Conciliación", type="primary", use_container_width=True):
            with st.spinner('Procesando... por favor espere.'):
                log_messages = [] # Reiniciamos el registro.
            try:
                    # 1. Cargar y Limpiar Datos
                    df_full = cargar_y_limpiar_datos(uploaded_actual, uploaded_anterior, log_messages)
                
                    if df_full is not None:
                        # Llamada dinámica a la función de la estrategia seleccionada
                        df_full = estrategia_actual["funcion_principal"](df_full, log_messages)
                    
                        # 3. Preparar DataFrames para la salida
                        df_saldos_abiertos = df_full[~df_full['Conciliado']].copy()
                        df_conciliados = df_full[df_full['Conciliado']].copy()

                        # -- Archivo CSV de Saldos para el próximo mes --
                        columnas_originales = ['Asiento', 'Referencia', 'Fecha', 'Débito Bolivar', 'Crédito Bolivar', 'Débito Dolar', 'Crédito Dolar']
                        columnas_a_exportar = [col for col in columnas_originales if col in df_saldos_abiertos.columns]
                        df_saldos_a_exportar = df_saldos_abiertos[columnas_a_exportar].copy()
                        if 'Fecha' in df_saldos_a_exportar.columns:
                            df_saldos_a_exportar['Fecha'] = pd.to_datetime(df_saldos_a_exportar['Fecha'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')
                        for col in ['Débito Bolivar', 'Crédito Bolivar', 'Débito Dolar', 'Crédito Dolar']:
                            if col in df_saldos_a_exportar.columns:
                                df_saldos_a_exportar[col] = df_saldos_a_exportar[col].round(2).apply(lambda x: f"{x:.2f}".replace('.', ','))
                        csv_output = df_saldos_a_exportar.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')

                        # -- Archivo Excel del Reporte con Formato Original --
                        output_excel = BytesIO()
                        with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
                            workbook = writer.book
                        
                            # --- CÁLCULO DE LA FECHA PARA EL ENCABEZADO ---
                            fecha_maxima = df_full['Fecha'].max()
                            ultimo_dia_mes = fecha_maxima + pd.offsets.MonthEnd(0)
                            meses_es = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
                            texto_fecha_encabezado = f"PARA EL {ultimo_dia_mes.day} DE {meses_es[ultimo_dia_mes.month].upper()} DE {ultimo_dia_mes.year}"
                        
                            # --- DEFINICIÓN DE FORMATOS ---
                            formato_encabezado_empresa = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 14})
                            formato_encabezado_sub = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'font_size': 11})
                            formato_header_tabla = workbook.add_format({'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#D9EAD3', 'border': 1, 'align': 'center'})
                            formato_bs = workbook.add_format({'num_format': '#,##0.00'})
                            formato_usd = workbook.add_format({'num_format': '$#,##0.00'})
                            formato_tasa = workbook.add_format({'num_format': '#,##0.0000'})
                            formato_total_pend_text = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1})
                            formato_total_pend_bs = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1, 'num_format': '#,##0.00'})
                            formato_total_pend_usd = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1, 'num_format': '$#,##0.00'})
                            formato_total_conc_text = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1})
                            formato_total_conc_num_bs = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1, 'num_format': '#,##0.00'})
                            formato_total_conc_num_usd = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1, 'num_format': '$#,##0.00'})
                            formato_diferencia_text = workbook.add_format({'bold': True, 'border': 1})
                            formato_diferencia_num_bs = workbook.add_format({'bold': True, 'border': 1, 'num_format': '#,##0.00'})
                            formato_diferencia_num_usd = workbook.add_format({'bold': True, 'border': 1, 'num_format': '$#,##0.00'})
                            # (Añade aquí el resto de tus formatos para la hoja de conciliación)
                            
                            # --- HOJA 1: PENDIENTES ---
                            df_reporte_pendientes_prep = df_saldos_abiertos.copy()
                            df_reporte_pendientes_prep['Monto Dólar'] = df_reporte_pendientes_prep['Monto_USD']
                            df_reporte_pendientes_prep['Bs.'] = df_reporte_pendientes_prep['Monto_BS']
                            monto_dolar_abs = np.abs(df_reporte_pendientes_prep['Monto Dólar'])
                            monto_bolivar_abs = np.abs(df_reporte_pendientes_prep['Bs.'])
                            df_reporte_pendientes_prep['Tasa'] = np.where(monto_dolar_abs != 0, monto_bolivar_abs / monto_dolar_abs, np.nan)
                            columnas_reporte_pendientes = ['Asiento', 'Referencia', 'Fecha', 'Monto Dólar', 'Tasa', 'Bs.']
                            df_reporte_pendientes_final = df_reporte_pendientes_prep[columnas_reporte_pendientes].sort_values(by='Fecha')
                        
                            if 'Fecha' in df_reporte_pendientes_final.columns:
                                df_reporte_pendientes_final['Fecha'] = pd.to_datetime(df_reporte_pendientes_final['Fecha'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')
                            
                            nombre_hoja_pendientes = estrategia_actual.get("nombre_hoja_excel", "Pendientes")
                            # Escribimos los datos en la hoja
                            df_reporte_pendientes_final.to_excel(writer, sheet_name=nombre_hoja_pendientes, index=False, header=False, startrow=5)
                            worksheet_pendientes = writer.sheets[nombre_hoja_pendientes]
                            
                            # --- ESCRITURA DEL ENCABEZADO Y FORMATO DE LA HOJA 1 ---
                            num_cols = len(df_reporte_pendientes_final.columns)
                            if num_cols > 0:
                                worksheet_pendientes.merge_range(0, 0, 0, num_cols - 1, casa_seleccionada, formato_encabezado_empresa)
                                worksheet_pendientes.merge_range(1, 0, 1, num_cols - 1, f"ESPECIFICACION DE LA CUENTA {cuenta_seleccionada.split(' - ')[0]}", formato_encabezado_sub)
                                worksheet_pendientes.merge_range(2, 0, 2, num_cols - 1, texto_fecha_encabezado, formato_encabezado_sub)
    
                            for col_num, value in enumerate(df_reporte_pendientes_final.columns.values):
                                worksheet_pendientes.write(4, col_num, value, formato_header_tabla)

                            worksheet_pendientes.hide_gridlines(2)
                            worksheet_pendientes.set_column('A:A', 15); worksheet_pendientes.set_column('B:B', 60); worksheet_pendientes.set_column('C:C', 12)
                            worksheet_pendientes.set_column('D:D', 18, formato_usd); worksheet_pendientes.set_column('E:E', 12, formato_tasa); worksheet_pendientes.set_column('F:F', 18, formato_bs)
    
                            total_dolar_pend = df_reporte_pendientes_final['Monto Dólar'].sum()
                            total_bs_pend = df_reporte_pendientes_final['Bs.'].sum()

                            if not df_reporte_pendientes_final.empty:
                                fila_excel_sum = len(df_reporte_pendientes_final) + 5
                                worksheet_pendientes.write(fila_excel_sum, 0, 'SUMA', formato_total_pend_text)
                                worksheet_pendientes.write(fila_excel_sum, 1, 'TOTAL SALDOS ABIERTOS', formato_total_pend_text)
                                worksheet_pendientes.write(fila_excel_sum, 3, total_dolar_pend, formato_total_pend_usd)
                                worksheet_pendientes.write(fila_excel_sum, 5, total_bs_pend, formato_total_pend_bs)
                       
                            # --- HOJA 2: CONCILIACIÓN ---
                            df_reporte_conciliados_prep = df_conciliados.copy()
                            df_reporte_conciliados_prep.rename(columns={'Grupo_Conciliado': 'Conciliación'}, inplace=True)
                            
                            # Definimos exactamente qué columnas queremos y en qué orden
                            columnas_reporte_conciliados = ['Asiento', 'Referencia', 'Fecha', 'Débito Bolivar', 'Crédito Bolivar', 'Débito Dolar', 'Crédito Dolar', 'Conciliación']
                            df_reporte_conciliados_final = df_reporte_conciliados_prep.reindex(columns=columnas_reporte_conciliados).sort_values(by='Fecha')

                            
                            # FIX DE FORMATO DE FECHA PARA EXCEL
                            if 'Fecha' in df_reporte_conciliados_final.columns:
                                df_reporte_conciliados_final['Fecha'] = pd.to_datetime(df_reporte_conciliados_final['Fecha'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')
                            
                            # Escribimos los datos SIN encabezado, empezando en la fila 6 (índice 5)
                            df_reporte_conciliados_final.to_excel(writer, sheet_name='Conciliación', index=False, header=False, startrow=5)
                            
                            worksheet_conciliados = writer.sheets['Conciliación']
                                                               
                            # --- ESCRITURA DEL NUEVO ENCABEZADO PARA LA HOJA DE CONCILIACIÓN ---
                            num_cols_conc = len(df_reporte_conciliados_final.columns)
                            if num_cols_conc > 0:
                                worksheet_conciliados.merge_range(0, 0, 0, num_cols_conc - 1, casa_seleccionada, formato_encabezado_empresa)
                                worksheet_conciliados.merge_range(1, 0, 1, num_cols_conc - 1, f"MOVIMIENTOS CONCILIADOS DE LA CUENTA {cuenta_seleccionada.split(' - ')[0]}", formato_encabezado_sub)
                                worksheet_conciliados.merge_range(2, 0, 2, num_cols_conc - 1, texto_fecha_encabezado, formato_encabezado_sub)

                            for col_num, value in enumerate(df_reporte_conciliados_final.columns.values):
                                worksheet_conciliados.write(4, col_num, value, formato_header_tabla)
                                
                            # --- APLICACIÓN DE FORMATOS DE COLUMNAS ---                        
                            worksheet_conciliados.hide_gridlines(2)
                            worksheet_conciliados.set_column('A:A', 15); worksheet_conciliados.set_column('B:B', 60); worksheet_conciliados.set_column('C:C', 12)
                            worksheet_conciliados.set_column('D:E', 15, formato_bs); worksheet_conciliados.set_column('F:G', 15, formato_usd)
                            worksheet_conciliados.set_column('H:H', 35)

                            # --- LÓGICA DE TOTALES ---
                            total_debito_bs = df_reporte_conciliados_final['Débito Bolivar'].sum()
                            total_credito_bs = df_reporte_conciliados_final['Crédito Bolivar'].sum()
                            total_debito_usd = df_reporte_conciliados_final['Débito Dolar'].sum()
                            total_credito_usd = df_reporte_conciliados_final['Crédito Dolar'].sum()
                            diferencia_bs = total_debito_bs - total_credito_bs
                            diferencia_usd = total_debito_usd - total_credito_usd
                            
                            if not df_reporte_conciliados_final.empty:
                                fila_excel_sum = len(df_reporte_conciliados_final) + 5
                                worksheet_conciliados.write(fila_excel_sum, 0, 'SUMA', formato_total_conc_text)
                                worksheet_conciliados.write(fila_excel_sum, 1, 'TOTAL CRUZADOS', formato_total_conc_text)
                                worksheet_conciliados.write(fila_excel_sum, 3, total_debito_bs, formato_total_conc_num_bs)
                                worksheet_conciliados.write(fila_excel_sum, 4, total_credito_bs, formato_total_conc_num_bs)
                                worksheet_conciliados.write(fila_excel_sum, 5, total_debito_usd, formato_total_conc_num_usd)
                                worksheet_conciliados.write(fila_excel_sum, 6, total_credito_usd, formato_total_conc_num_usd)
                                
                                fila_excel_dif = fila_excel_sum + 1
                                worksheet_conciliados.write(fila_excel_dif, 0, 'DIFERENCIA', formato_diferencia_text)
                                worksheet_conciliados.write(fila_excel_dif, 1, 'SALDO NETO (DEBITO - CREDITO)', formato_diferencia_text)
                                worksheet_conciliados.write(fila_excel_dif, 3, diferencia_bs, formato_diferencia_num_bs)
                                worksheet_conciliados.write(fila_excel_dif, 5, diferencia_usd, formato_diferencia_num_usd)
                            # --- FIN DE LA ESCRITURA DEL EXCEL ---
                            
                            output_excel.seek(0)
                    
                            # --- GUARDADO EN SESSION_STATE ---
                            st.session_state.log_messages = log_messages
                            st.session_state.processing_complete = True
                            st.session_state.csv_output = csv_output
                            st.session_state.excel_output = output_excel
                            st.session_state.df_saldos_abiertos = df_saldos_abiertos
                            st.session_state.df_conciliados = df_conciliados
                    else:
                        st.session_state.processing_complete = False
                        
            except Exception as e:
                st.error(f"❌ Ocurrió un error crítico durante el proceso: {e}")
                import traceback
                st.code(traceback.format_exc())
                st.session_state.processing_complete = False

# --- Sección de Resultados ---
    # Esta sección solo se muestra si la "bandera" processing_complete es True.
if st.session_state.processing_complete:
    st.success("✅ ¡Conciliación completada con éxito!")
    
    pass 
    res_col1, res_col2 = st.columns(2)
    with res_col1:
        st.metric("Movimientos Conciliados", len(st.session_state.df_conciliados))
        st.download_button("⬇️ Descargar Reporte Completo (Excel)", st.session_state.excel_output, "reporte_conciliacion.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    with res_col2:
        st.metric("Saldos Abiertos (Pendientes)", len(st.session_state.df_saldos_abiertos))
        st.download_button("⬇️ Descargar Saldos para Próximo Mes (CSV)", st.session_state.csv_output, "saldos_para_proximo_mes.csv", "text/csv", use_container_width=True)

    st.info("**Instrucción de Ciclo Mensual:** Para el próximo mes, debe usar el archivo `saldos_para_proximo_mes.csv` como el archivo de 'saldos anteriores'.")

    with st.expander("Ver registro detallado del proceso"):
        st.text_area("Log", '\n'.join(st.session_state.log_messages), height=300)

    st.subheader("Previsualización de Saldos Pendientes")
    st.dataframe(st.session_state.df_saldos_abiertos)
    st.subheader("Previsualización de Movimientos Conciliados")
    st.dataframe(st.session_state.df_conciliados)
    st.success("✅ ¡Conciliación completada con éxito!")
    
    pass 
    res_col1, res_col2 = st.columns(2)
    with res_col1:
        st.metric("Movimientos Conciliados", len(st.session_state.df_conciliados))
        st.download_button("⬇️ Descargar Reporte Completo (Excel)", st.session_state.excel_output, "reporte_conciliacion.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, key="download_excel")
    with res_col2:
        st.metric("Saldos Abiertos (Pendientes)", len(st.session_state.df_saldos_abiertos))
        st.download_button("⬇️ Descargar Saldos para Próximo Mes (CSV)", st.session_state.csv_output, "saldos_para_proximo_mes.csv", "text/csv", use_container_width=True, key="download_csv")

    st.info("**Instrucción de Ciclo Mensual:** Para el próximo mes, debe usar el archivo `saldos_para_proximo_mes.csv` como el archivo de 'saldos anteriores'.")

    with st.expander("Ver registro detallado del proceso"):
        st.text_area("Log", '\n'.join(st.session_state.log_messages), height=300)

    st.subheader("Previsualización de Saldos Pendientes")
    st.dataframe(st.session_state.df_saldos_abiertos)
    st.subheader("Previsualización de Movimientos Conciliados")
    st.dataframe(st.session_state.df_conciliados)


