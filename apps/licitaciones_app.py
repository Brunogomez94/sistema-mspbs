# =============================================================================
# IMPORTS
# =============================================================================
import hashlib
import io
import json
import numpy as np
import openpyxl
import os
import pandas as pd
import streamlit as st
import time
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text

# =============================================================================
# CONFIGURACIÓN DE BASE DE DATOS
# =============================================================================

def get_db_config_licitaciones():
    """Obtiene configuración de BD desde secrets o variables de entorno"""
    import os
    try:
        # Intentar leer de Streamlit Secrets (para deploy en la nube)
        # Verificar que st.secrets existe y tiene db_config
        if hasattr(st, 'secrets'):
            try:
                secrets = st.secrets
                if 'db_config' in secrets:
                    return {
                        'host': secrets['db_config']['host'],
                        'port': int(secrets['db_config']['port']),
                        'name': secrets['db_config']['dbname'],
                        'user': secrets['db_config']['user'],
                        'password': secrets['db_config']['password']
                    }
            except (KeyError, AttributeError, TypeError):
                pass
    except Exception:
        pass
    
    # Fallback a variables de entorno o valores por defecto (desarrollo local)
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'name': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'Dggies12345')
    }

# Inicializar variables (se actualizarán cuando se use get_db_config_licitaciones)
DB_CONFIG_VAR = get_db_config_licitaciones() if 'st' in globals() else {
    'host': 'localhost',
    'port': '5432',
    'name': 'postgres',
    'user': 'postgres',
    'password': 'Dggies12345'
}

DB_HOST = DB_CONFIG_VAR['host']
DB_PORT = DB_CONFIG_VAR['port']
DB_NAME = DB_CONFIG_VAR['name']
DB_USER = DB_CONFIG_VAR['user']
DB_PASSWORD = DB_CONFIG_VAR['password']

# Intervalo de actualización automática (en minutos)
INTERVALO_ACTUALIZACION = 10

# Crear conexión a PostgreSQL (lazy initialization - solo cuando se necesite)
engine = None

def get_supabase_api_config():
    """Obtener configuración de Supabase para API REST"""
    try:
        if hasattr(st, 'secrets') and 'supabase' in st.secrets:
            return {
                'url': st.secrets['supabase']['url'],
                'key': st.secrets['supabase']['key']
            }
    except Exception:
        pass
    
    # Fallback a variables de entorno
    import os
    return {
        'url': os.getenv('SUPABASE_URL', ''),
        'key': os.getenv('SUPABASE_KEY', '')
    }

@st.cache_resource
def get_engine(_host=None, _port=None, _dbname=None, _user=None, _password=None, _api_url=None, _api_key=None):
    """
    Obtener engine de conexión - API REST primero, luego conexión directa
    
    Los parámetros con _ son para invalidar el cache cuando cambien los secrets
    
    Retorna:
    - Si API REST está disponible: dict con {'type': 'api_rest', 'client': supabase_client}
    - Si conexión directa funciona: SQLAlchemy engine
    - Si ambos fallan: None
    """
    from urllib.parse import quote_plus
    
    config = get_db_config_licitaciones()
    host = config['host']
    port = config['port']
    dbname = config['name']
    user = config['user']
    password = config['password']
    
    # Asegurar formato correcto de usuario para Supabase
    if '.supabase.co' in host:
        instance_id = host.split('.')[1] if host.startswith('db.') else host.split('.')[0]
        if not user.startswith(f'postgres.{instance_id}'):
            user = f"postgres.{instance_id}"
    
    # INTENTO 1: API REST de Supabase (preferido)
    api_config = get_supabase_api_config()
    if api_config['url'] and api_config['key']:
        try:
            from supabase import create_client, Client
            supabase: Client = create_client(api_config['url'], api_config['key'])
            
            # Probar conexión - si falla, no es crítico, seguimos intentando
            try:
                # Intentar con 'usuarios' (en public) primero, luego otras opciones
                for table_name in ['usuarios', 'oxigeno_usuarios', 'oxigeno.usuarios']:
                    try:
                        response = supabase.table(table_name).select("id").limit(1).execute()
                        # Si funciona, retornar API REST
                        return {'type': 'api_rest', 'client': supabase, 'config': api_config}
                    except Exception as e:
                        # Continuar con siguiente tabla
                        continue
                # Si ninguna tabla funcionó, aún así retornar API REST (puede que la tabla no exista aún)
                return {'type': 'api_rest', 'client': supabase, 'config': api_config}
            except Exception as e:
                # Si hay error en la prueba, aún así retornar API REST
                return {'type': 'api_rest', 'client': supabase, 'config': api_config}
        except ImportError:
            # Si no está instalado supabase, continuar con conexión directa
            pass
        except Exception as e:
            # Si hay error, continuar con conexión directa
            pass
    
    # INTENTO 2: Conexión directa
    try:
        password_escaped = quote_plus(password)
        conn_str = f"postgresql://{user}:{password_escaped}@{host}:{port}/{dbname}?sslmode=require"
        engine = create_engine(
            conn_str,
            connect_args={
                "client_encoding": "utf8",
                "connect_timeout": 10,
                "sslmode": "require"
            },
            pool_pre_ping=True
        )
        # Probar conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception:
        return None

def get_direct_connection():
    """
    Obtener conexión directa a PostgreSQL
    Si estamos usando API REST, intenta crear una conexión directa como fallback
    Retorna: SQLAlchemy engine o None si falla
    """
    from urllib.parse import quote_plus
    
    config = get_db_config_licitaciones()
    host = config['host']
    port = config['port']
    dbname = config['name']
    user = config['user']
    password = config['password']
    
    # Asegurar formato correcto de usuario para Supabase
    if '.supabase.co' in host:
        instance_id = host.split('.')[1] if host.startswith('db.') else host.split('.')[0]
        if not user.startswith(f'postgres.{instance_id}'):
            user = f"postgres.{instance_id}"
    
    try:
        password_escaped = quote_plus(password)
        conn_str = f"postgresql://{user}:{password_escaped}@{host}:{port}/{dbname}?sslmode=require"
        engine = create_engine(
            conn_str,
            connect_args={
                "client_encoding": "utf8",
                "connect_timeout": 10,
                "sslmode": "require"
            },
            pool_pre_ping=True
        )
        # Probar conexión
        with engine.connect() as test_conn:
            test_conn.execute(text("SELECT 1"))
        return engine
    except Exception:
        return None

def safe_get_engine():
    """
    Obtener engine seguro: si es API REST, intenta conexión directa como fallback
    Retorna: SQLAlchemy engine (nunca dict de API REST)
    IMPORTANTE: Esta función es solo para operaciones que REQUIEREN conexión directa.
    Para operaciones que pueden usar API REST (como login), usar get_engine() directamente.
    """
    engine = get_engine()
    
    if engine is None:
        return None
    
    # Si es API REST, intentar conexión directa
    if isinstance(engine, dict) and engine.get('type') == 'api_rest':
        direct_engine = get_direct_connection()
        if direct_engine:
            return direct_engine
        # Si falla conexión directa, retornar None
        # PERO NO mostrar error aquí, porque puede que la operación pueda usar API REST
        return None
    
    # Si ya es conexión directa, retornarla
    return engine

def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    """
    Ejecutar consulta SQL compatible con API REST y conexión directa
    
    Args:
        query: Query SQL como string
        params: Diccionario de parámetros (solo para conexión directa)
        fetch_one: Si True, retorna solo el primer resultado
        fetch_all: Si True, retorna todos los resultados
    
    Returns:
        Resultado de la consulta según fetch_one/fetch_all
    """
    engine = get_engine()
    
    if engine is None:
        st.error("No se pudo conectar a la base de datos")
        return None
    
    # Si es API REST
    if isinstance(engine, dict) and engine.get('type') == 'api_rest':
        client = engine['client']
        
        # Para API REST, necesitamos convertir SQL a llamadas de API
        # Por ahora, solo soportamos consultas SELECT simples a tablas específicas
        # Extraer tabla y condiciones del query
        query_lower = query.lower().strip()
        
        if query_lower.startswith('select'):
            # Intentar parsear SELECT básico
            # Ejemplo: SELECT id, username FROM usuarios WHERE cedula = :cedula
            try:
                # Extraer nombre de tabla (asumiendo formato: FROM schema.table)
                if 'from' in query_lower:
                    parts = query_lower.split('from')
                    if len(parts) > 1:
                        table_part = parts[1].split()[0].strip()
                        # Remover comillas si las hay y manejar esquemas
                        table_name = table_part.replace('"', '').replace("'", "")
                        
                        # En Supabase API REST, las tablas con esquemas se acceden SIN el esquema
                        # IMPORTANTE: La API REST solo accede a tablas en 'public' por defecto
                        # Para tablas en otros esquemas, necesitas configurar "Exposed schemas" en Supabase
                        # O mover la tabla al esquema 'public'
                        table_names_to_try = []
                        if '.' in table_name:
                            schema, table = table_name.split('.', 1)
                            # Si el esquema es 'public', usar solo el nombre de la tabla
                            if schema.lower() == 'public':
                                table_names_to_try.append(table)  # usuarios
                            else:
                                # Para otros esquemas, intentar primero solo el nombre de la tabla
                                table_names_to_try.append(table)  # usuarios
                                # Luego con guion bajo
                                table_names_to_try.append(f"{schema}_{table}")  # oxigeno_usuarios
                                # Y finalmente el nombre completo
                                table_names_to_try.append(table_name)  # oxigeno.usuarios
                        else:
                            table_names_to_try.append(table_name)
                        
                        # Extraer columnas
                        select_part = parts[0].replace('select', '').strip()
                        # Manejar COUNT(*) como caso especial
                        is_count = 'count(*)' in select_part.lower() or select_part.strip() == 'count(*)'
                        
                        if is_count:
                            # Para COUNT(*), necesitamos obtener todos los resultados y contar
                            columns_str = '*'  # Seleccionar todas las columnas para contar
                        else:
                            columns = [col.strip().replace('"', '').replace("'", "") for col in select_part.split(',')]
                            columns_str = ','.join(columns)
                        
                        # Aplicar filtros si hay WHERE
                        filters = {}
                        if 'where' in query_lower:
                            where_part = query_lower.split('where')[1].split('limit')[0] if 'limit' in query_lower else query_lower.split('where')[1]
                            # Parsear condiciones básicas (solo = y AND por ahora)
                            if '=' in where_part:
                                conditions = where_part.split('and')
                                for condition in conditions:
                                    if '=' in condition:
                                        parts_cond = condition.split('=')
                                        if len(parts_cond) == 2:
                                            col = parts_cond[0].strip().replace('"', '').replace("'", "")
                                            val = parts_cond[1].strip().replace('"', '').replace("'", "")
                                            
                                            # Si val es un parámetro (:param), usar params
                                            if val.startswith(':'):
                                                param_name = val[1:]
                                                if params and param_name in params:
                                                    val = params[param_name]
                                                else:
                                                    continue
                                            
                                            filters[col] = val
                        
                        # Intentar con cada variante del nombre de tabla
                        for table_try in table_names_to_try:
                            try:
                                supabase_query = client.table(table_try).select(columns_str)
                                
                                # Aplicar filtros
                                for col, val in filters.items():
                                    supabase_query = supabase_query.eq(col, val)
                                
                                # Ejecutar
                                response = supabase_query.execute()
                                
                                # Si es COUNT(*), retornar el número de resultados
                                if is_count:
                                    count = len(response.data) if response.data else 0
                                    if fetch_one:
                                        return {'count': count} if isinstance(fetch_one, bool) else count
                                    return count
                                
                                if fetch_one:
                                    return response.data[0] if response.data else None
                                elif fetch_all:
                                    return response.data
                                else:
                                    return response.data
                            except Exception as table_error:
                                # Si esta tabla no funciona, intentar la siguiente
                                continue
                        
                        # Si ninguna tabla funcionó
                        raise Exception(f"No se pudo acceder a la tabla {table_name}")
            except Exception as e:
                st.error(f"Error ejecutando query con API REST: {e}")
                st.info(f"Query original: {query[:200]}...")
                return None
        else:
            st.warning("API REST solo soporta consultas SELECT. Para INSERT/UPDATE/DELETE usa conexión directa.")
            return None
    
    # Conexión directa
    from sqlalchemy import text
    query_obj = text(query)
    
    with engine.connect() as conn:
        if params:
            result = conn.execute(query_obj, params)
        else:
            result = conn.execute(query_obj)
        
        if fetch_one:
            return result.fetchone()
        elif fetch_all:
            return result.fetchall()
        else:
            return result

def limpiar_dataframe_numerico(df):
    """
    Limpia un DataFrame convirtiendo cadenas vacías a None en columnas numéricas
    y luego a 0
    """
    df_clean = df.copy()
    
    # Identificar columnas que deberían ser numéricas
    for col in df_clean.columns:
        # Intentar convertir a numérico
        try:
            # Reemplazar cadenas vacías con None
            df_clean[col] = df_clean[col].replace(['', ' ', 'nan', 'NaN'], None)
            
            # Intentar conversión numérica
            df_temp = pd.to_numeric(df_clean[col], errors='coerce')
            
            # Si al menos el 50% son números válidos, es una columna numérica
            if df_temp.notna().sum() / len(df_temp) > 0.5:
                df_clean[col] = df_temp.fillna(0)
        except:
            # Si falla, dejar como texto y rellenar vacíos
            df_clean[col] = df_clean[col].fillna('')
    
    return df_clean

def generar_sugerencias_prellenado(analisis):
    """
    Genera sugerencias de datos basadas en el análisis del archivo
    """
    sugerencias = {}
    
    for nombre_hoja, info_hoja in analisis['hojas'].items():
        sugerencias_hoja = {}
        
        # Analizar contenido para generar sugerencias
        for col_info in info_hoja['columnas_info']:
            col_name = col_info['nombre']
            muestra_valores = col_info['muestra_valores']
            
            # Sugerencias basadas en el nombre de la columna
            if any(palabra in col_name.lower() for palabra in ['fecha', 'date']):
                sugerencias_hoja[col_name] = {
                    'tipo_sugerido': 'fecha',
                    'formato_detectado': 'Detectar automáticamente',
                    'accion_sugerida': 'Convertir a formato fecha estándar'
                }
            
            elif any(palabra in col_name.lower() for palabra in ['precio', 'monto', 'costo', 'valor']):
                sugerencias_hoja[col_name] = {
                    'tipo_sugerido': 'monetario',
                    'formato_detectado': 'Numérico',
                    'accion_sugerida': 'Formatear como moneda'
                }
            
            elif any(palabra in col_name.lower() for palabra in ['cantidad', 'numero', 'num']):
                sugerencias_hoja[col_name] = {
                    'tipo_sugerido': 'numérico',
                    'formato_detectado': 'Entero/Decimal',
                    'accion_sugerida': 'Validar formato numérico'
                }
            
            elif any(palabra in col_name.lower() for palabra in ['codigo', 'id', 'simese']):
                sugerencias_hoja[col_name] = {
                    'tipo_sugerido': 'identificador',
                    'formato_detectado': 'Texto/Alfanumérico',
                    'accion_sugerida': 'Mantener como texto'
                }
            
            # Sugerencias basadas en el contenido
            if muestra_valores:
                primer_valor = str(muestra_valores[0]) if muestra_valores else ""
                
                # Detectar patrones en los datos
                if primer_valor.replace('.', '').replace(',', '').isdigit():
                    if 'precio' not in col_name.lower():
                        sugerencias_hoja[col_name] = {
                            'tipo_sugerido': 'numérico',
                            'valor_ejemplo': primer_valor,
                            'accion_sugerida': 'Convertir a número'
                        }
        
        if sugerencias_hoja:
            sugerencias[nombre_hoja] = sugerencias_hoja
    
    return sugerencias

def mostrar_sugerencias_prellenado(sugerencias):
    """
    Muestra las sugerencias de prellenado en la interfaz
    """
    if not sugerencias:
        return
    
    st.subheader("💡 Sugerencias de Procesamiento")
    st.write("*Basadas en el análisis de contenido de tu archivo:*")
    
    for nombre_hoja, sugerencias_hoja in sugerencias.items():
        if sugerencias_hoja:
            with st.expander(f"🔮 Sugerencias para hoja: {nombre_hoja}", expanded=False):
                for columna, sugerencia in sugerencias_hoja.items():
                    st.write(f"**{columna}**")
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.write(f"• Tipo sugerido: {sugerencia.get('tipo_sugerido', 'N/A')}")
                        st.write(f"• Acción: {sugerencia.get('accion_sugerida', 'Ninguna')}")
                        if 'valor_ejemplo' in sugerencia:
                            st.write(f"• Ejemplo: {sugerencia['valor_ejemplo']}")
                    
                    with col2:
                        aplicar_sugerencia = st.checkbox(
                            f"Aplicar para {columna}",
                            key=f"sugerencia_{nombre_hoja}_{columna}",
                            help=f"Aplicar procesamiento sugerido para {columna}"
                        )

def pre_analizar_excel(archivo_excel):
    """
    Realiza un pre-análisis del archivo Excel para mostrar estructura y permitir mapeo
    """
    try:
        archivo_excel.seek(0)
        excel_data = pd.read_excel(archivo_excel, sheet_name=None, engine='openpyxl')
        
        analisis = {
            'hojas': {},
            'resumen': {
                'total_hojas': len(excel_data),
                'nombres_hojas': list(excel_data.keys()),
                'total_registros': 0
            }
        }
        
        for nombre_hoja, df in excel_data.items():
            # Limpiar el DataFrame
            df_limpio = limpiar_dataframe_numerico(df)
            
            # Análisis de cada hoja
            analisis_hoja = {
                'nombre': nombre_hoja,
                'filas': len(df_limpio),
                'columnas': len(df_limpio.columns),
                'columnas_info': [],
                'muestra_datos': df_limpio.head(3).to_dict('records') if len(df_limpio) > 0 else [],
                'tipos_datos': {}
            }
            
            # Analizar cada columna
            for col in df_limpio.columns:
                col_info = {
                    'nombre': col,
                    'tipo_detectado': str(df_limpio[col].dtype),
                    'valores_unicos': df_limpio[col].nunique() if len(df_limpio) > 0 else 0,
                    'valores_nulos': df_limpio[col].isnull().sum(),
                    'muestra_valores': df_limpio[col].dropna().head(3).tolist() if len(df_limpio) > 0 else []
                }
                analisis_hoja['columnas_info'].append(col_info)
                analisis_hoja['tipos_datos'][col] = col_info['tipo_detectado']
            
            analisis['hojas'][nombre_hoja] = analisis_hoja
            analisis['resumen']['total_registros'] += analisis_hoja['filas']
        
        return True, analisis
        
    except Exception as e:
        return False, f"Error en pre-análisis: {str(e)}"

def mostrar_interfaz_mapeo_columnas(analisis, archivo_excel):
    """
    Muestra la interfaz para mapear columnas y confirmar carga
    """
    st.subheader("🔍 Pre-Análisis del Archivo Excel")
    
    # Mostrar resumen general
    with st.expander("📊 Resumen General", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Hojas", analisis['resumen']['total_hojas'])
        with col2:
            st.metric("Total Registros", analisis['resumen']['total_registros'])
        with col3:
            st.metric("Hojas Encontradas", len(analisis['resumen']['nombres_hojas']))
        
        st.write("**Hojas detectadas:**", ", ".join(analisis['resumen']['nombres_hojas']))
    
    # Mostrar análisis de cada hoja
    configuracion_mapeo = {}
    
    # Diccionario de campos estándar para cada tipo de hoja
    campos_estandar = {
        'llamado': [
            'NUMERO DE LLAMADO', 'AÑO DEL LLAMADO', 'NOMBRE DEL LLAMADO', 
            'EMPRESA ADJUDICADA', 'NUMERO CONTRATO', 'FECHA CONTRATO', 
            'VIGENCIA CONTRATO', 'OBSERVACIONES'
        ],
        'ejecucion_general': [
            'SIMESE (PEDIDO)', 'SERVICIO BENEFICIARIO', 'SUBSERVICIO BENEFICIARIO',
            'CODIGO DE REACTIVOS / INSUMOS', 'DESCRIPCION DEL PRODUCTO',
            'CANTIDAD SOLICITADA', 'PRECIO UNITARIO', 'MONTO TOTAL'
        ],
        'orden_de_compra': [
            'SIMESE (PEDIDO)', 'N° ORDEN DE COMPRA', 'FECHA DE EMISION',
            'CODIGO DE REACTIVOS / INSUMOS', 'SERVICIO BENEFICIARIO',
            'LOTE', 'ITEM', 'CANTIDAD SOLICITADA', 'PRECIO UNITARIO'
        ],
            }
    
    for nombre_hoja, info_hoja in analisis['hojas'].items():
        with st.expander(f"📄 Hoja: {nombre_hoja} ({info_hoja['filas']} filas, {info_hoja['columnas']} columnas)", expanded=True):
            
            # Mostrar información de columnas
            st.write("**Columnas detectadas:**")
            
            # Crear tabla de información de columnas
            cols_df = pd.DataFrame(info_hoja['columnas_info'])
            if not cols_df.empty:
                cols_df = cols_df[['nombre', 'tipo_detectado', 'valores_unicos', 'valores_nulos']]
                cols_df.columns = ['Columna', 'Tipo', 'Valores Únicos', 'Valores Nulos']
                st.dataframe(cols_df, use_container_width=True)
            
            # Mostrar muestra de datos
            if info_hoja['muestra_datos']:
                st.write("**Muestra de datos (primeras 3 filas):**")
                muestra_df = pd.DataFrame(info_hoja['muestra_datos'])
                st.dataframe(muestra_df, use_container_width=True)
            
            # Sección de configuración/mapeo para esta hoja
            st.write("**Configuración de carga:**")
            
            # Opciones específicas por hoja
            if nombre_hoja in ['llamado', 'ejecucion_general', 'orden_de_compra', ]:
                st.success(f"✅ Hoja '{nombre_hoja}' reconocida como hoja estándar")
                incluir_hoja = st.checkbox(f"Incluir hoja '{nombre_hoja}' en la carga", value=True, key=f"incluir_{nombre_hoja}")
                
                # MAPEO DE COLUMNAS para hojas estándar
                if incluir_hoja:
                    st.write("**🔗 Mapeo de Columnas:**")
                    st.write("*Selecciona cómo mapear las columnas de tu Excel a los campos estándar:*")
                    
                    mapeo_columnas = {}
                    columnas_excel = [col['nombre'] for col in info_hoja['columnas_info']]
                    campos_hoja = campos_estandar.get(nombre_hoja, [])
                    
                    # Crear un mapeo automático inteligente basado en similitud de nombres
                    mapeo_automatico = {}
                    for campo_std in campos_hoja:
                        # Buscar coincidencias por similitud de nombres
                        mejor_match = None
                        mejor_score = 0
                        
                        for col_excel in columnas_excel:
                            # Calcular similitud simple
                            palabras_campo = campo_std.lower().split()
                            palabras_columna = col_excel.lower().split()
                            
                            coincidencias = sum(1 for palabra in palabras_campo if any(palabra in pc for pc in palabras_columna))
                            score = coincidencias / len(palabras_campo) if palabras_campo else 0
                            
                            if score > mejor_score and score > 0.3:  # Umbral de similitud
                                mejor_score = score
                                mejor_match = col_excel
                        
                        mapeo_automatico[campo_std] = mejor_match
                    
                    # Mostrar controles de mapeo
                    col1, col2 = st.columns([1, 1])
                    
                    for i, campo_std in enumerate(campos_hoja):
                        with col1 if i % 2 == 0 else col2:
                            # Opciones para el selectbox
                            opciones = ["-- No mapear --"] + columnas_excel
                            
                            # Valor por defecto basado en mapeo automático
                            valor_default = 0  # "-- No mapear --"
                            if mapeo_automatico.get(campo_std) in columnas_excel:
                                valor_default = opciones.index(mapeo_automatico[campo_std])
                            
                            columna_mapeada = st.selectbox(
                                f"**{campo_std}**",
                                options=opciones,
                                index=valor_default,
                                key=f"mapeo_{nombre_hoja}_{campo_std}",
                                help=f"Selecciona la columna de Excel que corresponde a '{campo_std}'"
                            )
                            
                            if columna_mapeada != "-- No mapear --":
                                mapeo_columnas[campo_std] = columna_mapeada
                    
                    # Mostrar resumen del mapeo
                    if mapeo_columnas:
                        st.write("**📋 Resumen del mapeo:**")
                        for campo, columna in mapeo_columnas.items():
                            st.write(f"• {campo} ← {columna}")
                    else:
                        st.warning("⚠️ No se han mapeado columnas para esta hoja")
                    
                    # Opciones adicionales para la hoja
                    col1, col2 = st.columns(2)
                    with col1:
                        omitir_primeras_filas = st.number_input(
                            f"Omitir primeras N filas en {nombre_hoja}",
                            min_value=0, max_value=10, value=0,
                            key=f"omitir_{nombre_hoja}",
                            help="Útil si las primeras filas contienen encabezados adicionales"
                        )
                    with col2:
                        solo_filas_con_datos = st.checkbox(
                            f"Solo filas con datos en {nombre_hoja}",
                            value=True,
                            key=f"filtrar_{nombre_hoja}",
                            help="Excluye filas completamente vacías"
                        )
                
            else:
                st.warning(f"⚠️ Hoja '{nombre_hoja}' no es una hoja estándar reconocida")
                incluir_hoja = st.checkbox(f"Incluir hoja '{nombre_hoja}' en la carga", value=False, key=f"incluir_{nombre_hoja}")
                
                if incluir_hoja:
                    st.info("Esta hoja se cargará tal como está, sin mapeo de columnas específico")
                    mapeo_columnas = {}
                    omitir_primeras_filas = 0
                    solo_filas_con_datos = True
            
            # Guardar configuración de esta hoja
            configuracion_mapeo[nombre_hoja] = {
                'incluir': incluir_hoja,
                'info': info_hoja,
                'mapeo_columnas': mapeo_columnas if incluir_hoja else {},
                'omitir_primeras_filas': omitir_primeras_filas if incluir_hoja else 0,
                'solo_filas_con_datos': solo_filas_con_datos if incluir_hoja else True
            }
    
    return configuracion_mapeo

# Función para configurar la tabla de usuarios
def formatear_columnas_tabla(df, mapeo_columnas=None):
    """Formatea las columnas de un DataFrame para mostrar al usuario final"""
    mapeo_default = {
        'id': 'ID',
        'ruc': 'RUC',
        'razon_social': 'Razón Social',
        'direccion': 'Dirección',
        'correo_electronico': 'Correo Electrónico',
        'telefono': 'Teléfono',
        'contacto_nombre': 'Contacto',
        'activo': 'Estado',
        'fecha_registro': 'Fecha Registro',
        'fecha_actualizacion': 'Fecha Actualización',
        'fecha_creacion': 'Fecha Creación',
        'fecha_carga': 'Fecha Carga',
        'nombre_archivo': 'Nombre Archivo',
        'esquema': 'Esquema',
        'usuario': 'Usuario',
        'estado': 'Estado',
        'usuario_id': 'Usuario ID',
        'cedula': 'Cédula',
        'username': 'Usuario',
        'nombre_completo': 'Nombre Completo',
        'role': 'Rol',
        'ultimo_cambio_password': 'Último Cambio Password',
        'numero_orden': 'Número Orden',
        'fecha_emision': 'Fecha Emisión',
        'servicio_beneficiario': 'Servicio Beneficiario',
        'simese': 'SIMESE',
        'cantidad_items': 'Cantidad Items',
        'monto_total': 'Monto Total',
        'lote': 'Lote',
        'item': 'Item',
        'codigo_insumo': 'Código Insumo',
        'codigo_servicio': 'Código Servicio',
        'descripcion': 'Descripción',
        'cantidad': 'Cantidad',
        'unidad_medida': 'Unidad Medida',
        'precio_unitario': 'Precio Unitario',
        'observaciones': 'Observaciones'
    }
    
    if mapeo_columnas:
        mapeo_default.update(mapeo_columnas)
    
    df_formateado = df.copy()
    nuevos_nombres = {}
    for col in df_formateado.columns:
        col_lower = col.lower()
        if col_lower in mapeo_default:
            nuevos_nombres[col] = mapeo_default[col_lower]
        else:
            nuevos_nombres[col] = col.replace('_', ' ').title()
    
    df_formateado = df_formateado.rename(columns=nuevos_nombres)
    return df_formateado

def configurar_tabla_usuarios():
    """Crea la tabla de usuarios si no existe - compatible con API REST y conexión directa"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        
        # Si es API REST, no podemos crear tablas directamente
        # Solo verificar si existe y mostrar mensaje
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            client = engine['client']
                # Intentar acceder a la tabla para verificar si existe
            try:
                # Probar diferentes formatos de nombre de tabla
                for table_name in ['usuarios', 'oxigeno_usuarios', 'oxigeno.usuarios']:
                    try:
                        response = client.table(table_name).select("id").limit(1).execute()
                        # Si funciona, la tabla existe
                        return
                    except:
                        continue
                # Si ninguna funcionó, la tabla no existe
                st.warning("""
                ⚠️ **La tabla de usuarios no existe en Supabase**
                
                **Para crear la tabla:**
                1. Ve a Supabase → SQL Editor
                2. Ejecuta el script: `CREAR_TABLA_USUARIOS_SUPABASE.sql`
                3. Luego ejecuta: `MOVER_TABLA_A_PUBLIC.sql` para moverla al esquema public
                
                O ejecuta directamente:
                ```sql
                CREATE TABLE IF NOT EXISTS public.usuarios (
                    id INTEGER NOT NULL,
                    cedula VARCHAR(20) NOT NULL,
                    username VARCHAR(50) NOT NULL,
                    password VARCHAR(200) NOT NULL,
                    nombre_completo VARCHAR(100) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ultimo_cambio_password TIMESTAMP,
                    CONSTRAINT usuarios_pkey PRIMARY KEY (id),
                    CONSTRAINT usuarios_cedula_unique UNIQUE (cedula),
                    CONSTRAINT usuarios_username_unique UNIQUE (username)
                );
                
                CREATE SEQUENCE IF NOT EXISTS usuarios_id_seq;
                ALTER TABLE public.usuarios ALTER COLUMN id SET DEFAULT nextval('usuarios_id_seq');
                ALTER SEQUENCE usuarios_id_seq OWNED BY public.usuarios.id;
                
                INSERT INTO public.usuarios (cedula, username, password, nombre_completo, role)
                SELECT '123456', 'admin', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918', 'Administrador', 'admin'
                WHERE NOT EXISTS (SELECT 1 FROM public.usuarios WHERE username = 'admin');
                ```
                
                La contraseña por defecto del admin es: `admin`
                """)
            except Exception as e:
                st.warning(f"No se pudo verificar la tabla: {e}")
            return
        
        # Conexión directa - crear tabla normalmente
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS oxigeno;
            """))
            conn.commit()
            
            # Crear tabla en public para que funcione con API REST
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.usuarios (
                    id INTEGER NOT NULL,
                    cedula VARCHAR(20) NOT NULL,
                    username VARCHAR(50) NOT NULL,
                    password VARCHAR(200) NOT NULL,
                    nombre_completo VARCHAR(100) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ultimo_cambio_password TIMESTAMP,
                    CONSTRAINT usuarios_pkey PRIMARY KEY (id),
                    CONSTRAINT usuarios_cedula_unique UNIQUE (cedula),
                    CONSTRAINT usuarios_username_unique UNIQUE (username)
                );
            """))
            conn.commit()
            
            # Crear secuencia si no existe
            conn.execute(text("""
                CREATE SEQUENCE IF NOT EXISTS usuarios_id_seq;
                ALTER TABLE public.usuarios ALTER COLUMN id SET DEFAULT nextval('usuarios_id_seq');
                ALTER SEQUENCE usuarios_id_seq OWNED BY public.usuarios.id;
            """))
            conn.commit()
            
            # Verificar si existe el usuario admin
            query = text("SELECT COUNT(*) FROM public.usuarios WHERE username = 'admin'")
            result = conn.execute(query)
            count = result.scalar()
            
            if count == 0:
                # Crear usuario admin por defecto
                password_hash = hashlib.sha256("admin".encode()).hexdigest()
                
                query = text("""
                    INSERT INTO public.usuarios (cedula, username, password, nombre_completo, role, ultimo_cambio_password)
                    VALUES ('123456', 'admin', :password, 'Administrador del Sistema', 'admin', CURRENT_TIMESTAMP)
                """)
                
                conn.execute(query, {'password': password_hash})
                print("Usuario admin creado")
                
            return True
    except Exception as e:
        print(f"Error configurando tabla de usuarios: {e}")
        return False

# Función para configurar la tabla de archivos cargados
def configurar_tabla_cargas():
    """Crea la tabla para registrar las cargas de archivos CSV"""
    try:
        engine = get_engine()
        if engine is None:
            return False
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Para API REST, las tablas se crean manualmente en Supabase
            return True
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS oxigeno;
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oxigeno.archivos_cargados (
                    id SERIAL PRIMARY KEY,
                    nombre_archivo VARCHAR(255) NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usuario_id INTEGER NOT NULL,
                    ubicacion_fisica VARCHAR(500),
                    estado VARCHAR(50) DEFAULT 'Activo',
                    FOREIGN KEY (usuario_id) REFERENCES oxigeno.usuarios(id)
                );
            """))
            
            return True
    except Exception as e:
        print(f"Error configurando tabla de archivos: {e}")
        return False

# Crear tabla para almacenar órdenes de compra
def configurar_tabla_ordenes_compra():
    """Crea la tabla de órdenes de compra si no existe y ejecuta migraciones automáticas"""
    try:
        engine = get_engine()
        if engine is None:
            return False
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Para API REST, las tablas se crean manualmente en Supabase
            return True
        with engine.connect() as conn:
            # Crear esquema si no existe
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS oxigeno;
            """))
            conn.commit()
            
            # Crear tabla ordenes_compra con estructura completa
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oxigeno.ordenes_compra (
                    id SERIAL PRIMARY KEY,
                    numero_orden VARCHAR(50) UNIQUE NOT NULL,
                    fecha_emision TIMESTAMP NOT NULL,
                    esquema VARCHAR(100) NOT NULL,
                    servicio_beneficiario VARCHAR(200),
                    simese VARCHAR(50),
                    usuario_id INTEGER NOT NULL,
                    estado VARCHAR(50) NOT NULL DEFAULT 'Emitida',
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (usuario_id) REFERENCES oxigeno.usuarios(id)
                );
            """))
            conn.commit()
            
            # Crear tabla items_orden_compra con estructura completa
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oxigeno.items_orden_compra (
                    id SERIAL PRIMARY KEY,
                    orden_compra_id INTEGER NOT NULL,
                    lote VARCHAR(50),
                    item VARCHAR(50),
                    codigo_insumo VARCHAR(100),
                    codigo_servicio VARCHAR(100),
                    descripcion TEXT,
                    cantidad NUMERIC(15, 2) NOT NULL,
                    unidad_medida VARCHAR(50),
                    precio_unitario NUMERIC(15, 2),
                    monto_total NUMERIC(15, 2),
                    observaciones TEXT,
                    FOREIGN KEY (orden_compra_id) REFERENCES oxigeno.ordenes_compra(id) ON DELETE CASCADE
                );
            """))
            conn.commit()
            
            # ========================================
            # MIGRACIÓN AUTOMÁTICA DE COLUMNAS
            # ========================================
            
            # Columnas necesarias en ordenes_compra
            columnas_ordenes_compra = {
                'esquema': 'VARCHAR(100)',
                'servicio_beneficiario': 'VARCHAR(200)',
                'simese': 'VARCHAR(50)',
                'estado': "VARCHAR(50) DEFAULT 'Emitida'",
            }
            
            for columna, tipo in columnas_ordenes_compra.items():
                try:
                    # Verificar si existe
                    check = conn.execute(text(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'oxigeno' 
                        AND table_name = 'ordenes_compra' 
                        AND column_name = '{columna}'
                    """))
                    
                    if not check.fetchone():
                        # Agregar columna
                        conn.execute(text(f"""
                            ALTER TABLE oxigeno.ordenes_compra 
                            ADD COLUMN {columna} {tipo}
                        """))
                        conn.commit()
                        print(f"✅ Columna '{columna}' agregada a ordenes_compra")
                except Exception as e:
                    print(f"⚠️ Error al agregar columna '{columna}' a ordenes_compra: {e}")
            
            # Columnas necesarias en items_orden_compra
            columnas_items = {
                'lote': 'VARCHAR(50)',
                'item': 'VARCHAR(50)',
                'codigo_insumo': 'VARCHAR(100)',
                'codigo_servicio': 'VARCHAR(100)',
                'descripcion': 'TEXT',
                'cantidad': 'NUMERIC(15, 2)',
                'unidad_medida': 'VARCHAR(50)',
                'precio_unitario': 'NUMERIC(15, 2)',
                'monto_total': 'NUMERIC(15, 2)',
                'observaciones': 'TEXT',
            }
            
            for columna, tipo in columnas_items.items():
                try:
                    # Verificar si existe
                    check = conn.execute(text(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'oxigeno' 
                        AND table_name = 'items_orden_compra' 
                        AND column_name = '{columna}'
                    """))
                    
                    if not check.fetchone():
                        # Agregar columna
                        conn.execute(text(f"""
                            ALTER TABLE oxigeno.items_orden_compra 
                            ADD COLUMN {columna} {tipo}
                        """))
                        conn.commit()
                        print(f"✅ Columna '{columna}' agregada a items_orden_compra")
                except Exception as e:
                    print(f"⚠️ Error al agregar columna '{columna}' a items_orden_compra: {e}")
            
            return True
    except Exception as e:
        print(f"Error configurando tablas de órdenes de compra: {e}")
        return False

def configurar_tabla_proveedores():
    """Crea la tabla de proveedores si no existe"""
    try:
        engine = get_engine()
        if engine is None:
            return False
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Para API REST, las tablas se crean manualmente en Supabase
            return True
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS oxigeno;
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oxigeno.proveedores (
                    id SERIAL PRIMARY KEY,
                    ruc VARCHAR(50) UNIQUE NOT NULL,
                    razon_social VARCHAR(200) NOT NULL,
                    direccion TEXT,
                    correo_electronico VARCHAR(100),
                    telefono VARCHAR(50),
                    contacto_nombre VARCHAR(100),
                    observaciones TEXT,
                    activo BOOLEAN DEFAULT TRUE,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            return True
    except Exception as e:
        print(f"Error configurando tabla de proveedores: {e}")
        return False

def configurar_tabla_auditoria():
   """Crea la tabla de auditoría para registrar todas las actividades del sistema"""
   try:
       engine = get_engine()
       if engine is None:
           return
       # Si es API REST, solo verificar que existe (las tablas ya están creadas)
       if isinstance(engine, dict) and engine.get('type') == 'api_rest':
           # Las tablas ya están creadas en Supabase, no hacer nada
           return
       # Usar conexión directa para crear tabla si no existe
       engine = safe_get_engine()
       if engine is None:
           return
       with engine.connect() as conn:
           conn.execute(text("""
               CREATE SCHEMA IF NOT EXISTS oxigeno;
           """))
           
           conn.execute(text("""
               CREATE TABLE IF NOT EXISTS oxigeno.auditoria (
                   id SERIAL PRIMARY KEY,
                   usuario_id INTEGER NOT NULL,
                   usuario_nombre VARCHAR(100) NOT NULL,
                   accion VARCHAR(100) NOT NULL,
                   modulo VARCHAR(50) NOT NULL,
                   descripcion TEXT NOT NULL,
                   detalles JSONB,
                   ip_address VARCHAR(45),
                   user_agent TEXT,
                   fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   esquema_afectado VARCHAR(100),
                   registro_afectado_id INTEGER,
                   valores_anteriores JSONB,
                   valores_nuevos JSONB,
                   FOREIGN KEY (usuario_id) REFERENCES oxigeno.usuarios(id)
               );
           """))
           
           conn.execute(text("""
               CREATE INDEX IF NOT EXISTS idx_auditoria_usuario ON oxigeno.auditoria(usuario_id);
               CREATE INDEX IF NOT EXISTS idx_auditoria_fecha ON oxigeno.auditoria(fecha_hora DESC);
               CREATE INDEX IF NOT EXISTS idx_auditoria_modulo ON oxigeno.auditoria(modulo);
               CREATE INDEX IF NOT EXISTS idx_auditoria_accion ON oxigeno.auditoria(accion);
           """))
           
           return True
   except Exception as e:
       print(f"Error configurando tabla de auditoría: {e}")
       return False

def registrar_actividad(accion, modulo, descripcion, detalles=None, esquema_afectado=None, 
                      registro_afectado_id=None, valores_anteriores=None, valores_nuevos=None):
   """Registra una actividad en el sistema de auditoría"""
   try:
       if 'user_id' not in st.session_state or 'user_name' not in st.session_state:
           return False
       
       engine = get_engine()
       if engine is None:
           return False
       
       # Para API REST, la auditoría se puede implementar más adelante
       if isinstance(engine, dict) and engine.get('type') == 'api_rest':
           # Por ahora, solo registrar en logs
           print(f"Auditoría (API REST): {accion} - {modulo} - {descripcion}")
           return True
           
       with engine.connect() as conn:
           query = text("""
               INSERT INTO oxigeno.auditoria 
               (usuario_id, usuario_nombre, accion, modulo, descripcion, detalles, 
                esquema_afectado, registro_afectado_id, valores_anteriores, valores_nuevos)
               VALUES (:usuario_id, :usuario_nombre, :accion, :modulo, :descripcion, :detalles,
                       :esquema_afectado, :registro_afectado_id, :valores_anteriores, :valores_nuevos)
           """)
           
           conn.execute(query, {
               'usuario_id': st.session_state.user_id,
               'usuario_nombre': st.session_state.user_name,
               'accion': accion,
               'modulo': modulo,
               'descripcion': descripcion,
               'detalles': json.dumps(detalles) if detalles else None,
               'esquema_afectado': esquema_afectado,
               'registro_afectado_id': registro_afectado_id,
               'valores_anteriores': json.dumps(valores_anteriores) if valores_anteriores else None,
               'valores_nuevos': json.dumps(valores_nuevos) if valores_nuevos else None
           })
           
           return True
   except Exception as e:
       print(f"Error registrando actividad en auditoría: {e}")
       return False

def obtener_historial_actividades(limite=100, usuario_id=None, modulo=None, accion=None, fecha_desde=None, fecha_hasta=None):
    """Obtiene el historial de actividades con filtros opcionales"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return []
        
        # Para API REST, la auditoría se puede implementar más adelante
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Por ahora, retornar lista vacía
            return []
        
        # Usar conexión directa para auditoría
        engine = safe_get_engine()
        if engine is None:
            return []
        
        with engine.connect() as conn:
            query_base = """
                SELECT a.id, a.usuario_nombre, a.accion, a.modulo, a.descripcion, 
                       a.fecha_hora, a.esquema_afectado, a.detalles,
                       a.valores_anteriores, a.valores_nuevos
                FROM oxigeno.auditoria a
                WHERE 1=1
            """
            params = {}
            
            if usuario_id:
                query_base += " AND a.usuario_id = :usuario_id"
                params['usuario_id'] = usuario_id
            
            if modulo:
                query_base += " AND a.modulo = :modulo"
                params['modulo'] = modulo
            
            if accion:
                query_base += " AND a.accion = :accion"
                params['accion'] = accion
            
            if fecha_desde:
                query_base += " AND a.fecha_hora >= :fecha_desde"
                params['fecha_desde'] = fecha_desde
            
            if fecha_hasta:
                query_base += " AND a.fecha_hora <= :fecha_hasta"
                params['fecha_hasta'] = fecha_hasta
            
            query_base += " ORDER BY a.fecha_hora DESC LIMIT :limite"
            params['limite'] = limite
            
            query = text(query_base)
            result = conn.execute(query, params)
            
            actividades = []
            for row in result:
                actividades.append({
                    'id': row[0],
                    'usuario': row[1],
                    'accion': row[2],
                    'modulo': row[3],
                    'descripcion': row[4],
                    'fecha_hora': row[5],
                    'esquema_afectado': row[6],
                    'detalles': json.loads(row[7]) if row[7] else None,
                    'valores_anteriores': json.loads(row[8]) if row[8] else None,
                    'valores_nuevos': json.loads(row[9]) if row[9] else None
                })
            
            return actividades
    except Exception as e:
        st.error(f"Error obteniendo historial de actividades: {e}")
        return []

def numero_a_letras(numero):
    """Convierte un número a su representación en letras (simplificada)"""
    millones = int(numero / 1000000)
    miles = int((numero % 1000000) / 1000)
    unidades = int(numero % 1000)
    
    texto = ""
    
    if millones > 0:
        if millones == 1:
            texto += "UN MILLÓN "
        else:
            texto += f"{millones} MILLONES "
    
    if miles > 0:
        texto += f"{miles} MIL "
    
    if unidades > 0 or (millones == 0 and miles == 0):
        texto += f"{unidades} "
    
    return texto.strip()

def iniciar_actualizacion_automatica():
    """
    ⚠️ FUNCIÓN DESACTIVADA
    Configura la actualización automática de datos
    Esta función causaba consumo excesivo de recursos en el navegador
    """
    pass
    # # Verificar si ya se ha iniciado la actualización
    # if 'ultima_actualizacion' not in st.session_state:
    #     st.session_state.ultima_actualizacion = datetime.now()
    # else:
    #     # Verificar si es hora de actualizar
    #     tiempo_transcurrido = (datetime.now() - st.session_state.ultima_actualizacion).total_seconds() / 60
    #     if tiempo_transcurrido >= INTERVALO_ACTUALIZACION:
    #         st.session_state.ultima_actualizacion = datetime.now()
    #         st.info("Actualizando datos...")
    #         # Aquí puedes poner el código para refrescar los datos
    #         time.sleep(1)
    #         st.rerun()

def obtener_esquemas_postgres():
    """Obtiene la lista de esquemas existentes en PostgreSQL, excluyendo esquemas del sistema"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return []
        
        # Para API REST, no podemos obtener esquemas directamente
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Retornar lista vacía o esquemas conocidos
            return []
        
        # Usar conexión directa para obtener esquemas
        engine = safe_get_engine()
        if engine is None:
            return []
        
        with engine.connect() as conn:
            query = text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public', 'reactivos_py', 'siciap', 'pg_toast')
                AND schema_name NOT LIKE 'pg_temp_%'
                AND schema_name NOT LIKE 'pg_toast_temp_%'
                ORDER BY schema_name
            """)
            
            result = conn.execute(query)
            
            esquemas = [row[0] for row in result]
            return esquemas
    except Exception as e:
        st.error(f"Error obteniendo esquemas: {e}")
        return []

import openpyxl
import pandas as pd

def cargar_archivo_con_configuracion(archivo_excel, nombre_archivo, esquema, configuracion_mapeo, limpiar_datos, validar_estructura, datos_formulario=None):
    """
    Carga archivo Excel con configuración personalizada basada en el pre-análisis
    """
    try:
        # Leer el contenido del archivo Excel
        archivo_excel.seek(0)
        excel_data = pd.read_excel(archivo_excel, sheet_name=None, engine='openpyxl')
        
        # Filtrar solo las hojas incluidas en la configuración
        hojas_a_procesar = {hoja: df for hoja, df in excel_data.items() 
                          if hoja in configuracion_mapeo and configuracion_mapeo[hoja]['incluir']}
        
        if not hojas_a_procesar:
            return False, "No se seleccionaron hojas para procesar"
        
        # Validar hojas requeridas
        hojas_requeridas = ['llamado', 'ejecucion_general', 'orden_de_compra', ]
        hojas_faltantes = [hoja for hoja in hojas_requeridas if hoja not in hojas_a_procesar]
        
        if hojas_faltantes:
            return False, f"Faltan hojas requeridas: {', '.join(hojas_faltantes)}"
        
        # Formatear el nombre del esquema
        esquema_formateado = esquema.strip().lower().replace(' ', '_').replace('-', '_')
        
        # Registrar actividad de carga
        registrar_actividad(
            accion="Inicio carga Excel con configuración",
            modulo="Carga de Archivos",
            descripcion=f"Iniciando carga de {nombre_archivo} con {len(hojas_a_procesar)} hojas",
            detalles={
                "archivo": nombre_archivo,
                "esquema": esquema_formateado,
                "hojas_incluidas": list(hojas_a_procesar.keys()),
                "limpiar_datos": limpiar_datos,
                "validar_estructura": validar_estructura
            }
        )
        
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return False, "No se pudo conectar a la base de datos"
        
        # Si es API REST, intentar obtener conexión directa como fallback
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Intentar conexión directa para operaciones complejas
            from urllib.parse import quote_plus
            config = get_db_config_licitaciones()
            host = config['host']
            port = config['port']
            dbname = config['name']
            user = config['user']
            password = config['password']
            
            # Asegurar formato correcto de usuario para Supabase
            if '.supabase.co' in host:
                instance_id = host.split('.')[1] if host.startswith('db.') else host.split('.')[0]
                if not user.startswith(f'postgres.{instance_id}'):
                    user = f"postgres.{instance_id}"
            
            try:
                password_escaped = quote_plus(password)
                conn_str = f"postgresql://{user}:{password_escaped}@{host}:{port}/{dbname}?sslmode=require"
                engine = create_engine(
                    conn_str,
                    connect_args={
                        "client_encoding": "utf8",
                        "connect_timeout": 10,
                        "sslmode": "require"
                    },
                    pool_pre_ping=True
                )
                # Probar conexión
                with engine.connect() as test_conn:
                    test_conn.execute(text("SELECT 1"))
            except Exception as e:
                st.error(f"La carga de archivos Excel requiere conexión directa. Error: {e}")
                return False, f"No se pudo establecer conexión directa: {e}"
        
        with engine.connect() as conn:
            # Iniciar transacción
            trans = conn.begin()
            try:
                # 1. Crear el esquema si no existe
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{esquema_formateado}"'))
                
                # 2. Procesar cada hoja incluida
                resultados_carga = {}
                
                for nombre_hoja, df in hojas_a_procesar.items():
                    try:
                        config_hoja = configuracion_mapeo[nombre_hoja]
                        
                        # Aplicar configuraciones específicas de la hoja
                        if config_hoja.get('omitir_primeras_filas', 0) > 0:
                            df = df.iloc[config_hoja['omitir_primeras_filas']:].reset_index(drop=True)
                        
                        if config_hoja.get('solo_filas_con_datos', True):
                            df = df.dropna(how='all')
                        
                        # Aplicar mapeo de columnas si existe
                        mapeo_columnas = config_hoja.get('mapeo_columnas', {})
                        if mapeo_columnas:
                            # Crear DataFrame con columnas mapeadas
                            df_mapeado = pd.DataFrame()
                            
                            for campo_std, col_excel in mapeo_columnas.items():
                                if col_excel in df.columns:
                                    df_mapeado[campo_std] = df[col_excel]
                                else:
                                    # Si no se encuentra la columna, crear con valores vacíos
                                    df_mapeado[campo_std] = None
                            
                            # Agregar columnas no mapeadas del Excel original (opcional)
                            for col in df.columns:
                                if col not in mapeo_columnas.values():
                                    # Agregar con prefijo para identificar que no está mapeada
                                    df_mapeado[f"ORIGINAL_{col}"] = df[col]
                            
                            df = df_mapeado
                        
                        # Aplicar limpieza si está habilitada
                        if limpiar_datos:
                            df = limpiar_dataframe_numerico(df)
                        
                        # Validar estructura si está habilitada
                        if validar_estructura:
                            # Validaciones básicas
                            if len(df) == 0:
                                resultados_carga[nombre_hoja] = f"⚠️ Hoja vacía después del procesamiento"
                                continue
                            
                            if len(df.columns) == 0:
                                resultados_carga[nombre_hoja] = f"❌ Sin columnas después del mapeo"
                                continue
                        
                        # Cargar según el tipo de hoja
                        if nombre_hoja == 'llamado':
                            crear_tabla_llamado(conn, esquema_formateado, df)
                        elif nombre_hoja == 'ejecucion_general':
                            crear_tabla_ejecucion_general(conn, esquema_formateado, df)
                        elif nombre_hoja == 'orden_de_compra':
                            crear_tabla_orden_compra(conn, esquema_formateado, df)
                        else:
                            # Para hojas no estándar, crear tabla genérica
                            df.to_sql(nombre_hoja.lower().replace(' ', '_'), conn, 
                                    schema=esquema_formateado, if_exists='replace', 
                                    index=False, method=None)
                        
                        # Registrar resultado con información del mapeo
                        info_resultado = f"✅ {len(df)} registros"
                        if mapeo_columnas:
                            info_resultado += f" (mapeadas: {len(mapeo_columnas)} columnas)"
                        
                        resultados_carga[nombre_hoja] = info_resultado
                        
                    except Exception as e:
                        resultados_carga[nombre_hoja] = f"❌ Error: {str(e)[:100]}"
                        # Continuar con las otras hojas en lugar de fallar completamente
                        continue
                
                # 3. Guardar registro del archivo en la tabla de control
                archivo_excel.seek(0)
                contenido_bytes = archivo_excel.read()
                
                query = text("""
                    INSERT INTO oxigeno.archivos_cargados 
                    (nombre_archivo, esquema, usuario_id, contenido_original)
                    VALUES (:nombre, :esquema, :usuario_id, :contenido)
                    RETURNING id
                """)
                
                result = conn.execute(query, {
                    'nombre': nombre_archivo,
                    'esquema': esquema_formateado,
                    'usuario_id': st.session_state.user_id,
                    'contenido': contenido_bytes
                })
                
                archivo_id = result.scalar()
                
                # Confirmar transacción
                trans.commit()
                
                # Registrar actividad exitosa
                registrar_actividad(
                    accion="Carga Excel exitosa",
                    modulo="Carga de Archivos",
                    descripcion=f"Archivo {nombre_archivo} cargado exitosamente",
                    detalles={
                        "archivo_id": archivo_id,
                        "esquema": esquema_formateado,
                        "resultados": resultados_carga
                    }
                )
                
                # Crear mensaje de resultado
                mensaje_exitoso = [f"✅ Archivo '{nombre_archivo}' cargado exitosamente en esquema '{esquema_formateado}'"]
                mensaje_exitoso.append("📊 Resultados por hoja:")
                for hoja, resultado in resultados_carga.items():
                    mensaje_exitoso.append(f"   • {hoja}: {resultado}")
                
                return True, "\n".join(mensaje_exitoso)
                
            except Exception as e:
                # Rollback en caso de error
                trans.rollback()
                
                registrar_actividad(
                    accion="Error carga Excel",
                    modulo="Carga de Archivos",
                    descripcion=f"Error cargando {nombre_archivo}: {str(e)}",
                    detalles={
                        "error": str(e),
                        "esquema": esquema_formateado
                    }
                )
                
                return False, f"Error durante la carga: {str(e)}"
                
    except Exception as e:
        return False, f"Error procesando archivo: {str(e)}"

def cargar_archivo_a_postgres(archivo_excel, nombre_archivo, esquema, empresa_para_tablas=None, datos_formulario=None):
    """
    Carga un archivo Excel directamente a PostgreSQL creando las 4 tablas del esquema
    OPTIMIZADO: Con chunks, barra de progreso y validación
    
    Args:
        archivo_excel: Archivo Excel subido
        nombre_archivo: Nombre del archivo
        esquema: Nombre del esquema donde se creará
        empresa_para_tablas: (Opcional) Prefijo de empresa para las tablas
        datos_formulario: (Opcional) Diccionario con datos adicionales del formulario
    """
    try:
        # =====================================================
        # PASO 1: VALIDACIÓN PRE-CARGA
        # =====================================================
        archivo_excel.seek(0)
        contenido_bytes = archivo_excel.read()
        tamaño_mb = len(contenido_bytes) / (1024 * 1024)
        
        st.info(f"📊 Tamaño del archivo: {tamaño_mb:.2f} MB")
        
        # Advertencia para archivos grandes
        if tamaño_mb > 5:
            st.warning(f"⚠️ Archivo grande ({tamaño_mb:.1f} MB) - Esto puede tardar varios minutos")
            tiempo_estimado = int(tamaño_mb * 10)  # ~10 seg por MB
            st.info(f"⏱️ Tiempo estimado: ~{tiempo_estimado} segundos")
        
        # =====================================================
        # PASO 2: LEER EXCEL CON FEEDBACK
        # =====================================================
        archivo_excel.seek(0)  # Reiniciar puntero
        
        with st.spinner("📖 Leyendo archivo Excel..."):
            # Leer todas las hojas del Excel
            excel_data = pd.read_excel(archivo_excel, sheet_name=None, engine='openpyxl')
        
        st.success("✅ Archivo Excel leído correctamente")
        
        # Verificar que existan las hojas necesarias
        hojas_requeridas = ['llamado', 'ejecucion_general', 'orden_de_compra']
        hojas_encontradas = list(excel_data.keys())
        
        # Validar hojas
        for hoja in hojas_requeridas:
            if hoja not in hojas_encontradas:
                return False, f"Error: Falta la hoja '{hoja}' en el archivo Excel. Hojas encontradas: {hojas_encontradas}"
        
        # Mostrar información de las hojas
        total_filas = sum(len(df) for df in excel_data.values())
        st.info(f"📋 Total de registros a cargar: {total_filas:,}")
        for hoja, df in excel_data.items():
            st.caption(f"   • {hoja}: {len(df):,} registros")
        
        # =====================================================
        # PASO 3: FORMATEAR ESQUEMA
        # =====================================================
        esquema_formateado = esquema.strip().lower().replace(' ', '_').replace('-', '_')
        st.info(f"🗂️ Esquema de destino: {esquema_formateado}")
        
        # =====================================================
        # PASO 4: CARGAR A BASE DE DATOS CON PROGRESO
        # =====================================================
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return False, "No se pudo conectar a la base de datos"
        
        # Si es API REST, intentar obtener conexión directa como fallback
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Intentar conexión directa para operaciones complejas
            from urllib.parse import quote_plus
            config = get_db_config_licitaciones()
            host = config['host']
            port = config['port']
            dbname = config['name']
            user = config['user']
            password = config['password']
            
            # Asegurar formato correcto de usuario para Supabase
            if '.supabase.co' in host:
                instance_id = host.split('.')[1] if host.startswith('db.') else host.split('.')[0]
                if not user.startswith(f'postgres.{instance_id}'):
                    user = f"postgres.{instance_id}"
            
            try:
                password_escaped = quote_plus(password)
                conn_str = f"postgresql://{user}:{password_escaped}@{host}:{port}/{dbname}?sslmode=require"
                engine = create_engine(
                    conn_str,
                    connect_args={
                        "client_encoding": "utf8",
                        "connect_timeout": 10,
                        "sslmode": "require"
                    },
                    pool_pre_ping=True
                )
                # Probar conexión
                with engine.connect() as test_conn:
                    test_conn.execute(text("SELECT 1"))
            except Exception as e:
                st.error(f"La carga de archivos Excel requiere conexión directa. Error: {e}")
                return False, f"No se pudo establecer conexión directa: {e}"
        
        with engine.connect() as conn:
            # Iniciar transacción
            trans = conn.begin()
            try:
                # Barra de progreso principal
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # 1. Crear el esquema (10%)
                status_text.text("🔨 Creando esquema...")
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{esquema_formateado}"'))
                progress_bar.progress(0.1)
                
                # 2. Cargar tabla 'llamado' (30%)
                status_text.text("📋 Cargando tabla 'llamado'...")
                df_llamado = excel_data['llamado']
                crear_tabla_llamado(conn, esquema_formateado, df_llamado)
                progress_bar.progress(0.3)
                
                # 3. Cargar tabla 'ejecucion_general' (60%)
                status_text.text(f"📊 Cargando tabla 'ejecucion_general' ({len(excel_data['ejecucion_general']):,} registros)...")
                df_ejecucion_general = excel_data['ejecucion_general']
                crear_tabla_ejecucion_general(conn, esquema_formateado, df_ejecucion_general)
                progress_bar.progress(0.6)
                
                # 4. Cargar tabla 'orden_de_compra' (85%)
                status_text.text(f"🛒 Cargando tabla 'orden_de_compra' ({len(excel_data['orden_de_compra']):,} registros)...")
                df_orden_compra = excel_data['orden_de_compra']
                crear_tabla_orden_compra(conn, esquema_formateado, df_orden_compra)
                progress_bar.progress(0.85)
                
                # 5. Guardar registro del archivo en la tabla de control (95%)
                status_text.text("💾 Guardando registro del archivo...")
                query = text("""
                    INSERT INTO oxigeno.archivos_cargados 
                    (nombre_archivo, esquema, usuario_id)
                    VALUES (:nombre, :esquema, :usuario_id)
                    RETURNING id
                """)
                
                result = conn.execute(query, {
                    'nombre': nombre_archivo,
                    'esquema': esquema_formateado,
                    'usuario_id': st.session_state.user_id
                })
                
                archivo_id = result.scalar()
                progress_bar.progress(0.95)
                
                # 6. Confirmar transacción (100%)
                status_text.text("✅ Confirmando cambios...")
                trans.commit()
                progress_bar.progress(1.0)
                
                # Limpiar elementos de progreso
                time.sleep(0.5)
                status_text.empty()
                progress_bar.empty()
                
                mensaje_exito = f"✅ Archivo Excel cargado correctamente en esquema '{esquema_formateado}'"
                if empresa_para_tablas:
                    mensaje_exito += f" para empresa '{empresa_para_tablas}'"
                mensaje_exito += f"\n📁 ID de archivo: {archivo_id}\n📊 Total de registros: {total_filas:,}"
                
                return True, mensaje_exito
                
            except Exception as e:
                # Revertir transacción en caso de error
                status_text.text("❌ Error detectado, revirtiendo cambios...")
                trans.rollback()
                progress_bar.empty()
                status_text.empty()
                raise e
                
    except Exception as e:
        return False, f"Error al cargar archivo Excel: {e}"

def crear_tabla_llamado(conn, esquema, df):
    """Crea la tabla llamado con su estructura específica y acepta todas las variantes de columnas"""
    # Crear tabla con TODAS las columnas (incluida CODIGO_LLAMADO)
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{esquema}".llamado (
        "CODIGO_LLAMADO" VARCHAR(50),
        "I_D" INTEGER,
        "MODALIDAD" VARCHAR(20),
        "NUMERO_DE_LLAMADO" INTEGER,
        "AÑO_DEL_LLAMADO" INTEGER,
        "NOMBRE_DEL_LLAMADO" TEXT,
        "EMPRESA_ADJUDICADA" TEXT,
        "RUC" VARCHAR(50),
        "FECHA_FIRMA_CONTRATO" DATE,
        "NUMERO_CONTRATO" VARCHAR(100),
        "VIGENCIA_CONTRATO" TEXT,
        "Fecha_de_Inicio_de_Poliza" DATE,
        "Fecha_de_Finalizacion_de_Poliza" DATE
    )
    '''
    conn.execute(text(create_sql))
    
    # MAPEO: Normalizar nombres de columnas con ESPACIOS a nombres con GUIONES
    mapeo_columnas = {}
    for col in df.columns:
        # Normalizar: reemplazar espacios por guiones bajos
        col_normalizada = col.replace(' ', '_')
        mapeo_columnas[col] = col_normalizada
    
    # Renombrar columnas del DataFrame
    df_clean = df.copy()
    df_clean = df_clean.rename(columns=mapeo_columnas)
    
    # Convertir columnas de fecha ANTES de insertar
    columnas_fecha = ['FECHA_FIRMA_CONTRATO', 'Fecha_de_Inicio_de_Poliza', 'Fecha_de_Finalizacion_de_Poliza']
    for col_fecha in columnas_fecha:
        if col_fecha in df_clean.columns:
            # Convertir a datetime y luego a date (solo la parte de fecha, sin hora)
            df_clean[col_fecha] = pd.to_datetime(df_clean[col_fecha], errors='coerce').dt.date
    
    # Limpiar y cargar datos
    df_clean = df_clean.fillna('')
    
    # Verificar si ya hay datos en la tabla llamado
    check_query = text(f'SELECT COUNT(*) FROM "{esquema}".llamado')
    result = conn.execute(check_query)
    count = result.scalar()
    
    if count == 0:
        # Solo insertar si la tabla está vacía
        df_clean.to_sql('llamado', conn, schema=esquema, if_exists='append', index=False, method='multi')
    else:
        # Ya hay datos, no insertar para evitar duplicados
        pass

def crear_tabla_ejecucion_general(conn, esquema, df):
    """Crea la tabla ejecucion_general con su estructura específica
    OPTIMIZADO: Carga en chunks para no congelar el navegador"""
    # Crear tabla con nombres NORMALIZADOS
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{esquema}".ejecucion_general (
        "CODIGO_EMISOR" VARCHAR(50),
        "CODIGO_LICITACION" VARCHAR(50),
        "I_D" INTEGER,
        "MODALIDAD" VARCHAR(20),
        "NUMERO_DE_LLAMADO" INTEGER,
        "AÑO_DEL_LLAMADO" INTEGER,
        "NOMBRE_DEL_LLAMADO" TEXT,
        "EMPRESA_ADJUDICADA" TEXT,
        "RUC" VARCHAR(50),
        "FECHA_FIRMA_CONTRATO" DATE,
        "NUMERO_CONTRATO" VARCHAR(100),
        "VIGENCIA_CONTRATO" TEXT,
        "Fecha_de_Inicio_de_Poliza" DATE,
        "Fecha_de_Finalizacion_de_Poliza" DATE,
        "ESTADO_DEL_LOTE_ITEM" TEXT,
        "LOTE" DECIMAL(10,2),
        "ITEM" DECIMAL(10,2),
        "SERVICIO_BENEFICIARIO" TEXT,
        "DESCRIPCION_DEL_PRODUCTO" TEXT,
        "PRESENTACION" TEXT,
        "MARCA" TEXT,
        "PROCEDENCIA" TEXT,
        "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA" TEXT,
        "UNIDAD_DE_MEDIDA" VARCHAR(50),
        "PRECIO_UNITARIO" DECIMAL(15,2),
        "CANTIDAD_MINIMA" DECIMAL(15,2),
        "CANTIDAD_MAXIMA" DECIMAL(15,2),
        "REDISTRIBUCION_CANTIDAD_MINIMA" DECIMAL(15,2),
        "REDISTRIBUCION_CANTIDAD_MAXIMA" DECIMAL(15,2),
        "ENTRADAS_20P_ADENDAS_DE_AMPLIACION" DECIMAL(15,2),
        "SALIDAS_ADENDAS_DE_DISMINUCION" DECIMAL(15,2),
        "TOTAL_ADJUDICADO" DECIMAL(15,2),
        "CANTIDAD_EMITIDA" DECIMAL(15,2),
        "SALDO_A_EMITIR" DECIMAL(15,2),
        "PORCENTAJE_EMITIDO" DECIMAL(5,2)
    )
    '''
    conn.execute(text(create_sql))
    
    # NORMALIZAR nombres de columnas
    df_clean = df.copy()
    
    # Crear un mapeo más robusto
    nuevas_columnas = []
    for col in df_clean.columns:
        # Reemplazar en orden específico para evitar problemas
        col_norm = col
        
        # Primero reemplazar // por un marcador temporal
        col_norm = col_norm.replace('//', '___SLASH___')
        
        # Luego reemplazar / simple por _
        col_norm = col_norm.replace('/', '_')
        
        # Luego volver el marcador a un solo _
        col_norm = col_norm.replace('___SLASH___', '_')
        
        # Reemplazar espacios por _
        col_norm = col_norm.replace(' ', '_')
        
        # Reemplazar otros caracteres especiales
        col_norm = col_norm.replace('%', 'P')
        col_norm = col_norm.replace('(', '')
        col_norm = col_norm.replace(')', '')
        
        # Limpiar múltiples guiones bajos consecutivos
        while '__' in col_norm:
            col_norm = col_norm.replace('__', '_')
        
        nuevas_columnas.append(col_norm)
    
    df_clean.columns = nuevas_columnas
    
    # Limpiar DataFrame con función existente
    df_clean = limpiar_dataframe_numerico(df_clean)
    
    # Convertir columnas de fecha ANTES de insertar
    columnas_fecha = ['FECHA_FIRMA_CONTRATO', 'Fecha_de_Inicio_de_Poliza', 'Fecha_de_Finalizacion_de_Poliza']
    for col_fecha in columnas_fecha:
        if col_fecha in df_clean.columns:
            # Convertir a datetime y luego a date (solo la parte de fecha, sin hora)
            df_clean[col_fecha] = pd.to_datetime(df_clean[col_fecha], errors='coerce').dt.date
    
    # ===== OPTIMIZACIÓN: CARGAR EN CHUNKS SI HAY MUCHOS REGISTROS =====
    total_registros = len(df_clean)
    
    if total_registros > 100:
        # Cargar en chunks
        chunk_size = 500
        progress_container = st.empty()
        
        for i in range(0, total_registros, chunk_size):
            chunk = df_clean.iloc[i:i+chunk_size]
            registros_hasta_ahora = min(i + chunk_size, total_registros)
            
            # Mostrar progreso
            progress_container.info(
                f"📊 Cargando ejecucion_general... {registros_hasta_ahora}/{total_registros} "
                f"({int(registros_hasta_ahora/total_registros*100)}%)"
            )
            
            # Insertar chunk
            chunk.to_sql('ejecucion_general', conn, schema=esquema, 
                        if_exists='append', index=False, method=None)
        
        # Limpiar mensaje de progreso
        progress_container.empty()
    else:
        # Si son pocos registros, cargar todo de una vez
        df_clean.to_sql('ejecucion_general', conn, schema=esquema, 
                        if_exists='append', index=False, method=None)


def crear_tabla_orden_compra(conn, esquema, df):
    """Crea la tabla orden_de_compra con su estructura específica
    OPTIMIZADO: Carga en chunks para no congelar el navegador
    ACTUALIZADO: Con las columnas EXACTAS del Excel de Bruno"""
    # Crear tabla con las columnas EXACTAS
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{esquema}".orden_de_compra (
        "NUMERO_ORDEN_DE_COMPRA" VARCHAR(100),
        "FECHA_DE_EMISION" DATE,
        "CODIGO_DE_EMISION_DE_ORDEN_DE_COMPRA" VARCHAR(100),
        "I_D" VARCHAR(50),
        "MODALIDAD" VARCHAR(20),
        "NUMERO_DE_LLAMADO" VARCHAR(50),
        "AÑO_DEL_LLAMADO" VARCHAR(50),
        "NOMBRE_DEL_LLAMADO" TEXT,
        "EMPRESA_ADJUDICADA" TEXT,
        "RUC" VARCHAR(50),
        "FECHA_FIRMA_CONTRATO" DATE,
        "NUMERO_CONTRATO" VARCHAR(100),
        "VIGENCIA_CONTRATO" TEXT,
        "Fecha_de_Inicio_de_Poliza" DATE,
        "Fecha_de_Finalizacion_de_Poliza" DATE,
        "ESTADO_DEL_LOTE_ITEM" TEXT,
        "SERVICIO_BENEFICIARIO" TEXT,
        "LOTE" VARCHAR(50),
        "ITEM" VARCHAR(50),
        "CANTIDAD_SOLICITADA" DECIMAL(15,2),
        "UNIDAD_DE_MEDIDA" VARCHAR(50),
        "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA" TEXT,
        "PRECIO_UNITARIO" DECIMAL(15,2),
        "PORCENTAJE_DEL_LOTE_ITEM_GLOBAL" DECIMAL(5,2),
        "SALDO_A_EMITIR_DEL_SERVICIO_SANITARIO" DECIMAL(15,2),
        "Observaciones" TEXT
    )
    '''
    conn.execute(text(create_sql))
    
    # NORMALIZAR nombres de columnas
    df_clean = df.copy()
    
    nuevas_columnas = []
    for col in df_clean.columns:
        col_norm = col
        
        # Reemplazar // por un marcador temporal
        col_norm = col_norm.replace('//', '___SLASH___')
        
        # Reemplazar / simple por _
        col_norm = col_norm.replace('/', '_')
        
        # Volver el marcador a un solo _
        col_norm = col_norm.replace('___SLASH___', '_')
        
        # Reemplazar espacios por _
        col_norm = col_norm.replace(' ', '_')
        
        # Reemplazar otros caracteres especiales
        col_norm = col_norm.replace('+', '_')
        col_norm = col_norm.replace('°', '')
        col_norm = col_norm.replace('(', '')
        col_norm = col_norm.replace(')', '')
        
        # Limpiar múltiples guiones bajos consecutivos
        while '__' in col_norm:
            col_norm = col_norm.replace('__', '_')
        
        nuevas_columnas.append(col_norm)
    
    df_clean.columns = nuevas_columnas
    
    # Convertir columnas de fecha ANTES de insertar
    columnas_fecha = ['FECHA_DE_EMISION', 'FECHA_FIRMA_CONTRATO', 'Fecha_de_Inicio_de_Poliza', 'Fecha_de_Finalizacion_de_Poliza']
    for col_fecha in columnas_fecha:
        if col_fecha in df_clean.columns:
            df_clean[col_fecha] = pd.to_datetime(df_clean[col_fecha], errors='coerce')
    
    # ===== LIMPIAR COLUMNAS NUMÉRICAS PRIMERO =====
    # Esto DEBE ir ANTES del filtro de filas vacías
    MIN_INT64 = -9223372036854775808
    MAX_INT64 = 9223372036854775807
    
    columnas_numericas = ['CANTIDAD_SOLICITADA', 'PRECIO_UNITARIO', 'PORCENTAJE_DEL_LOTE_ITEM_GLOBAL', 'SALDO_A_EMITIR_DEL_SERVICIO_SANITARIO']
    
    for col in df_clean.columns:
        if col not in columnas_numericas:
            # Para columnas de texto: rellenar con cadena vacía
            df_clean[col] = df_clean[col].fillna('')
        else:
            # Para columnas numéricas: convertir a numeric y manejar NaN correctamente
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            
            # Reemplazar valores extremos Y NaN con None (NULL en SQL)
            df_clean[col] = df_clean[col].where(
                (df_clean[col] != MIN_INT64) & 
                (df_clean[col] != MAX_INT64) &
                (df_clean[col].notna()),
                None
            )
    
    # ===== FILTRAR FILAS VACÍAS =====
    # Eliminar filas donde TODAS las columnas importantes estén vacías
    columnas_importantes = ['NUMERO_ORDEN_DE_COMPRA', 'SERVICIO_BENEFICIARIO', 'LOTE', 'ITEM']
    
    # Crear máscara: al menos UNA columna importante debe tener datos
    mascara = False
    for col in columnas_importantes:
        if col in df_clean.columns:
            mascara = mascara | df_clean[col].notna() & (df_clean[col] != '') & (df_clean[col] != ' ')
    
    # Aplicar filtro
    filas_originales = len(df_clean)
    df_clean = df_clean[mascara]
    filas_con_datos = len(df_clean)
    
    if filas_con_datos < filas_originales:
        st.info(f"🧹 Filtradas {filas_originales - filas_con_datos} filas vacías. Cargando {filas_con_datos} filas con datos.")
    
    # Convertir fechas a date después del filtro
    for col_fecha in columnas_fecha:
        if col_fecha in df_clean.columns:
            df_clean[col_fecha] = df_clean[col_fecha].dt.date
    
    # ===== CARGAR EN CHUNKS =====
    total_registros = len(df_clean)
    chunk_size = 500
    
    progress_container = st.empty()
    
    for i in range(0, total_registros, chunk_size):
        chunk = df_clean.iloc[i:i+chunk_size].copy()
        registros_hasta_ahora = min(i + chunk_size, total_registros)
        
        # CRÍTICO: Convertir TODAS las cadenas vacías a None justo antes de insertar
        for col in chunk.columns:
            if col in columnas_numericas:
                # Asegurar que columnas numéricas NO tengan cadenas vacías
                chunk[col] = chunk[col].replace(['', ' ', 'nan', 'NaN'], None)
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                chunk[col] = chunk[col].where(pd.notna(chunk[col]), None)
        
        progress_container.info(
            f"🛒 Cargando orden_de_compra... {registros_hasta_ahora}/{total_registros} "
            f"({int(registros_hasta_ahora/total_registros*100)}%)"
        )
        
        chunk.to_sql('orden_de_compra', conn, schema=esquema, if_exists='append', index=False, method=None)
    
    progress_container.empty()


def obtener_archivos_cargados():
    """Obtiene la lista de archivos cargados con su estado actual - SOLO ACTIVOS Y ÚNICOS"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return []
        
        # Para API REST, usar execute_query
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Por ahora, retornar lista vacía (se puede implementar con API REST más adelante)
            return []
        
        with engine.connect() as conn:
            query = text("""
                SELECT DISTINCT ON (ac.esquema) 
                       ac.id, ac.nombre_archivo, ac.esquema, ac.fecha_carga, 
                       u.username as usuario, ac.estado
                FROM oxigeno.archivos_cargados ac
                JOIN oxigeno.usuarios u ON ac.usuario_id = u.id
                WHERE ac.estado = 'Activo'
                ORDER BY ac.esquema, ac.fecha_carga DESC
            """)
            
            result = conn.execute(query)
            
            archivos = []
            for row in result:
                archivos.append({
                    'id': row[0],
                    'nombre_archivo': row[1],
                    'esquema': row[2],
                    'fecha_carga': row[3],
                    'usuario': row[4],
                    'estado': row[5]
                })
            
            return archivos
    except Exception as e:
        st.error(f"Error obteniendo archivos cargados: {e}")
        return []

def eliminar_esquema_postgres(esquema):
    """Elimina un esquema de PostgreSQL y actualiza la tabla de cargas"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return False, "No se pudo conectar a la base de datos"
        
        # Si es API REST, intentar obtener conexión directa como fallback
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Intentar conexión directa para operaciones complejas
            from urllib.parse import quote_plus
            config = get_db_config_licitaciones()
            host = config['host']
            port = config['port']
            dbname = config['name']
            user = config['user']
            password = config['password']
            
            # Asegurar formato correcto de usuario para Supabase
            if '.supabase.co' in host:
                instance_id = host.split('.')[1] if host.startswith('db.') else host.split('.')[0]
                if not user.startswith(f'postgres.{instance_id}'):
                    user = f"postgres.{instance_id}"
            
            try:
                password_escaped = quote_plus(password)
                conn_str = f"postgresql://{user}:{password_escaped}@{host}:{port}/{dbname}?sslmode=require"
                engine = create_engine(
                    conn_str,
                    connect_args={
                        "client_encoding": "utf8",
                        "connect_timeout": 10,
                        "sslmode": "require"
                    },
                    pool_pre_ping=True
                )
                # Probar conexión
                with engine.connect() as test_conn:
                    test_conn.execute(text("SELECT 1"))
            except Exception as e:
                st.error(f"La eliminación de esquemas requiere conexión directa. Error: {e}")
                return False, f"No se pudo establecer conexión directa: {e}"
        
        with engine.connect() as conn:
            # Iniciar transacción
            trans = conn.begin()
            try:
                # Actualizar estado en la tabla de archivos cargados
                query_update = text("""
                    UPDATE archivos_cargados
                    SET estado = 'Eliminado'
                    WHERE esquema = :esquema
                """)
                
                conn.execute(query_update, {'esquema': esquema})
                
                # Eliminar el esquema
                query = text(f'DROP SCHEMA IF EXISTS "{esquema}" CASCADE')
                conn.execute(query)
                
                # Confirmar transacción
                trans.commit()
                
                return True, f"Esquema '{esquema}' eliminado correctamente."
            except Exception as e:
                # Revertir transacción en caso de error
                trans.rollback()
                raise e
    except Exception as e:
        return False, f"Error al eliminar esquema: {e}"

def pagina_login():
    """Página de inicio de sesión"""
    st.title("Sistema de Gestión de Licitaciones")
    st.subheader("Iniciar sesión")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        cedula = st.text_input("Cédula de Identidad:")  # Cambiar a cédula
        password = st.text_input("Contraseña:", type="password")
        
        if st.button("Ingresar"):
            if not cedula or not password:  # Cambiar validación
                st.error("Por favor, complete todos los campos.")
            else:
                # Verificar credenciales
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                
                try:
                    # Verificar credenciales usando función compatible con API REST
                    # Nota: La tabla debe estar en el esquema 'public' para que funcione con API REST
                    query = """
                        SELECT id, username, role, nombre_completo, ultimo_cambio_password 
                        FROM usuarios 
                        WHERE cedula = :cedula AND password = :password
                    """
                    
                    user = execute_query(
                        query, 
                        params={'cedula': cedula, 'password': password_hash},
                        fetch_one=True
                    )
                    
                    if user:
                        # Autenticación exitosa
                        # Si es dict (API REST), convertir a tupla para compatibilidad
                        if isinstance(user, dict):
                            user_tuple = (
                                user.get('id'),
                                user.get('username'),
                                user.get('role'),
                                user.get('nombre_completo'),
                                user.get('ultimo_cambio_password')
                            )
                            user = user_tuple
                        
                        st.session_state.logged_in = True
                        st.session_state.user_id = user[0]
                        st.session_state.username = user[1]
                        st.session_state.user_role = user[2]
                        st.session_state.user_name = user[3]
                        
                        # Verificar si se requiere cambio de contraseña
                        if user[4] is None:  # ultimo_cambio_password es NULL
                            st.session_state.requiere_cambio_password = True
                            st.warning("Se requiere cambiar su contraseña. Será redirigido para hacerlo.")
                            time.sleep(2)
                        else:
                            st.session_state.requiere_cambio_password = False
                        
                        st.success("Inicio de sesión exitoso!")
                        registrar_actividad(
                            accion="LOGIN",
                            modulo="USUARIOS", 
                            descripcion=f"Usuario {cedula} inició sesión exitosamente")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Cédula o contraseña incorrectos.")
                except Exception as e:
                    st.error(f"Error al verificar credenciales: {e}")    
    with col2:
        st.image("https://via.placeholder.com/300x200?text=Logo+Sistema", width=300)

def obtener_proveedores():
    try:
        engine = safe_get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return []
        with engine.connect() as conn:
            query = text("""
                SELECT razon_social, ruc 
                FROM oxigeno.proveedores 
                WHERE activo = TRUE 
                ORDER BY razon_social
            """)
            result = conn.execute(query)
            proveedores = []
            for row in result:
                proveedores.append({
                    'nombre': row[0],  # razon_social
                    'ruc': row[1]      # ruc
                })
            return proveedores
    except Exception as e:
        st.error(f"Error al obtener proveedores: {e}")
        return []
        
        # Campos en el orden solicitado
        col1, col2 = st.columns(2)
        
        with col1:
            id_licitacion = st.text_input("I.D.:")
            modalidad = st.text_input("Modalidad:", placeholder="Ej: CD, LP, LC, CO, LPN, CVE, LPI, LCO, MCN...")
            numero_anio = st.text_input("N° / Año de Modalidad:")
        
        with col2:
            nombre_llamado = st.text_input("Nombre del llamado:")
            
            # Autocompletado de proveedores
            proveedores_existentes = obtener_proveedores()
            
            if proveedores_existentes:
                empresa_options = [f"{p['nombre']} - {p['ruc']}" for p in proveedores_existentes]
                
                empresa_seleccionada = st.selectbox(
                    "Empresa Adjudicada:",
                    options=["Seleccionar..."] + empresa_options + ["+ Nuevo Proveedor"]
                )
                
                if empresa_seleccionada == "+ Nuevo Proveedor":
                    st.info("💡 Vaya a 'Gestión de Proveedores' para registrar una nueva empresa")
                    empresa_adjudicada = st.text_input("Nombre de la empresa:", disabled=True)
                    ruc = st.text_input("RUC:", disabled=True)
                elif empresa_seleccionada != "Seleccionar...":
                    # Extraer datos del proveedor seleccionado
                    empresa_adjudicada = empresa_seleccionada.split(" - ")[0]
                    ruc_autocompletado = empresa_seleccionada.split(" - ")[-1]
                    ruc = st.text_input("RUC:", value=ruc_autocompletado, disabled=True)
                else:
                    empresa_adjudicada = ""
                    ruc = st.text_input("RUC:", disabled=True)
            else:
                st.warning("⚠️ No hay proveedores registrados. Registre primero en 'Gestión de Proveedores'")
                empresa_adjudicada = st.text_input("Empresa Adjudicada:", disabled=True)
                ruc = st.text_input("RUC:", disabled=True)
        
        # Segunda fila de campos
        col3, col4 = st.columns(2)
        
        with col3:
            vigencia_contrato = st.text_input("Vigencia del Contrato:")
        
        with col4:
            # Usar widget de fecha con formato DD/MM/YYYY
            fecha_firma = st.date_input(
                "Fecha de la firma del contrato:",
                value=datetime.now(),
                format="DD/MM/YYYY"  # ✅ Formato día/mes/año
            )
            
            # Mostrar la fecha seleccionada en español
            st.caption(f"📅 Fecha seleccionada: {fecha_firma.strftime('%d/%m/%Y')}")
        
        # Otros campos adicionales
        col5, col6 = st.columns(2)
        
        with col5:
            numero_contrato = st.text_input("Número de contrato/año:")
        
        with col6:
            # Generar sugerencia de nombre de esquema
            if modalidad and numero_anio:
                esquema_sugerido = f"{modalidad.strip().lower()}-{numero_anio.strip()}"
                esquema_personalizado = st.text_input("Nombre del esquema:", value=esquema_sugerido)
            else:
                esquema_personalizado = st.text_input("Nombre del esquema:")
        
        st.divider()
        
        # Campo para subir archivo
        archivo = st.file_uploader("Seleccionar archivo:", type=["csv", "xlsx", "xls"])
        
        # SECCIÓN DE ANÁLISIS DE DATOS
        if archivo is not None:
            st.subheader("📊 Análisis del Archivo")
            
            # Checkbox para activar análisis
            mostrar_analisis = st.checkbox("🔍 Mostrar análisis detallado del archivo")
            
            if mostrar_analisis:
                try:
                    # Determinar el tipo de archivo
                    extension = archivo.name.split('.')[-1].lower()
                    
                    if extension in ['xlsx', 'xls']:
                        # Análisis para archivos Excel
                        xls = pd.ExcelFile(archivo)
                        
                        st.write(f"**📁 Archivo:** {archivo.name}")
                        st.write(f"**📄 Tipo:** Excel ({extension.upper()})")
                        st.write(f"**📋 Hojas encontradas:** {len(xls.sheet_names)}")
                        
                        # Mostrar hojas disponibles
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Hojas en el archivo:**")
                            for i, sheet in enumerate(xls.sheet_names, 1):
                                st.write(f"{i}. {sheet}")
                        
                        with col2:
                            # Verificar hojas requeridas
                            required_sheets = ["ejecucion_general", "orden_de_compra", "llamado"]
                            missing_sheets = []
                            found_sheets = []
                            
                            for req_sheet in required_sheets:
                                found = any(req_sheet.lower() == sheet.lower() for sheet in xls.sheet_names)
                                if found:
                                    found_sheets.append(req_sheet)
                                else:
                                    missing_sheets.append(req_sheet)
                            
                            st.write("**Estado de hojas requeridas:**")
                            for sheet in found_sheets:
                                st.write(f"✅ {sheet}")
                            for sheet in missing_sheets:
                                st.write(f"❌ {sheet}")
                        
                        # Análisis detallado de cada hoja
                        hoja_analisis = st.selectbox(
                            "Seleccionar hoja para análisis detallado:",
                            options=xls.sheet_names
                        )
                        
                        if hoja_analisis:
                            # Leer una muestra de la hoja seleccionada
                            df_sample = pd.read_excel(xls, sheet_name=hoja_analisis, nrows=10)
                            
                            st.write(f"**📊 Análisis de la hoja '{hoja_analisis}':**")
                            
                            # Información básica
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Columnas", len(df_sample.columns))
                            with col2:
                                # Leer toda la hoja para contar filas (puede ser lento para archivos grandes)
                                try:
                                    df_full = pd.read_excel(xls, sheet_name=hoja_analisis)
                                    st.metric("Total Filas", len(df_full))
                                except:
                                    st.metric("Total Filas", "Error al contar")
                            with col3:
                                # Contar columnas vacías
                                empty_cols = df_sample.isnull().all().sum()
                                st.metric("Columnas Vacías", empty_cols)
                            
                            # Mostrar nombres de columnas
                            st.write("**Columnas encontradas:**")
                            cols_text = ", ".join(df_sample.columns.tolist())
                            st.text_area("Lista de columnas:", value=cols_text, height=100, disabled=True)
                            
                            # Mostrar muestra de datos
                            st.write("**Muestra de datos (primeras 10 filas):**")
                            st.dataframe(df_sample)
                            
                            # Verificar tipos de datos
                            st.write("**Tipos de datos por columna:**")
                            tipos_df = pd.DataFrame({
                                'Columna': df_sample.columns,
                                'Tipo': df_sample.dtypes.values,
                                'Valores Nulos': df_sample.isnull().sum().values,
                                'Valores Únicos': df_sample.nunique().values
                            })
                            st.dataframe(tipos_df)
                    
                    elif extension == 'csv':
                        # Análisis para archivos CSV (código similar al anterior)
                        contenido = archivo.getvalue().decode('utf-8')
                        df_sample = pd.read_csv(io.StringIO(contenido), nrows=10)
                        
                        st.write(f"**📁 Archivo:** {archivo.name}")
                        st.write(f"**📄 Tipo:** CSV")
                        
                        # Información básica
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Columnas", len(df_sample.columns))
                        with col2:
                            try:
                                df_full = pd.read_csv(io.StringIO(contenido))
                                st.metric("Total Filas", len(df_full))
                            except:
                                st.metric("Total Filas", "Error al contar")
                        with col3:
                            empty_cols = df_sample.isnull().all().sum()
                            st.metric("Columnas Vacías", empty_cols)
                        
                        st.write("**Columnas encontradas:**")
                        cols_text = ", ".join(df_sample.columns.tolist())
                        st.text_area("Lista de columnas:", value=cols_text, height=100, disabled=True)
                        
                        st.write("**Muestra de datos (primeras 10 filas):**")
                        st.dataframe(df_sample)
                    
                    # Mensaje de estado
                    if extension in ['xlsx', 'xls'] and 'missing_sheets' in locals() and missing_sheets:
                        st.warning(f"⚠️ Faltan las siguientes hojas requeridas: {', '.join(missing_sheets)}")
                        st.info("El sistema puede intentar generar automáticamente algunas hojas faltantes.")
                    else:
                        st.success("✅ El archivo parece tener la estructura correcta para ser procesado.")
                
                except Exception as e:
                    st.error(f"Error al analizar el archivo: {str(e)}")
            
            # Separador visual
            st.divider()
        
        # BOTONES DEL FORMULARIO - SIEMPRE VISIBLES
        col1, col2 = st.columns(2)
        
        with col1:
            # Botón para procesar - siempre visible
            submit = st.form_submit_button("🚀 Procesar y Cargar Archivo", type="primary", disabled=(archivo is None))
        
        with col2:
            # Botón para limpiar - siempre visible
            limpiar = st.form_submit_button("🗑️ Limpiar Formulario", type="secondary")
        
        # Procesar según el botón presionado
        if limpiar:
            # Limpiar estado del formulario
            st.rerun()
        
        elif submit:
            if not archivo:
                st.error("Por favor, seleccione un archivo Excel o CSV.")
            elif not id_licitacion or not modalidad or not numero_anio or not nombre_llamado or not empresa_adjudicada or not vigencia_contrato or not numero_contrato:
                st.error("Por favor, complete todos los campos obligatorios.")
            else:
                # Usar el esquema personalizado
                esquema = esquema_personalizado
                empresa_para_tablas = empresa_adjudicada.strip().upper().replace(" ", "_")
                
                # Crear diccionario con datos del formulario
                datos_formulario = {
                    'id_licitacion': id_licitacion,
                    'modalidad': modalidad,
                    'numero_anio': numero_anio,
                    'nombre_llamado': nombre_llamado,
                    'empresa_adjudicada': empresa_adjudicada,
                    'ruc': ruc,
                    'fecha_firma': fecha_firma,
                    'numero_contrato': numero_contrato,
                    'vigencia_contrato': vigencia_contrato
                }
                
                # Procesar el archivo con el prefijo de empresa en las tablas
                with st.spinner("Procesando archivo y creando tablas..."):
                    success, message = cargar_archivo_a_postgres(
                        archivo,
                        archivo.name,
                        esquema,
                        empresa_para_tablas,  # Pasar la empresa como parámetro adicional
                        datos_formulario  # Pasar datos del formulario
                    )
                
                if success:
                    st.success(f"Archivo cargado correctamente en el esquema '{esquema}' con tablas de empresa '{empresa_para_tablas}'")
                    st.balloons()
                else:
                    st.error(message)

def pagina_cargar_archivo():
    """Página para cargar un nuevo archivo"""
    st.header("Cargar Archivo")
    
    # Inicializar variables de estado para autocompletado
    if 'licitacion_seleccionada' not in st.session_state:
        st.session_state.licitacion_seleccionada = None
    if 'licitacion_data' not in st.session_state:
        st.session_state.licitacion_data = {}
    if 'datos_confirmados' not in st.session_state:
        st.session_state.datos_confirmados = False
    
    # BUSCADOR PEQUEÑO SOLO PARA ID - FUERA DEL FORMULARIO
    col_search1, col_search2 = st.columns([1, 3])
    with col_search1:
        st.write("Buscar ID existente:")
    with col_search2:
        id_busqueda = st.text_input("", placeholder="Escriba un ID para buscar...", key="id_search")
        if id_busqueda:
            # Buscar en la base de datos si existe una licitación con ese ID
            try:
                engine = safe_get_engine()
                if engine is None:
                    st.error("No se pudo conectar a la base de datos")
                    return
                with engine.connect() as conn:
                    esquemas = obtener_esquemas_postgres()
                    licitacion_encontrada = False
                    
                    for esquema in esquemas:
                        try:
                            query = text(f"""
                                SELECT "I_D", "NOMBRE_DEL_LLAMADO"
                                FROM "{esquema}"."llamado"
                                WHERE "I_D" ILIKE :id_busqueda
                                LIMIT 5
                            """)
                            result = conn.execute(query, {'id_busqueda': f"%{id_busqueda}%"})
                            resultados = result.fetchall()
                            
                            if resultados:
                                licitacion_encontrada = True
                                st.success("IDs encontrados:")
                                for r in resultados:
                                    if st.button(f"{r[0]} - {r[1]}", key=f"btn_{r[0]}"):
                                        st.session_state.licitacion_seleccionada = r[0]
                                        st.rerun()
                        except Exception:
                            pass
                    
                    if not licitacion_encontrada:
                        st.info("No se encontraron IDs similares")
            except Exception as e:
                st.error(f"Error al buscar: {e}")
    
    # Formulario principal para subir archivo
    with st.form("upload_form", clear_on_submit=False):
        st.subheader("📋 Información de la Licitación")
        
        # Campos en el orden solicitado
        col1, col2 = st.columns(2)
        
        with col1:
            id_licitacion = st.text_input("I.D.:", 
                                         value=st.session_state.licitacion_data.get('id_licitacion', ''))
            
            modalidad = st.selectbox("Modalidad:", 
                                   options=["Seleccionar...", "CD", "LP", "LC", "CO", "LPN", "CVE", "LPI", "LCO", "MCN"],
                                   index=0,
                                   key="modalidad_select")
            
            numero_anio = st.text_input("N° / Año de Modalidad:",
                                       value=st.session_state.licitacion_data.get('numero_anio', ''))
            
            servicio_beneficiario = st.text_input("Servicio Beneficiario (Opcional):",
                                                value=st.session_state.licitacion_data.get('servicio_beneficiario', ''),
                                                help="Ej: HOSPITAL GENERAL DE BARRIO OBRERO. Útil para diferenciar licitaciones con el mismo nombre.")
            
            # Botón de confirmación justo después de Servicio Beneficiario
            confirmar_datos_iniciales = st.form_submit_button("Confirmar Datos Iniciales")
            
            if confirmar_datos_iniciales:
                if not id_licitacion or modalidad == "Seleccionar..." or not numero_anio:
                    st.error("Por favor, complete todos los campos obligatorios antes de confirmar.")
                else:
                    # Verificar si ya existe una combinación similar en la base de datos
                    try:
                        engine = get_engine()
                        if engine is None:
                            st.error("No se pudo conectar a la base de datos")
                            return
                        with engine.connect() as conn:
                            esquemas = obtener_esquemas_postgres()
                            duplicado_encontrado = False
                            
                            for esquema in esquemas:
                                try:
                                    query = text(f"""
                                        SELECT "I_D", "NOMBRE_DEL_LLAMADO", "EMPRESA_ADJUDICADA", "NUMERO_DE_LLAMADO"
                                        FROM "{esquema}"."llamado"
                                        WHERE "I_D" = :id_licitacion
                                    """)
                                    result = conn.execute(query, {'id_licitacion': id_licitacion})
                                    coincidencia = result.fetchone()
                                    
                                    if coincidencia:
                                        duplicado_encontrado = True
                                        st.info(f"ℹ️ Ya existe la licitación ID '{id_licitacion}' en el esquema '{esquema}'.")
                                        st.info(f"📋 Detalles: {coincidencia[1]} - Empresa: {coincidencia[2]} - N°: {coincidencia[3]}")
                                        st.success(f"✅ Puede agregar datos de OTRO SERVICIO a esta misma licitación.")
                                        st.warning(f"⚠️ Nota: Los datos de '{servicio_beneficiario}' se AGREGARÁN a los existentes sin eliminar nada.")
                                        break
                                except Exception:
                                    pass
                            
                            # Permitir continuar incluso si hay duplicado (para agregar más servicios)
                            st.success("✅ Datos iniciales confirmados. Por favor, complete el resto del formulario.")
                            st.session_state.datos_confirmados = True
                            # Guardar servicio beneficiario en session_state
                            st.session_state.licitacion_data['servicio_beneficiario'] = servicio_beneficiario
                    except Exception as e:
                        st.error(f"Error al verificar duplicados: {e}")
        
        # Si se ingresó un ID de licitación, intentar buscar datos existentes
        if id_licitacion and id_licitacion == st.session_state.licitacion_seleccionada:
            # Ya tenemos los datos, no hay que volver a buscar
            pass
        elif id_licitacion and id_licitacion != st.session_state.licitacion_seleccionada:
            # Código existente para buscar licitación por ID...
            pass
        
        with col2:
            nombre_llamado = st.text_input("Nombre del llamado:", 
                                          value=st.session_state.licitacion_data.get('nombre_llamado', ''))
            
            # Autocompletado de proveedores
            proveedores_existentes = obtener_proveedores()
            
            if proveedores_existentes:
                # Preparar opciones solo con nombres (sin RUC)
                empresas = [p['nombre'] for p in proveedores_existentes]
                
                empresa_seleccionada = st.selectbox(
                    "Empresa Adjudicada:",
                    options=["Seleccionar..."] + empresas + ["+ Nuevo Proveedor"],
                    key="empresa_select"
                )
                
                if empresa_seleccionada == "+ Nuevo Proveedor":
                    st.info("💡 Vaya a 'Gestión de Proveedores' para registrar una nueva empresa")
                    empresa_adjudicada = ""
                    ruc = st.text_input("RUC:", disabled=True)
                elif empresa_seleccionada != "Seleccionar...":
                    # Buscar el RUC correspondiente
                    empresa_adjudicada = empresa_seleccionada
                    
                    # Búsqueda directa del RUC
                    ruc_value = ""
                    for p in proveedores_existentes:
                        if p['nombre'] == empresa_adjudicada:
                            ruc_value = p['ruc']
                            break
                    
                    # Mostrar el RUC (visible pero no editable)
                    ruc = st.text_input("RUC:", value=ruc_value, disabled=True)
                else:
                    empresa_adjudicada = ""
                    ruc = st.text_input("RUC:", disabled=True)
            else:
                st.warning("⚠️ No hay proveedores registrados. Registre primero en 'Gestión de Proveedores'")
                empresa_adjudicada = ""
                ruc = st.text_input("RUC:", disabled=True)
        
        # Segunda fila de campos
        col3, col4 = st.columns(2)
        
        with col3:
            vigencia_contrato = st.text_input("Vigencia del Contrato:")
        
        with col4:
            # Usar widget de fecha con formato DD/MM/YYYY
            fecha_firma = st.date_input(
                "Fecha de la firma del contrato:",
                value=datetime.now(),
                format="DD/MM/YYYY"  # ✅ Formato día/mes/año
            )
            
            # Mostrar la fecha seleccionada
            st.caption(f"📅 Fecha seleccionada: {fecha_firma.strftime('%d/%m/%Y')}")
        
        # Último campo
        col5, _ = st.columns(2)
        
        with col5:
            numero_contrato = st.text_input("Número de contrato/año:")
        
        st.divider()
        
        # Campo para subir archivo
        archivo = st.file_uploader("📎 Seleccionar archivo Excel:", type=["xlsx", "xls"], key="archivo_upload")
        
        # ESQUEMA GENERADO AUTOMÁTICAMENTE (OCULTO)
        if modalidad != "Seleccionar..." and numero_anio:
            esquema_personalizado = f"{modalidad.strip().lower()}-{numero_anio.strip()}"
        else:
            esquema_personalizado = f"licitacion-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # SECCIÓN DE ANÁLISIS DE DATOS
        if archivo is not None:
            st.subheader("📊 Análisis del Archivo")
            
            # Checkbox para activar análisis
            mostrar_analisis = st.checkbox("🔍 Mostrar análisis detallado del archivo")
            
            if mostrar_analisis:
                try:
                    # Determinar el tipo de archivo
                    extension = archivo.name.split('.')[-1].lower()
                    
                    if extension in ['xlsx', 'xls']:
                        # ===== PRE-ANÁLISIS COMPLETO DEL ARCHIVO =====
                        archivo.seek(0)
                        exito_preanalisis, analisis = pre_analizar_excel(archivo)
                        archivo.seek(0)  # Resetear para uso posterior
                        
                        if exito_preanalisis and analisis:
                            # Mostrar resumen general
                            st.write(f"**📄 Archivo:** {archivo.name}")
                            st.write(f"**📁 Tipo:** Excel ({extension.upper()})")
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total de Hojas", analisis['resumen']['total_hojas'])
                            with col2:
                                st.metric("Total de Registros", analisis['resumen']['total_registros'])
                            with col3:
                                st.metric("Hojas", ", ".join(analisis['resumen']['nombres_hojas'][:2]) + ("..." if len(analisis['resumen']['nombres_hojas']) > 2 else ""))
                            
                            # Verificar hojas requeridas
                            st.markdown("---")
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write("**Hojas en el archivo:**")
                                for i, sheet in enumerate(analisis['resumen']['nombres_hojas'], 1):
                                    st.write(f"{i}. {sheet}")
                            
                            with col2:
                                required_sheets = ["ejecucion_general", "orden_de_compra", "llamado"]
                                missing_sheets = []
                                found_sheets = []
                                
                                for req_sheet in required_sheets:
                                    found = any(req_sheet.lower() == sheet.lower() for sheet in analisis['resumen']['nombres_hojas'])
                                    if found:
                                        found_sheets.append(req_sheet)
                                    else:
                                        missing_sheets.append(req_sheet)
                                
                                st.write("**Estado de hojas requeridas:**")
                                for sheet in found_sheets:
                                    st.write(f"✅ {sheet}")
                                for sheet in missing_sheets:
                                    st.write(f"❌ {sheet}")
                            
                            st.markdown("---")
                            
                            # ===== ANÁLISIS DETALLADO POR HOJA =====
                            st.subheader("🔍 Análisis Detallado por Hoja")
                            
                            for nombre_hoja, info_hoja in analisis['hojas'].items():
                                with st.expander(f"📄 {nombre_hoja} ({info_hoja['filas']} filas × {info_hoja['columnas']} columnas)", expanded=False):
                                    
                                    # Métricas de la hoja
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("Filas", info_hoja['filas'])
                                    with col2:
                                        st.metric("Columnas", info_hoja['columnas'])
                                    with col3:
                                        valores_nulos_total = sum([col['valores_nulos'] for col in info_hoja['columnas_info']])
                                        st.metric("Valores Nulos", valores_nulos_total)
                                    
                                    # Información de columnas
                                    st.write("**Columnas detectadas:**")
                                    df_columnas_info = pd.DataFrame([{
                                        'Columna': col['nombre'],
                                        'Tipo': col['tipo_detectado'],
                                        'Únicos': col['valores_unicos'],
                                        'Nulos': col['valores_nulos']
                                    } for col in info_hoja['columnas_info']])
                                    
                                    st.dataframe(df_columnas_info, use_container_width=True)
                                    
                                    # Muestra de datos
                                    if info_hoja['muestra_datos']:
                                        st.write("**Vista previa (primeras 3 filas):**")
                                        df_muestra = pd.DataFrame(info_hoja['muestra_datos'])
                                        st.dataframe(df_muestra, use_container_width=True)
                            
                            st.markdown("---")
                            
                            # ===== SUGERENCIAS DE PRELLENADO =====
                            sugerencias = generar_sugerencias_prellenado(analisis)
                            
                            if sugerencias:
                                st.subheader("💡 Sugerencias de Procesamiento")
                                st.write("*Recomendaciones basadas en el análisis del contenido:*")
                                
                                for nombre_hoja, sugerencias_hoja in sugerencias.items():
                                    if sugerencias_hoja:
                                        with st.expander(f"🔮 Sugerencias para: {nombre_hoja}", expanded=False):
                                            for columna, sugerencia in sugerencias_hoja.items():
                                                col1, col2 = st.columns([3, 1])
                                                
                                                with col1:
                                                    st.write(f"**{columna}**")
                                                    st.write(f"• Tipo sugerido: `{sugerencia.get('tipo_sugerido', 'N/A')}`")
                                                    st.write(f"• Acción: {sugerencia.get('accion_sugerida', 'Ninguna')}")
                                                    if 'valor_ejemplo' in sugerencia:
                                                        st.write(f"• Ejemplo: `{sugerencia['valor_ejemplo']}`")
                                                
                                                with col2:
                                                    st.info("ℹ️ Info")
                            
                            # Mensaje de estado
                            st.markdown("---")
                            if missing_sheets:
                                st.warning(f"⚠️ Faltan las siguientes hojas requeridas: {', '.join(missing_sheets)}")
                                st.info("💡 El sistema intentará generar automáticamente las hojas faltantes durante la carga.")
                            else:
                                st.success("✅ El archivo tiene todas las hojas requeridas y parece estar listo para procesarse.")
                        
                        else:
                            # Análisis básico si falla el pre-análisis completo
                            xls = pd.ExcelFile(archivo)
                            
                            st.write(f"**📄 Archivo:** {archivo.name}")
                            st.write(f"**📁 Tipo:** Excel ({extension.upper()})")
                            st.write(f"**📋 Hojas encontradas:** {len(xls.sheet_names)}")
                            
                            # Mostrar hojas disponibles
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write("**Hojas en el archivo:**")
                                for i, sheet in enumerate(xls.sheet_names, 1):
                                    st.write(f"{i}. {sheet}")
                            
                            with col2:
                                # Verificar hojas requeridas
                                required_sheets = ["ejecucion_general", "orden_de_compra", "llamado"]
                                missing_sheets = []
                                found_sheets = []
                                
                                for req_sheet in required_sheets:
                                    found = any(req_sheet.lower() == sheet.lower() for sheet in xls.sheet_names)
                                    if found:
                                        found_sheets.append(req_sheet)
                                    else:
                                        missing_sheets.append(req_sheet)
                                
                                st.write("**Estado de hojas requeridas:**")
                                for sheet in found_sheets:
                                    st.write(f"✅ {sheet}")
                                for sheet in missing_sheets:
                                    st.write(f"❌ {sheet}")
                            
                            # Análisis detallado de cada hoja
                            hoja_analisis = st.selectbox(
                                "Seleccionar hoja para análisis detallado:",
                                options=xls.sheet_names,
                                key="hoja_analisis_select"
                            )
                            
                            if hoja_analisis:
                                # Leer una muestra de la hoja seleccionada
                                df_sample = pd.read_excel(xls, sheet_name=hoja_analisis, nrows=10)
                                
                                st.write(f"**📊 Análisis de la hoja '{hoja_analisis}':**")
                                
                                # Información básica
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Total Columnas", len(df_sample.columns))
                                with col2:
                                    # Leer toda la hoja para contar filas
                                    try:
                                        df_full = pd.read_excel(xls, sheet_name=hoja_analisis)
                                        st.metric("Total Filas", len(df_full))
                                    except:
                                        st.metric("Total Filas", "Error al contar")
                                with col3:
                                    # Contar columnas vacías
                                    empty_cols = df_sample.isnull().all().sum()
                                    st.metric("Columnas Vacías", empty_cols)
                                
                                # Mostrar nombres de columnas
                                st.write("**Columnas encontradas:**")
                                cols_text = ", ".join(df_sample.columns.tolist())
                                st.text_area("Lista de columnas:", value=cols_text, height=100, disabled=True)
                                
                                # Mostrar muestra de datos
                                st.write("**Muestra de datos (primeras 10 filas):**")
                                st.dataframe(df_sample)
                                
                                # Verificar tipos de datos
                                st.write("**Tipos de datos por columna:**")
                                tipos_df = pd.DataFrame({
                                    'Columna': df_sample.columns,
                                    'Tipo': df_sample.dtypes.values,
                                    'Valores Nulos': df_sample.isnull().sum().values,
                                    'Valores Únicos': df_sample.nunique().values
                                })
                                st.dataframe(tipos_df)
                            
                            # Mensaje de estado
                            if 'missing_sheets' in locals() and missing_sheets:
                                st.warning(f"⚠️ Faltan las siguientes hojas requeridas: {', '.join(missing_sheets)}")
                                st.info("El sistema puede intentar generar automáticamente algunas hojas faltantes.")
                            else:
                                st.success("✅ El archivo parece tener la estructura correcta para ser procesado.")
                
                except Exception as e:
                    st.error(f"Error al analizar el archivo: {str(e)}")
            
            # Separador visual
            st.divider()
        else:
            st.info("Por favor, seleccione un archivo para cargar (.xlsx, .xls o .csv)")
        
        # BOTONES DEL FORMULARIO - SIEMPRE VISIBLES
        col_btns1, col_btns2 = st.columns(2)
        
        with col_btns1:
            # Botón para procesar - Deshabilitar si los datos no están confirmados
            submit = st.form_submit_button("🚀 Procesar y Cargar Archivo", type="primary", 
                                          disabled=not st.session_state.datos_confirmados)
        
        with col_btns2:
            # Botón para limpiar
            limpiar = st.form_submit_button("🗑️ Limpiar Formulario", type="secondary")
    
    # LÓGICA DE PROCESAMIENTO FUERA DEL FORMULARIO
    if submit:
        # Verificar que se ha seleccionado un archivo
        if archivo is None:
            st.error("Por favor, seleccione un archivo Excel o CSV.")
            return
        
        # Verificar campos obligatorios
        if not id_licitacion or modalidad == "Seleccionar..." or not numero_anio or not nombre_llamado or empresa_seleccionada == "Seleccionar..." or not vigencia_contrato or not numero_contrato:
            st.error("Por favor, complete todos los campos obligatorios.")
            return
        
        # Verificar que se ha obtenido un RUC
        if not ruc:
            st.error("No se ha podido obtener el RUC para la empresa seleccionada.")
            return
        
        # Usar el esquema personalizado
        esquema = esquema_personalizado
        
        # Crear diccionario con datos del formulario
        datos_formulario = {
            'id_licitacion': id_licitacion,
            'modalidad': modalidad,
            'numero_anio': numero_anio,
            'nombre_llamado': nombre_llamado,
            'empresa_adjudicada': empresa_adjudicada,
            'ruc': ruc,
            'fecha_firma': fecha_firma,
            'numero_contrato': numero_contrato,
            'vigencia_contrato': vigencia_contrato
        }
        
        # Procesar el archivo
        with st.spinner("Procesando archivo y creando tablas..."):
            success, message = cargar_archivo_a_postgres(
                archivo,
                archivo.name,
                esquema
            )
        
        if success:
            st.success(f"✅ Archivo cargado correctamente en el esquema '{esquema}'")
            
            # ✅ ACTUALIZAR LA TABLA LLAMADO CON MODALIDAD Y OTROS DATOS DEL FORMULARIO
            try:
                esquema_formateado = esquema.strip().lower().replace(' ', '_').replace('-', '_')
                
                engine = safe_get_engine()
                if engine is None:
                    st.error("No se pudo conectar a la base de datos")
                    return
                with engine.connect() as conn:
                    # Actualizar todos los campos del formulario en la tabla llamado
                    query = text(f"""
                        UPDATE "{esquema_formateado}".llamado
                        SET "MODALIDAD" = :modalidad,
                            "I_D" = :id_licitacion,
                            "NUMERO_DE_LLAMADO" = :numero_llamado,
                            "AÑO_DEL_LLAMADO" = :anio_llamado,
                            "NOMBRE_DEL_LLAMADO" = :nombre_llamado,
                            "EMPRESA_ADJUDICADA" = :empresa,
                            "RUC" = :ruc,
                            "FECHA_FIRMA_CONTRATO" = :fecha_firma,
                            "NUMERO_CONTRATO" = :numero_contrato,
                            "VIGENCIA_CONTRATO" = :vigencia
                    """)
                    
                    # Extraer número de llamado y año del formato "numero/año"
                    numero_parts = datos_formulario['numero_anio'].split('/')
                    numero_llamado = numero_parts[0] if len(numero_parts) > 0 else datos_formulario['numero_anio']
                    anio_llamado = numero_parts[1] if len(numero_parts) > 1 else datetime.now().year
                    
                    # Limpiar ID de licitación (quitar puntos de separador de miles)
                    id_licitacion_limpio = str(datos_formulario['id_licitacion']).replace('.', '').replace(',', '')
                    
                    conn.execute(query, {
                        'modalidad': datos_formulario['modalidad'],
                        'id_licitacion': id_licitacion_limpio,
                        'numero_llamado': numero_llamado,
                        'anio_llamado': anio_llamado,
                        'nombre_llamado': datos_formulario['nombre_llamado'],
                        'empresa': datos_formulario['empresa_adjudicada'],
                        'ruc': datos_formulario['ruc'],
                        'fecha_firma': datos_formulario['fecha_firma'],
                        'numero_contrato': datos_formulario['numero_contrato'],
                        'vigencia': datos_formulario['vigencia_contrato']
                    })
                    
                    conn.commit()
                    st.success("✅ Datos del formulario actualizados en la tabla llamado")
                    
            except Exception as e:
                st.warning(f"⚠️ Archivo cargado pero no se pudieron actualizar todos los datos: {e}")
            
            st.balloons()
            
            # Registrar actividad
            registrar_actividad(
                accion="CREATE",
                modulo="LICITACIONES",
                descripcion=f"Archivo cargado: {archivo.name} en esquema {esquema}",
                esquema_afectado=esquema
            )
            
            # Limpiar estado después de éxito
            st.session_state.licitacion_seleccionada = None
            st.session_state.licitacion_data = {}
            st.session_state.datos_confirmados = False
            
            time.sleep(2)
            st.rerun()
        else:
            st.error(message)
    
    elif limpiar:
        # Limpiar estado del formulario
        st.session_state.licitacion_seleccionada = None
        st.session_state.licitacion_data = {}
        st.session_state.datos_confirmados = False
        st.rerun()

def pagina_ver_cargas():
    """Página para ver las cargas realizadas"""
    st.header("Archivos Cargados")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Botón para actualizar manualmente
        if st.button("🔄 Actualizar ahora"):
            st.rerun()
    
    with col2:
        # ⚠️ DESACTIVADO: Opción para actualización automática
        # Causa consumo excesivo de recursos en el navegador
        st.caption("💡 Usa el botón 'Actualizar ahora' para refrescar los datos")
    
    # ⚠️ CÓDIGO DE AUTO-REFRESH DESACTIVADO PARA EVITAR RALENTIZACIÓN
    # if auto_refresh:
    #     # Inicializar contador si no existe
    #     if 'last_refresh_time' not in st.session_state:
    #         st.session_state.last_refresh_time = time.time()
    #     
    #     # Calcular tiempo transcurrido
    #     current_time = time.time()
    #     elapsed = current_time - st.session_state.last_refresh_time
    #     remaining = max(60 - elapsed, 0)
    #     
    #     # Mostrar barra de progreso para tiempo restante
    #     st.progress(elapsed / 60)
    #     st.caption(f"Próxima actualización en {int(remaining)} segundos")
    #     
    #     # Refrescar si ha pasado 1 minuto
    #     if elapsed >= 60:
    #         st.session_state.last_refresh_time = current_time
    #         st.rerun()
    
    # Obtener archivos actualizados
    archivos = obtener_archivos_cargados()
    
    if archivos:
        # Convertir a DataFrame para mejor visualización
        df_archivos = pd.DataFrame(archivos)
        
        # Dar formato a las fechas
        if 'fecha_carga' in df_archivos.columns:
            df_archivos['fecha_carga'] = pd.to_datetime(df_archivos['fecha_carga']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Formatear columnas para el usuario final
        df_archivos_formateado = formatear_columnas_tabla(df_archivos)
        
        # Colorear estado
        def colorear_estado(estado):
            if estado == 'Activo':
                return 'background-color: #d4edda; color: #155724'
            elif estado == 'Eliminado':
                return 'background-color: #f8d7da; color: #721c24'
            else:
                return ''
        
        # Aplicar estilo condicional
        df_styled = df_archivos_formateado.style.applymap(colorear_estado, subset=['Estado'])
        
        # Mostrar DataFrame
        st.dataframe(df_styled, use_container_width=True)
        
        # Ofrecer descarga del contenido original si está activo
        archivos_activos = [a for a in archivos if a['estado'] == 'Activo']
        if archivos_activos:
            archivo_id = st.selectbox(
                "Seleccionar archivo para descargar contenido original:",
                options=[f"{a['nombre_archivo']} ({a['esquema']})" for a in archivos_activos],
                index=None
            )
            
            if archivo_id:
                archivo_seleccionado = next((a for a in archivos_activos if f"{a['nombre_archivo']} ({a['esquema']})" == archivo_id), None)
                
                if archivo_seleccionado:
                    st.info(f"📄 Archivo: **{archivo_seleccionado['nombre_archivo']}**")
                    st.info(f"📂 Esquema: **{archivo_seleccionado['esquema']}**")
                    st.info(f"📅 Fecha de carga: **{archivo_seleccionado['fecha_carga']}**")
                    st.info(f"👤 Usuario: **{archivo_seleccionado['usuario']}**")
        else:
            st.info("No hay archivos activos para mostrar.")
    else:
        st.info("No hay archivos cargados para mostrar.")

def pagina_eliminar_esquemas():
    """Página para eliminar esquemas (licitaciones)"""
    st.header("Eliminar Licitaciones")
    
    st.warning("⚠️ Advertencia: Esta operación eliminará permanentemente todos los datos asociados a la licitación seleccionada.")
    
    # Obtener esquemas existentes
    esquemas = obtener_esquemas_postgres()
    
    if esquemas:
        esquema_a_eliminar = st.selectbox(
            "Seleccionar licitación a eliminar:",
            options=esquemas
        )
        
        if st.button("Eliminar Licitación", type="primary", use_container_width=True):
            # Pedir confirmación
            if st.checkbox("Confirmo que deseo eliminar esta licitación permanentemente"):
                success, message = eliminar_esquema_postgres(esquema_a_eliminar)
                
                if success:
                    st.success(message)
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error(message)
            else:
                st.warning("Debe confirmar la eliminación")
    else:
        st.info("No hay licitaciones para eliminar.")

def pagina_gestionar_proveedores():
    """Página para gestionar proveedores"""
    st.header("Gestión de Proveedores")
    if 'last_page' not in st.session_state:
        st.session_state.last_page = None
    
    # Pestañas para diferentes funciones
    tab1, tab2, tab3 = st.tabs(["Lista de Proveedores", "Nuevo Proveedor", "Importar CSV"])
    
    with tab1:
        st.subheader("Proveedores Registrados")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            filtro_ruc = st.text_input("🔍 Filtrar por RUC:", placeholder="Ej: 80026564")
        with col2:
            filtro_nombre = st.text_input("🔍 Filtrar por Razón Social:", placeholder="Ej: CHACO")
        with col3:
            mostrar_inactivos = st.checkbox("Mostrar inactivos", value=False)
        
        # Obtener proveedores con filtros
        try:
            engine = safe_get_engine()
            if engine is None:
                st.error("No se pudo conectar a la base de datos")
                return
            with engine.connect() as conn:
                query_base = """
                    SELECT id, ruc, razon_social, direccion, correo_electronico, 
                           telefono, contacto_nombre, activo, fecha_registro, fecha_actualizacion
                    FROM oxigeno.proveedores
                    WHERE 1=1
                """
                params = {}
                
                if filtro_ruc:
                    query_base += " AND ruc ILIKE :ruc"
                    params['ruc'] = f"%{filtro_ruc}%"
                
                if filtro_nombre:
                    query_base += " AND razon_social ILIKE :nombre"
                    params['nombre'] = f"%{filtro_nombre}%"
                
                if not mostrar_inactivos:
                    query_base += " AND activo = TRUE"
                
                query_base += " ORDER BY razon_social"
                
                query = text(query_base)
                result = conn.execute(query, params)
                
                proveedores = []
                for row in result:
                    proveedores.append({
                        'id': row[0],
                        'ruc': row[1],
                        'razon_social': row[2],
                        'direccion': row[3] or 'No especificada',
                        'correo_electronico': row[4] or 'No especificado',
                        'telefono': row[5] or 'No especificado',
                        'contacto_nombre': row[6] or 'No especificado',
                        'activo': '✅ Activo' if row[7] else '❌ Inactivo',
                        'fecha_registro': row[8],
                        'fecha_actualizacion': row[9]
                    })
                
                if proveedores:
                    # Convertir a DataFrame para mejor visualización
                    df_proveedores = pd.DataFrame(proveedores)
                    
                    # Dar formato a las fechas
                    if 'fecha_registro' in df_proveedores.columns:
                        df_proveedores['fecha_registro'] = pd.to_datetime(df_proveedores['fecha_registro']).dt.strftime('%Y-%m-%d')
                    if 'fecha_actualizacion' in df_proveedores.columns:
                        df_proveedores['fecha_actualizacion'] = pd.to_datetime(df_proveedores['fecha_actualizacion']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # Mostrar proveedores
                    df_proveedores_formateado = formatear_columnas_tabla(df_proveedores)
                    st.dataframe(df_proveedores_formateado, use_container_width=True)
                    
                    # NUEVA SECCIÓN: Editar Proveedor
                    st.divider()
                    st.subheader("✏️ Editar Datos de Proveedor")
                    
                    # Selector de proveedor para editar
                    proveedor_options = [f"{p['ruc']} - {p['razon_social']}" for p in proveedores]
                    proveedor_seleccionado = st.selectbox(
                        "Seleccionar proveedor para editar:",
                        options=["Seleccionar..."] + proveedor_options
                    )
                    
                    # Inicializar estados para el manejo de eliminación
                    if 'proveedor_a_eliminar' not in st.session_state:
                        st.session_state.proveedor_a_eliminar = None
                    
                    # Si hay un proveedor seleccionado pero no hay confirmación de eliminación en progreso
                    if proveedor_seleccionado != "Seleccionar..." and st.session_state.proveedor_a_eliminar is None:
                        # Encontrar el proveedor seleccionado
                        ruc_seleccionado = proveedor_seleccionado.split(" - ")[0]
                        proveedor = next((p for p in proveedores if p['ruc'] == ruc_seleccionado), None)
                        
                        if proveedor:
                            with st.form("editar_proveedor_form"):
                                st.write(f"**Editando:** {proveedor['razon_social']} ({proveedor['ruc']})")
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    nuevo_ruc = st.text_input("RUC:", value=proveedor['ruc'])
                                    nueva_razon = st.text_input("Razón Social:", value=proveedor['razon_social'])
                                    nueva_direccion = st.text_area("Dirección:", value=proveedor['direccion'] if proveedor['direccion'] != 'No especificada' else '')
                                
                                with col2:
                                    nuevo_correo = st.text_input("Correo Electrónico:", value=proveedor['correo_electronico'] if proveedor['correo_electronico'] != 'No especificado' else '')
                                    nuevo_telefono = st.text_input("Teléfono:", value=proveedor['telefono'] if proveedor['telefono'] != 'No especificado' else '')
                                    nuevo_contacto = st.text_input("Nombre de Contacto:", value=proveedor['contacto_nombre'] if proveedor['contacto_nombre'] != 'No especificado' else '')
                                
                                # Estado activo/inactivo
                                activo_actual = proveedor['activo'] == '✅ Activo'
                                activo = st.checkbox("Proveedor activo", value=activo_actual)
                                
                                observaciones = st.text_area("Observaciones:")
                                
                                # Botones de acción
                                col_btn1, col_btn2, col_btn3 = st.columns(3)
                                with col_btn1:
                                    actualizar = st.form_submit_button("💾 Actualizar Proveedor", type="primary")
                                with col_btn2:
                                    cambiar_estado = st.form_submit_button("🔄 Cambiar Estado", type="secondary")
                                with col_btn3:
                                    eliminar = st.form_submit_button("🗑️ Eliminar Proveedor", type="secondary", help="Esta acción eliminará permanentemente el proveedor")
                                
                                # Procesar eliminación - solo guardamos los datos en session_state
                                if eliminar:
                                    # Guardar datos para eliminar
                                    st.session_state.proveedor_a_eliminar = proveedor['id']
                                    st.session_state.razon_social_eliminar = proveedor['razon_social']
                                    st.session_state.ruc_eliminar = proveedor['ruc']
                                    st.session_state.direccion_eliminar = proveedor['direccion']
                                    st.session_state.correo_eliminar = proveedor['correo_electronico']
                                
                                # Procesar actualización
                                if actualizar:
                                    try:
                                        engine = safe_get_engine()
                                        if engine is None:
                                            st.error("No se pudo conectar a la base de datos")
                                            return
                                        with engine.connect() as conn:
                                            # Verificar si el RUC ya existe en otro proveedor
                                            if nuevo_ruc != proveedor['ruc']:
                                                query_check = text("""
                                                    SELECT COUNT(*) FROM oxigeno.proveedores
                                                    WHERE ruc = :ruc AND id != :id
                                                """)
                                                result_check = conn.execute(query_check, {
                                                    'ruc': nuevo_ruc,
                                                    'id': proveedor['id']
                                                })
                                                count = result_check.scalar()
                                                
                                                if count > 0:
                                                    st.error(f"El RUC '{nuevo_ruc}' ya está registrado para otro proveedor.")
                                                    return
                                            
                                            # Actualizar proveedor
                                            query = text("""
                                                UPDATE oxigeno.proveedores
                                                SET ruc = :ruc, razon_social = :razon_social, direccion = :direccion,
                                                    correo_electronico = :correo, telefono = :telefono,
                                                    contacto_nombre = :contacto, observaciones = :observaciones,
                                                    activo = :activo
                                                WHERE id = :id
                                            """)
                                            
                                            conn.execute(query, {
                                                'ruc': nuevo_ruc,
                                                'razon_social': nueva_razon,
                                                'direccion': nueva_direccion or None,
                                                'correo': nuevo_correo or None,
                                                'telefono': nuevo_telefono or None,
                                                'contacto': nuevo_contacto or None,
                                                'observaciones': observaciones,
                                                'activo': activo,
                                                'id': proveedor['id']
                                            })
                                            
                                            # Hacer commit explícito
                                            conn.commit()
                                            
                                            st.success(f"✅ Proveedor {nueva_razon} actualizado correctamente")
                                            time.sleep(5)  # Esperar más tiempo
                                            st.rerun()
                                    except Exception as e:
                                        st.error(f"Error al actualizar proveedor: {e}")
                                
                                # Procesar cambio de estado
                                if cambiar_estado:
                                    try:
                                        nuevo_estado = not activo_actual
                                        estado_texto = "activo" if nuevo_estado else "inactivo"
                                        
                                        with st.spinner(f"Cambiando estado a {estado_texto}... Por favor espere"):
                                            engine = safe_get_engine()
                                            if engine is None:
                                                st.error("No se pudo conectar a la base de datos")
                                                return
                                            with engine.connect() as conn:
                                                query = text("""
                                                    UPDATE oxigeno.proveedores
                                                    SET activo = :activo
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'activo': nuevo_estado,
                                                    'id': proveedor['id']
                                                })
                                                
                                                # Hacer commit explícito
                                                conn.commit()
                                                
                                                st.success(f"✅ Proveedor {proveedor['razon_social']} marcado como {estado_texto}")
                                                # Esperar más tiempo para que la BD procese el cambio
                                                time.sleep(5)
                                                st.rerun()
                                    except Exception as e:
                                        st.error(f"Error al cambiar estado del proveedor: {e}")
                    
                    # PANTALLA DE CONFIRMACIÓN DE ELIMINACIÓN - FUERA DEL FORMULARIO
                    if st.session_state.proveedor_a_eliminar is not None:
                        st.markdown("---")
                        st.warning("⚠️ **CONFIRMACIÓN DE ELIMINACIÓN REQUERIDA**")
                        
                        confirmar_eliminacion = st.checkbox(
                            f"Confirmo que deseo eliminar permanentemente a '{st.session_state.razon_social_eliminar}' (RUC: {st.session_state.ruc_eliminar})"
                        )
                        
                        if confirmar_eliminacion:
                            st.markdown("### ⬇️ Haga clic en el botón rojo para confirmar la eliminación ⬇️")
                            
                            if st.button("🗑️ ELIMINAR DEFINITIVAMENTE", type="primary", use_container_width=True):
                                try:
                                    with st.spinner("Eliminando proveedor... Por favor espere"):
                                        # Mostrar mensaje de proceso en curso
                                        process_placeholder = st.empty()
                                        process_placeholder.warning("⏳ OPERACIÓN EN PROGRESO - NO INTERRUMPA")
                                        
                                        engine = safe_get_engine()
                                        if engine is None:
                                            st.error("No se pudo conectar a la base de datos")
                                            return
                                        with engine.connect() as conn:
                                            # Proceder con la eliminación
                                            query_delete = text("DELETE FROM oxigeno.proveedores WHERE id = :id")
                                            conn.execute(query_delete, {'id': st.session_state.proveedor_a_eliminar})
                                            conn.commit()  # Hacer commit explícito
                                            
                                            # Registrar actividad de eliminación
                                            registrar_actividad(
                                                accion="DELETE",
                                                modulo="PROVEEDORES",
                                                descripcion=f"Proveedor eliminado: {st.session_state.razon_social_eliminar} (RUC: {st.session_state.ruc_eliminar})",
                                                valores_anteriores={
                                                    'id': st.session_state.proveedor_a_eliminar,
                                                    'ruc': st.session_state.ruc_eliminar,
                                                    'razon_social': st.session_state.razon_social_eliminar,
                                                    'direccion': st.session_state.direccion_eliminar,
                                                    'correo_electronico': st.session_state.correo_eliminar
                                                }
                                            )
                                        
                                        # Reemplazar el mensaje de procesamiento con el de éxito
                                        process_placeholder.empty()
                                        st.success(f"✅ Proveedor '{st.session_state.razon_social_eliminar}' eliminado correctamente")
                                        st.balloons()
                                        
                                        # Limpiar variables de sesión
                                        st.session_state.proveedor_a_eliminar = None
                                        if 'razon_social_eliminar' in st.session_state:
                                            del st.session_state.razon_social_eliminar
                                        if 'ruc_eliminar' in st.session_state:
                                            del st.session_state.ruc_eliminar
                                        if 'direccion_eliminar' in st.session_state:
                                            del st.session_state.direccion_eliminar
                                        if 'correo_eliminar' in st.session_state:
                                            del st.session_state.correo_eliminar
                                        
                                        # Esperar para que el usuario vea el mensaje
                                        time.sleep(3)
                                        st.rerun()
                                        
                                except Exception as e:
                                    st.error(f"Error al eliminar proveedor: {e}")
                        
                        # Botón para cancelar
                        if st.button("❌ Cancelar", type="secondary"):
                            st.session_state.proveedor_a_eliminar = None
                            if 'razon_social_eliminar' in st.session_state:
                                del st.session_state.razon_social_eliminar
                            if 'ruc_eliminar' in st.session_state:
                                del st.session_state.ruc_eliminar
                            if 'direccion_eliminar' in st.session_state:
                                del st.session_state.direccion_eliminar
                            if 'correo_eliminar' in st.session_state:
                                del st.session_state.correo_eliminar
                            st.rerun()
                else:
                    st.info("No hay proveedores registrados que coincidan con los filtros.")
        except Exception as e:
            st.error(f"Error al obtener proveedores: {e}")
    
    # Continuar con el código para las otras pestañas (tab2, tab3)...
    with tab2:
        st.subheader("Registrar Nuevo Proveedor")
        
        with st.form("nuevo_proveedor_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                ruc_proveedor = st.text_input("RUC: *", placeholder="Ej: 80026564-5")
                razon_social = st.text_input("Razón Social: *", placeholder="Ej: EMPRESA S.A.")
                direccion = st.text_area("Dirección:", placeholder="Dirección completa")
            
            with col2:
                correo_electronico = st.text_input("Correo Electrónico:", placeholder="contacto@empresa.com")
                telefono = st.text_input("Teléfono:", placeholder="021-123456")
                contacto_nombre = st.text_input("Nombre de Contacto:", placeholder="Juan Pérez")
            
            observaciones = st.text_area("Observaciones:", placeholder="Información adicional...")
            
            submit = st.form_submit_button("📝 Registrar Proveedor", type="primary")
            
            if submit:
                if not ruc_proveedor or not razon_social:
                    st.error("Por favor, complete los campos obligatorios (RUC y Razón Social).")
                else:
                    try:
                        # Verificar si el RUC ya existe
                        engine = safe_get_engine()
                        if engine is None:
                            st.error("No se pudo conectar a la base de datos")
                            return
                        with engine.connect() as conn:
                            query = text("SELECT COUNT(*) FROM oxigeno.proveedores WHERE ruc = :ruc")
                            result = conn.execute(query, {'ruc': ruc_proveedor})
                            count = result.scalar()
                            
                            if count > 0:
                                st.error(f"El RUC '{ruc_proveedor}' ya está registrado.")
                            else:
                                # Registrar nuevo proveedor
                                query = text("""
                                    INSERT INTO oxigeno.proveedores 
                                    (ruc, razon_social, direccion, correo_electronico, telefono, contacto_nombre, observaciones)
                                    VALUES (:ruc, :razon_social, :direccion, :correo, :telefono, :contacto, :observaciones)
                                """)
                                
                                conn.execute(query, {
                                    'ruc': ruc_proveedor,
                                    'razon_social': razon_social,
                                    'direccion': direccion,
                                    'correo': correo_electronico,
                                    'telefono': telefono,
                                    'contacto': contacto_nombre,
                                    'observaciones': observaciones
                                })
                                
                                # Hacer commit explícito
                                conn.commit()
                                
                                st.success(f"Proveedor '{razon_social}' registrado exitosamente")
                                time.sleep(5)  # Esperar más tiempo
                                st.rerun()
                    except Exception as e:
                        st.error(f"Error al registrar proveedor: {e}")
    
    with tab3:
        st.subheader("Importar Proveedores desde CSV")
        
        st.info("""
        📋 **Formato esperado del CSV:**
        - Columna 1: RUC
        - Columna 2: Razón Social
        - Columna 3: Dirección (opcional)
        - Columna 4: Correo Electrónico (opcional)
        """)
        
        archivo_csv = st.file_uploader("Seleccionar archivo CSV:", type=["csv"])
        
        if archivo_csv is not None:
            st.write(f"**Archivo seleccionado:** {archivo_csv.name}")
            st.write(f"**Tamaño:** {archivo_csv.size} bytes")
            
            # Botón para analizar archivo
            if st.button("🔍 Analizar Archivo"):
                try:
                    # Función mejorada para leer CSV completo
                    def leer_csv_robusto(archivo):
                        """Lee un archivo CSV de forma robusta con múltiples intentos y mejor manejo de encoding"""
                        delimitadores = [',', ';', '\t', '|']
                        # ✅ ORDEN MEJORADO: Priorizar encodings comunes en Paraguay/América Latina
                        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-8-sig', 'windows-1252']
                        
                        mejor_df = None
                        mejor_config = None
                        max_filas = 0
                        
                        # Probar todas las combinaciones y quedarse con la que lee más filas
                        for encoding in encodings:
                            for delimiter in delimitadores:
                                try:
                                    archivo.seek(0)
                                    
                                    # Leer TODO el archivo (sin límite de filas)
                                    df_temp = pd.read_csv(
                                        archivo, 
                                        delimiter=delimiter,
                                        encoding=encoding,
                                        quotechar='"',
                                        skipinitialspace=True,
                                        on_bad_lines='skip',
                                        engine='python',
                                        encoding_errors='replace'  # ✅ Reemplazar caracteres problemáticos
                                    )
                                    
                                    # Verificar que el DataFrame tenga contenido válido
                                    if len(df_temp) > max_filas and len(df_temp.columns) >= 2:
                                        # Verificar que no todas las celdas estén vacías
                                        celdas_no_vacias = df_temp.notna().sum().sum()
                                        
                                        # ✅ NUEVA VALIDACIÓN: Verificar que no haya muchos caracteres raros
                                        texto_muestra = ' '.join(df_temp.astype(str).iloc[0].tolist())
                                        caracteres_raros = texto_muestra.count('Ã') + texto_muestra.count('â')
                                        
                                        # Si hay contenido válido y pocos caracteres raros
                                        if celdas_no_vacias > 0 and caracteres_raros < 5:
                                            mejor_df = df_temp.copy()
                                            mejor_config = (delimiter, encoding)
                                            max_filas = len(df_temp)
                                            break  # Si encontramos una buena lectura, salimos
                                    
                                except Exception:
                                    continue
                            
                            # Si ya encontramos una buena lectura, no seguir probando
                            if mejor_df is not None:
                                break
                        
                        return mejor_df, mejor_config
                    
                    # Intentar leer el archivo completo
                    st.write("**🔄 Analizando archivo completo...**")
                    
                    with st.spinner("Procesando archivo CSV..."):
                        df, config = leer_csv_robusto(archivo_csv)
                    
                    if df is None:
                        st.error("❌ No se pudo leer el archivo con ningún formato estándar.")
                        return
                    
                    delimiter_usado, encoding_usado = config
                    st.success(f"✅ Archivo leído correctamente (Delimitador: '{delimiter_usado}', Encoding: {encoding_usado})")
                    
                    # Limpiar DataFrame
                    df_original = df.copy()
                    df = df.dropna(how='all')
                    df = df.fillna('')
                    
                    # Información detallada del archivo
                    st.write("### 📊 Información del Archivo")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📝 Filas Totales", len(df_original))
                    with col2:
                        st.metric("✅ Filas Válidas", len(df))
                    with col3:
                        st.metric("📋 Columnas", len(df.columns))
                    with col4:
                        celdas_con_datos = df.astype(str).ne('').sum().sum()
                        st.metric("📊 Celdas con Datos", celdas_con_datos)
                    
                    # Guardar DataFrame procesado en session_state
                    st.session_state.df_importar = df
                    st.session_state.delimiter_usado = delimiter_usado
                    st.session_state.encoding_usado = encoding_usado
                    st.session_state.filas_originales = len(df_original)
                    st.session_state.celdas_con_datos = celdas_con_datos
                    
                    st.success(f"✅ Archivo analizado completamente: {len(df):,} filas listas para mapeo")
                    
                except Exception as e:
                    st.error(f"❌ Error al procesar el archivo: {e}")
            
            # Mostrar mapeo y importación si el DataFrame está disponible
            if 'df_importar' in st.session_state:
                df = st.session_state.df_importar
                
                st.divider()
                st.subheader("🗂️ Mapeo de Columnas")
                
                # Autodetección inteligente de columnas
                def detectar_columna_ruc(columnas):
                    for i, col in enumerate(columnas):
                        col_lower = str(col).lower()
                        if any(palabra in col_lower for palabra in ['ruc', 'rut', 'cuit', 'nit', 'documento']):
                            return i
                    return 0
                
                def detectar_columna_razon(columnas):
                    for i, col in enumerate(columnas):
                        col_lower = str(col).lower()
                        if any(palabra in col_lower for palabra in ['razon', 'nombre', 'empresa', 'social']):
                            return i
                    return 1 if len(columnas) > 1 else 0
                
                ruc_idx = detectar_columna_ruc(df.columns.tolist())
                razon_idx = detectar_columna_razon(df.columns.tolist())
                
                col1, col2 = st.columns(2)
                with col1:
                    col_ruc = st.selectbox("Columna RUC:", options=df.columns.tolist(), index=ruc_idx)
                    col_razon = st.selectbox("Columna Razón Social:", options=df.columns.tolist(), index=razon_idx)
                
                with col2:
                    opciones_direccion = ["No mapear"] + df.columns.tolist()
                    col_direccion = st.selectbox("Columna Dirección (opcional):", options=opciones_direccion)
                    
                    opciones_correo = ["No mapear"] + df.columns.tolist()
                    col_correo = st.selectbox("Columna Correo (opcional):", options=opciones_correo)
                
                # Análisis de datos antes de importar
                registros_procesables = df[(df[col_ruc].astype(str).str.strip().ne('')) & 
                                         (df[col_razon].astype(str).str.strip().ne(''))].shape[0]
                
                # Checkbox para confirmar importación
                confirmar_importacion = st.checkbox(
                    f"Confirmo la importación de {registros_procesables:,} registros procesables",
                    help="Esta acción insertará todos los registros válidos en la base de datos"
                )
                
                # Importación
                if confirmar_importacion:
                    if st.button("🚀 Importar Proveedores", type="primary"):
                        try:
                            insertados = 0
                            duplicados = 0
                            errores = 0
                            errores_detalle = []
                            
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            total_filas = len(df)
                            
                            for index, row in df.iterrows():
                                try:
                                    progress = (index + 1) / total_filas
                                    progress_bar.progress(progress)
                                    status_text.text(f'Procesando fila {index + 1} de {total_filas}...')
                                    
                                    ruc = str(row[col_ruc]).strip()
                                    razon_social = str(row[col_razon]).strip()
                                    direccion = str(row[col_direccion]).strip() if col_direccion != "No mapear" and pd.notna(row[col_direccion]) else None
                                    correo = str(row[col_correo]).strip() if col_correo != "No mapear" and pd.notna(row[col_correo]) else None
                                    
                                    if not ruc or not razon_social or ruc.lower() == "nan" or razon_social.lower() == "nan":
                                        errores += 1
                                        errores_detalle.append(f"Fila {index + 1}: RUC o Razón Social vacíos")
                                        continue
                                    
                                    engine = safe_get_engine()
                                    if engine is None:
                                        st.error("No se pudo conectar a la base de datos")
                                        return
                                    with engine.connect() as conn:
                                        trans = conn.begin()
                                        try:
                                            query = text("""
                                                INSERT INTO oxigeno.proveedores (ruc, razon_social, direccion, correo_electronico)
                                                VALUES (:ruc, :razon_social, :direccion, :correo)
                                                ON CONFLICT (ruc) DO NOTHING
                                                RETURNING id
                                            """)
                                            
                                            result = conn.execute(query, {
                                                'ruc': ruc,
                                                'razon_social': razon_social,
                                                'direccion': direccion,
                                                'correo': correo
                                            })
                                            
                                            if result.rowcount > 0:
                                                trans.commit()
                                                insertados += 1
                                            else:
                                                trans.rollback()
                                                duplicados += 1
                                                errores_detalle.append(f"Fila {index + 1}: RUC {ruc} ya existe")
                                            
                                        except Exception as e:
                                            trans.rollback()
                                            errores += 1
                                            errores_detalle.append(f"Fila {index + 1}: Error - {str(e)}")
                                
                                except Exception as e:
                                    errores += 1
                                    errores_detalle.append(f"Fila {index + 1}: Error general - {str(e)}")
                            
                            progress_bar.empty()
                            status_text.empty()
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("✅ Insertados", insertados)
                            with col2:
                                st.metric("⚠️ Duplicados", duplicados)
                            with col3:
                                st.metric("❌ Errores", errores)
                            
                            if insertados > 0:
                                st.success(f"✅ Importación completada: {insertados} nuevos registros")
                                registrar_actividad(
                                    accion="IMPORT",
                                    modulo="PROVEEDORES",
                                    descripcion=f"Importación CSV: {insertados} insertados, {duplicados} duplicados, {errores} errores"
                                )
                            
                            if 'df_importar' in st.session_state:
                                del st.session_state.df_importar
                                del st.session_state.delimiter_usado
                                del st.session_state.encoding_usado
                                
                        except Exception as e:
                            st.error(f"Error durante la importación: {e}")

def eliminar_proveedor_bulk():
    """Función para eliminar múltiples proveedores (admin only)"""
    if st.session_state.user_role != 'admin':
        st.error("❌ Solo administradores pueden realizar eliminaciones masivas")
        return
    
    st.subheader("🗑️ Eliminación Masiva de Proveedores")
    st.warning("⚠️ **FUNCIÓN ADMINISTRATIVA** - Use con extrema precaución")
    
    try:
        engine = safe_get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            # Diagnóstico: mostrar el número de proveedores inactivos
            diag_query = text("""
                SELECT COUNT(*) 
                FROM oxigeno.proveedores 
                WHERE activo = FALSE
            """)
            diag_result = conn.execute(diag_query)
            count_inactivos = diag_result.scalar()
            
            if count_inactivos > 0:
                st.info(f"Diagnóstico: Se encontraron {count_inactivos} proveedores inactivos en la base de datos")
            else:
                st.warning("⚠️ No hay proveedores inactivos en la base de datos.")
                st.info("Para inactivar un proveedor, vaya a la pestaña 'Lista de Proveedores', seleccione un proveedor y haga clic en 'Cambiar Estado'.")
                return
            
            # Obtener proveedores inactivos
            query = text("""
                SELECT p.id, p.ruc, p.razon_social, p.fecha_registro
                FROM oxigeno.proveedores p
                WHERE p.activo = FALSE
                ORDER BY p.fecha_registro DESC
            """)
            
            result = conn.execute(query)
            proveedores_eliminables = [
                {'id': row[0], 'ruc': row[1], 'razon_social': row[2], 'fecha_registro': row[3]}
                for row in result
            ]
            
            if proveedores_eliminables:
                st.success(f"📊 Se encontraron {len(proveedores_eliminables)} proveedores inactivos que pueden eliminarse:")
                
                # Mostrar lista
                df_eliminables = pd.DataFrame(proveedores_eliminables)
                df_eliminables['fecha_registro'] = pd.to_datetime(df_eliminables['fecha_registro']).dt.strftime('%Y-%m-%d')
                df_eliminables_formateado = formatear_columnas_tabla(df_eliminables)
                st.dataframe(df_eliminables_formateado, use_container_width=True)
                
                # Opción de eliminación masiva
                if st.checkbox("Confirmo que deseo eliminar TODOS los proveedores inactivos listados"):
                    if st.button("🗑️ ELIMINAR TODOS LOS INACTIVOS", type="primary"):
                        try:
                            eliminados = 0
                            for proveedor in proveedores_eliminables:
                                query_delete = text("DELETE FROM oxigeno.proveedores WHERE id = :id")
                                conn.execute(query_delete, {'id': proveedor['id']})
                                eliminados += 1
                            
                            # Registrar actividad masiva
                            registrar_actividad(
                                accion="DELETE",
                                modulo="PROVEEDORES",
                                descripcion=f"Eliminación masiva: {eliminados} proveedores inactivos eliminados",
                                detalles={
                                    'cantidad_eliminados': eliminados,
                                    'tipo': 'eliminacion_masiva_inactivos'
                                }
                            )
                            
                            st.success(f"✅ {eliminados} proveedores eliminados correctamente")
                            st.balloons()
                            time.sleep(2)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Error en eliminación masiva: {e}")
            else:
                st.warning("⚠️ No se encontraron proveedores inactivos para eliminar.")
                
    except Exception as e:
        st.error(f"Error obteniendo proveedores eliminables: {e}")

def pagina_administrar_usuarios():
    """Página para administrar usuarios"""
    st.header("Administrar Usuarios")
    
    # Pestañas para diferentes funciones
    tab1, tab2, tab3 = st.tabs(["Lista de Usuarios", "Crear Usuario", "Asignar Servicios"])
    
    with tab1:
        st.subheader("Usuarios del Sistema")
        
        # Obtener usuarios
        try:
            engine = safe_get_engine()
            if engine is None:
                st.error("No se pudo conectar a la base de datos")
                return
            with engine.connect() as conn:
                query = text("""
                    SELECT id, cedula, username, nombre_completo, role, fecha_creacion, ultimo_cambio_password
                    FROM public.usuarios
                    ORDER BY username
                """)
                
                result = conn.execute(query)
                
                usuarios = []
                for row in result:
                    usuarios.append({
                        'id': row[0],
                        'cedula': row[1],
                        'username': row[2],
                        'nombre_completo': row[3],
                        'role': row[4],
                        'fecha_creacion': row[5],
                        'ultimo_cambio_password': row[6]
                    })
                
                if usuarios:
                    # Convertir a DataFrame para mejor visualización
                    df_usuarios = pd.DataFrame(usuarios)
                    
                    # Dar formato a las fechas
                    if 'fecha_creacion' in df_usuarios.columns:
                        df_usuarios['fecha_creacion'] = pd.to_datetime(df_usuarios['fecha_creacion']).dt.strftime('%Y-%m-%d %H:%M')
                    if 'ultimo_cambio_password' in df_usuarios.columns:
                        df_usuarios['ultimo_cambio_password'] = pd.to_datetime(df_usuarios['ultimo_cambio_password']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # Formatear columnas para el usuario final
                    df_usuarios_formateado = formatear_columnas_tabla(df_usuarios)
                    
                    # Mostrar usuarios
                    st.dataframe(df_usuarios_formateado, use_container_width=True)
                    
                    # Selector para editar usuario
                    usuario_a_editar = st.selectbox(
                        "Seleccionar usuario para editar:",
                        options=[u['username'] for u in usuarios]
                    )
                    
                    usuario = next((u for u in usuarios if u['username'] == usuario_a_editar), None)
                    
                    if usuario:
                        with st.form("editar_usuario_form"):
                            st.subheader(f"Editar Usuario: {usuario['username']}")
                            
                            # Campos para editar
                            cedula = st.text_input("Cédula de Identidad:", value=usuario['cedula'])
                            nombre = st.text_input("Nombre completo:", value=usuario['nombre_completo'])
                            rol = st.selectbox(
                                "Rol:",
                                options=["admin", "user"],
                                index=0 if usuario['role'] == "admin" else 1
                            )
                            reset_password = st.checkbox("Resetear contraseña")
                            new_password = st.text_input("Nueva contraseña:", type="password") if reset_password else None
                            
                            # Botón para actualizar
                            submit = st.form_submit_button("Actualizar Usuario")
                            
                            if submit:
                                try:
                                    engine = get_engine()
                                    if engine is None:
                                        st.error("No se pudo conectar a la base de datos")
                                        return
                                    with engine.connect() as conn:
                                        # Iniciar transacción
                                        trans = conn.begin()
                                        try:
                                            # Verificar si la cédula ya existe en otro usuario
                                            if cedula != usuario['cedula']:
                                                query_check = text("""
                                                    SELECT COUNT(*) FROM public.usuarios
                                                    WHERE cedula = :cedula AND id != :id
                                                """)
                                                result_check = conn.execute(query_check, {
                                                    'cedula': cedula,
                                                    'id': usuario['id']
                                                })
                                                count = result_check.scalar()
                                                
                                                if count > 0:
                                                    st.error(f"La cédula '{cedula}' ya está asignada a otro usuario.")
                                                    trans.rollback()
                                                    return
                                            
                                            # Actualizar usuario
                                            if reset_password and new_password:
                                                # Actualizar con nueva contraseña
                                                password_hash = hashlib.sha256(new_password.encode()).hexdigest()
                                                
                                                query = text("""
                                                    UPDATE public.usuarios
                                                    SET cedula = :cedula, nombre_completo = :nombre, role = :rol, 
                                                        password = :password, ultimo_cambio_password = CURRENT_TIMESTAMP
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'cedula': cedula,
                                                    'nombre': nombre,
                                                    'rol': rol,
                                                    'password': password_hash,
                                                    'id': usuario['id']
                                                })
                                            else:
                                                # Actualizar sin cambiar contraseña
                                                query = text("""
                                                    UPDATE public.usuarios
                                                    SET cedula = :cedula, nombre_completo = :nombre, role = :rol
                                                    WHERE id = :id
                                                """)
                                                
                                                conn.execute(query, {
                                                    'cedula': cedula,
                                                    'nombre': nombre,
                                                    'rol': rol,
                                                    'id': usuario['id']
                                                })
                                            
                                            # Confirmar transacción
                                            trans.commit()
                                            
                                            st.success(f"Usuario {usuario['username']} actualizado correctamente")
                                            time.sleep(1)
                                            st.rerun()
                                            
                                        except Exception as e:
                                            # Revertir transacción en caso de error
                                            trans.rollback()
                                            raise e
                                            
                                except Exception as e:
                                    st.error(f"Error al actualizar usuario: {e}")
                else:
                    st.info("No hay usuarios para mostrar.")
        except Exception as e:
            st.error(f"Error al obtener usuarios: {e}")
    
    with tab2:
        st.subheader("Crear Nuevo Usuario")
        
        with st.form("nuevo_usuario_form"):
            cedula = st.text_input("Cédula de Identidad:")
            username = st.text_input("Nombre de usuario:")
            password = st.text_input("Contraseña:", type="password")
            nombre = st.text_input("Nombre completo:")
            rol = st.selectbox(
                "Rol:",
                options=["user", "admin"]
            )
            requiere_cambio = st.checkbox("Requerir cambio de contraseña en próximo inicio de sesión", value=True)
            
            submit = st.form_submit_button("Crear Usuario")
            
            if submit:
                if not cedula or not username or not password or not nombre:
                    st.error("Por favor, complete todos los campos.")
                else:
                    try:
                        # Verificar si el usuario o cédula ya existen
                        engine = get_engine()
                        if engine is None:
                            st.error("No se pudo conectar a la base de datos")
                            return
                        with engine.connect() as conn:
                            query = text("""
                                SELECT 
                                    (SELECT COUNT(*) FROM public.usuarios WHERE username = :username) as count_username,
                                    (SELECT COUNT(*) FROM public.usuarios WHERE cedula = :cedula) as count_cedula
                            """)
                            result = conn.execute(query, {'username': username, 'cedula': cedula})
                            counts = result.fetchone()
                            
                            if counts[0] > 0:
                                st.error(f"El nombre de usuario '{username}' ya existe.")
                            elif counts[1] > 0:
                                st.error(f"La cédula '{cedula}' ya está registrada.")
                            else:
                                # Crear nuevo usuario
                                password_hash = hashlib.sha256(password.encode()).hexdigest()
                                
                                query = text("""
                                    INSERT INTO public.usuarios 
                                    (cedula, username, password, nombre_completo, role, ultimo_cambio_password)
                                    VALUES (:cedula, :username, :password, :nombre, :rol, 
                                           CASE WHEN :requiere_cambio THEN NULL ELSE CURRENT_TIMESTAMP END)
                                """)
                                
                                conn.execute(query, {
                                    'cedula': cedula,
                                    'username': username,
                                    'password': password_hash,
                                    'nombre': nombre,
                                    'rol': rol,
                                    'requiere_cambio': requiere_cambio
                                })
                                conn.commit()
                                
                                # Registrar actividad
                                registrar_actividad(
                                    accion="CREATE",
                                    modulo="USUARIOS",
                                    descripcion=f"Usuario {username} creado exitosamente"
                                )
                                
                                st.success(f"Usuario '{username}' creado exitosamente")
                                time.sleep(1)
                                st.rerun()
                    except Exception as e:
                        st.error(f"Error al crear usuario: {e}")

    with tab3:
        st.subheader("🏥 Asignar Servicios a Usuarios")
        
        # Crear tabla si no existe
        crear_tabla_usuario_servicio()
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Obtener usuarios
            try:
                engine = get_engine()
                if engine is None:
                    st.error("No se pudo conectar a la base de datos")
                    return
                with engine.connect() as conn:
                    query = text("""
                        SELECT id, username, nombre_completo, role 
                        FROM public.usuarios 
                        ORDER BY username
                    """)
                    result = conn.execute(query)
                    usuarios = []
                    for row in result:
                        usuarios.append({
                            'id': row[0],
                            'username': row[1],
                            'nombre': row[2],
                            'role': row[3]
                        })
                    
                    if usuarios:
                        usuario_sel = st.selectbox(
                            "Seleccionar usuario:",
                            options=usuarios,
                            format_func=lambda x: f"{x['username']} - {x['nombre']} ({x['role']})"
                        )
                        
                        if usuario_sel:
                            st.write("**Servicios actuales:**")
                            servicios_actuales = obtener_servicios_usuario(usuario_sel['id'])
                            
                            if servicios_actuales:
                                for srv in servicios_actuales:
                                    col_srv, col_btn = st.columns([4, 1])
                                    with col_srv:
                                        permisos = []
                                        if srv['puede_crear_oc']:
                                            permisos.append("📄 OC")
                                        if srv['puede_crear_acta']:
                                            permisos.append("📋 Acta")
                                        st.write(f"• {srv['servicio']} ({', '.join(permisos)})")
                                    with col_btn:
                                        if st.button("❌", key=f"del_{srv['servicio']}_{usuario_sel['id']}"):
                                            if quitar_servicio_usuario(usuario_sel['id'], srv['servicio']):
                                                st.success("Servicio eliminado")
                                                time.sleep(1)
                                                st.rerun()
                            else:
                                st.info("Sin servicios asignados")
                    else:
                        st.warning("No hay usuarios registrados")
                        
            except Exception as e:
                st.error(f"Error al obtener usuarios: {e}")
        
        with col2:
            if 'usuario_sel' in locals() and usuario_sel:
                st.write("**Asignar nuevo servicio:**")
                
                servicios_disponibles = obtener_servicios_disponibles()
                
                if servicios_disponibles:
                    # Filtrar servicios ya asignados
                    servicios_actuales_nombres = [s['servicio'] for s in servicios_actuales]
                    servicios_no_asignados = [s for s in servicios_disponibles if s not in servicios_actuales_nombres]
                    
                    if servicios_no_asignados:
                        servicio_nuevo = st.selectbox(
                            "Servicio:",
                            options=servicios_no_asignados
                        )
                        
                        st.write("**Permisos:**")
                        puede_oc = st.checkbox("📄 Puede crear Órdenes de Compra", value=True)
                        puede_acta = st.checkbox("📋 Puede crear Actas de Recepción", value=True)
                        
                        if st.button("➕ Asignar Servicio", use_container_width=True):
                            if asignar_servicio_usuario(usuario_sel['id'], servicio_nuevo, puede_oc, puede_acta):
                                st.success(f"Servicio '{servicio_nuevo}' asignado correctamente")
                                registrar_actividad(
                                    accion="UPDATE",
                                    modulo="USUARIOS",
                                    descripcion=f"Servicio {servicio_nuevo} asignado a usuario {usuario_sel['username']}"
                                )
                                time.sleep(1)
                                st.rerun()
                    else:
                        st.info("Todos los servicios ya están asignados a este usuario")
                else:
                    st.warning("No hay servicios disponibles en las licitaciones cargadas")

def pagina_historial_actividades():
    """Página para mostrar el historial de actividades del sistema"""
    st.header("📋 Historial de Actividades del Sistema")
    
    # Filtros
    with st.expander("🔍 Filtros de Búsqueda", expanded=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            try:
                engine = get_engine()
                if engine is None:
                    st.error("No se pudo conectar a la base de datos")
                    return
                with engine.connect() as conn:
                    query = text("SELECT id, username, nombre_completo FROM public.usuarios ORDER BY username")
                    result = conn.execute(query)
                    usuarios = [{'id': row[0], 'username': row[1], 'nombre': row[2]} for row in result]
                    
                usuario_filtro = st.selectbox(
                    "Filtrar por Usuario:",
                    options=[None] + [u['id'] for u in usuarios],
                    format_func=lambda x: "Todos los usuarios" if x is None else f"{next(u['username'] for u in usuarios if u['id'] == x)} ({next(u['nombre'] for u in usuarios if u['id'] == x)})"
                )
            except:
                usuario_filtro = None
        
        with col2:
            modulos = ["PROVEEDORES", "ORDENES_COMPRA", "USUARIOS", "LICITACIONES", "ARCHIVOS", "LOGIN"]
            modulo_filtro = st.selectbox(
                "Filtrar por Módulo:",
                options=[None] + modulos,
                format_func=lambda x: "Todos los módulos" if x is None else x
            )
        
        with col3:
            acciones = ["CREATE", "UPDATE", "DELETE", "LOGIN", "LOGOUT", "IMPORT", "EXPORT"]
            accion_filtro = st.selectbox(
                "Filtrar por Acción:",
                options=[None] + acciones,
                format_func=lambda x: "Todas las acciones" if x is None else x
            )
        
        col4, col5, col6 = st.columns(3)
        with col4:
            fecha_desde = st.date_input("Fecha desde:", value=None)
        with col5:
            fecha_hasta = st.date_input("Fecha hasta:", value=None)
        with col6:
            limite = st.number_input("Máximo registros:", min_value=10, max_value=1000, value=100, step=10)
    
    if st.button("🔍 Aplicar Filtros"):
        st.rerun()
    
    actividades = obtener_historial_actividades(
        limite=limite,
        usuario_id=usuario_filtro,
        modulo=modulo_filtro,
        accion=accion_filtro,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta
    )
    
    if actividades:
        st.subheader(f"📊 Mostrando {len(actividades)} actividades")
        
        df_actividades = pd.DataFrame(actividades)
        df_actividades['fecha_hora'] = pd.to_datetime(df_actividades['fecha_hora']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Formatear columnas usando la función de formateo
        df_actividades_formateado = formatear_columnas_tabla(df_actividades, {
            'fecha_hora': 'Fecha/Hora',
            'usuario': 'Usuario',
            'modulo': 'Módulo',
            'accion': 'Acción',
            'descripcion': 'Descripción',
            'esquema_afectado': 'Esquema Afectado'
        })
        
        # Seleccionar solo las columnas que queremos mostrar
        columnas_mostrar = ['Fecha/Hora', 'Usuario', 'Módulo', 'Acción', 'Descripción', 'Esquema Afectado']
        df_display = df_actividades_formateado[columnas_mostrar]
        
        def colorear_accion(val):
            if val == 'CREATE':
                return 'background-color: #d4edda; color: #155724'
            elif val == 'UPDATE':
                return 'background-color: #fff3cd; color: #856404'
            elif val == 'DELETE':
                return 'background-color: #f8d7da; color: #721c24'
            elif val == 'LOGIN':
                return 'background-color: #d1ecf1; color: #0c5460'
            else:
                return ''
        
        df_styled = df_display.style.applymap(colorear_accion, subset=['Acción'])
        st.dataframe(df_styled, use_container_width=True)
        
        st.subheader("🔍 Detalles de Actividad")
        actividad_seleccionada = st.selectbox(
            "Seleccionar actividad para ver detalles:",
            options=range(len(actividades)),
            format_func=lambda x: f"{actividades[x]['fecha_hora'].strftime('%Y-%m-%d %H:%M')} - {actividades[x]['usuario']} - {actividades[x]['descripcion']}"
        )
        
        if actividad_seleccionada is not None:
            actividad = actividades[actividad_seleccionada]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Información General:**")
                st.write(f"• **Usuario:** {actividad['usuario']}")
                st.write(f"• **Fecha/Hora:** {actividad['fecha_hora'].strftime('%Y-%m-%d %H:%M:%S')}")
                st.write(f"• **Módulo:** {actividad['modulo']}")
                st.write(f"• **Acción:** {actividad['accion']}")
                st.write(f"• **Descripción:** {actividad['descripcion']}")
                if actividad['esquema_afectado']:
                    st.write(f"• **Esquema Afectado:** {actividad['esquema_afectado']}")
            
            with col2:
                if actividad['detalles']:
                    st.write("**Detalles Adicionales:**")
                    st.json(actividad['detalles'])
                
                if actividad['valores_anteriores'] or actividad['valores_nuevos']:
                    st.write("**Cambios Realizados:**")
                    
                    if actividad['valores_anteriores']:
                        st.write("*Valores Anteriores:*")
                        st.json(actividad['valores_anteriores'])
                    
                    if actividad['valores_nuevos']:
                        st.write("*Valores Nuevos:*")
                        st.json(actividad['valores_nuevos'])
        
        st.subheader("📤 Exportar Historial")
        if st.button("Descargar Historial como CSV"):
            csv = df_display.to_csv(index=False)
            st.download_button(
                label="📥 Descargar CSV",
                data=csv.encode('utf-8'),
                file_name=f"historial_actividades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    else:
        st.info("No se encontraron actividades con los filtros aplicados.")
    
    # Estadísticas del sistema
    with st.expander("📊 Estadísticas del Sistema"):
        try:
            engine = get_engine()
            if engine is None:
                st.error("No se pudo conectar a la base de datos")
                return
            with engine.connect() as conn:
                # Actividades por usuario
                query = text("""
                    SELECT usuario_nombre, COUNT(*) as total
                    FROM oxigeno.auditoria
                    GROUP BY usuario_nombre
                    ORDER BY total DESC
                    LIMIT 10
                """)
                result = conn.execute(query)
                
                st.write("**Top 10 Usuarios más Activos:**")
                for row in result:
                    st.write(f"• {row[0]}: {row[1]} actividades")
                
                # Actividades por módulo
                query = text("""
                    SELECT modulo, COUNT(*) as total
                    FROM oxigeno.auditoria
                    GROUP BY modulo
                    ORDER BY total DESC
                """)
                result = conn.execute(query)
                
                st.write("**Actividades por Módulo:**")
                for row in result:
                    st.write(f"• {row[0]}: {row[1]} actividades")
                    
        except Exception as e:
            st.error(f"Error obteniendo estadísticas: {e}")

def pagina_cambiar_password():
    """Página para cambiar contraseña del usuario actual"""
    st.header("Cambiar Contraseña")
    
    # Obtener engine
    engine = get_engine()
    if engine is None:
        st.error("No se pudo conectar a la base de datos")
        return
    
    # Verificar si el usuario debe cambiar su contraseña
    # Si es API REST, usar execute_query
    if isinstance(engine, dict) and engine.get('type') == 'api_rest':
        query = """
            SELECT ultimo_cambio_password 
            FROM public.usuarios
            WHERE id = :user_id
        """
        result = execute_query(query, params={'user_id': st.session_state.user_id}, fetch_one=True)
        if result:
            ultimo_cambio = result.get('ultimo_cambio_password') if isinstance(result, dict) else result[0] if isinstance(result, tuple) else None
        else:
            ultimo_cambio = None
    else:
        # Conexión directa
        with engine.connect() as conn:
            query = text("""
                SELECT ultimo_cambio_password 
                FROM public.usuarios
                WHERE id = :user_id
            """)
            
            result = conn.execute(query, {'user_id': st.session_state.user_id})
            ultimo_cambio = result.scalar()
    
    if ultimo_cambio is None:
        st.warning("⚠️ Se requiere cambiar su contraseña. Por favor, establezca una nueva contraseña para continuar.")
    
    with st.form("cambiar_password_form"):
        password_actual = st.text_input("Contraseña actual:", type="password")
        password_nueva = st.text_input("Nueva contraseña:", type="password")
        password_confirmar = st.text_input("Confirmar nueva contraseña:", type="password")
        
        # Agregar reglas de validación para contraseñas
        if password_nueva:
            col1, col2 = st.columns(2)
            with col1:
                if len(password_nueva) >= 8:
                    st.success("✅ Mínimo 8 caracteres")
                else:
                    st.error("❌ Mínimo 8 caracteres")
            
            with col2:
                if any(c.isdigit() for c in password_nueva):
                    st.success("✅ Al menos un número")
                else:
                    st.error("❌ Al menos un número")
        
        # ✅ AGREGAR EL BOTÓN DE SUBMIT - ESTE ES EL QUE FALTABA
        submit = st.form_submit_button("Cambiar Contraseña")
        
        if submit:
            if not password_actual or not password_nueva or not password_confirmar:
                st.error("Por favor, complete todos los campos.")
            elif password_nueva != password_confirmar:
                st.error("Las contraseñas no coinciden.")
            elif len(password_nueva) < 8:
                st.error("La contraseña debe tener al menos 8 caracteres.")
            elif not any(c.isdigit() for c in password_nueva):
                st.error("La contraseña debe contener al menos un número.")
            else:
                try:
                    # Verificar contraseña actual
                    password_hash_actual = hashlib.sha256(password_actual.encode()).hexdigest()
                    
                    # engine ya está definido al inicio de la función
                    if engine is None:
                        st.error("No se pudo conectar a la base de datos")
                        return
                    
                    # Si es API REST, usar execute_query
                    if isinstance(engine, dict) and engine.get('type') == 'api_rest':
                        # Verificar contraseña actual
                        query = """
                            SELECT COUNT(*) 
                            FROM public.usuarios
                            WHERE id = :user_id AND password = :password
                        """
                        result = execute_query(query, params={
                            'user_id': st.session_state.user_id,
                            'password': password_hash_actual
                        }, fetch_one=True)
                        
                        # execute_query con COUNT(*) retorna un número directamente
                        if isinstance(result, (int, float)):
                            count = int(result)
                        elif isinstance(result, dict):
                            count = result.get('count', 0) if 'count' in result else (1 if result else 0)
                        elif isinstance(result, tuple):
                            count = result[0] if result else 0
                        elif result is None:
                            count = 0
                        else:
                            # Si es una lista, contar elementos
                            count = len(result) if isinstance(result, list) else (1 if result else 0)
                        
                        if count == 0:
                            st.error("La contraseña actual es incorrecta.")
                        else:
                            # Actualizar contraseña usando API REST
                            password_hash_nueva = hashlib.sha256(password_nueva.encode()).hexdigest()
                            
                            client = engine.get('client')
                            if client:
                                # Usar Supabase API para actualizar
                                response = client.table('usuarios').update({
                                    'password': password_hash_nueva,
                                    'ultimo_cambio_password': 'now()'
                                }).eq('id', st.session_state.user_id).execute()
                                
                                if response.data:
                                    st.success("✅ Contraseña actualizada correctamente.")
                                    
                                    # Si se requería cambio de contraseña, actualizar el estado de la sesión
                                    if 'requiere_cambio_password' in st.session_state and st.session_state.requiere_cambio_password:
                                        st.session_state.requiere_cambio_password = False
                                        st.info("Ya puede acceder a todas las funcionalidades del sistema.")
                                        time.sleep(2)
                                        st.rerun()
                                else:
                                    st.error("No se pudo actualizar la contraseña.")
                    else:
                        # Conexión directa
                        with engine.connect() as conn:
                            query = text("""
                                SELECT COUNT(*) 
                                FROM public.usuarios
                                WHERE id = :user_id AND password = :password
                            """)
                            
                            result = conn.execute(query, {
                                'user_id': st.session_state.user_id,
                                'password': password_hash_actual
                            })
                            
                            count = result.scalar()
                            
                            if count == 0:
                                st.error("La contraseña actual es incorrecta.")
                            else:
                                # Actualizar contraseña
                                password_hash_nueva = hashlib.sha256(password_nueva.encode()).hexdigest()
                                
                                query = text("""
                                    UPDATE public.usuarios
                                    SET password = :password, ultimo_cambio_password = CURRENT_TIMESTAMP
                                    WHERE id = :user_id
                                """)
                                
                                conn.execute(query, {
                                    'password': password_hash_nueva,
                                    'user_id': st.session_state.user_id
                                })
                                
                                conn.commit()  # ✅ Hacer commit explícito
                                
                                st.success("Contraseña cambiada exitosamente.")
                                
                                # Si se requería cambio de contraseña, actualizar el estado de la sesión
                                if 'requiere_cambio_password' in st.session_state and st.session_state.requiere_cambio_password:
                                    st.session_state.requiere_cambio_password = False
                                    st.info("Ya puede acceder a todas las funcionalidades del sistema.")
                                    time.sleep(2)
                                    st.rerun()
                except Exception as e:
                    st.error(f"Error al cambiar contraseña: {e}")

def obtener_datos_items(esquema, servicio=None):
    """Obtiene los datos de los items disponibles para generar órdenes de compra"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            # Consulta para obtener items disponibles desde ejecucion_general
            query = text(f"""
                SELECT 
                    "LOTE",
                    "ITEM",
                    "CODIGO_EMISOR",
                    "CODIGO_EMISOR",
                    "SERVICIO_BENEFICIARIO",
                    "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                    "UNIDAD_DE_MEDIDA",
                    "PRECIO_UNITARIO",
                    "CANTIDAD_MAXIMA",
                    "CANTIDAD_EMITIDA",
                    "SALDO_A_EMITIR"
                FROM "{esquema}"."ejecucion_general"
                WHERE "SALDO_A_EMITIR" > 0
            """)
            
            # Si se especifica un servicio, filtrar por ese servicio
            if servicio:
                query = text(f"""
                    SELECT 
                        "LOTE",
                        "ITEM",
                        "CODIGO_EMISOR",
                        "CODIGO_EMISOR",
                        "SERVICIO_BENEFICIARIO",
                        "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                        "UNIDAD_DE_MEDIDA",
                        "PRECIO_UNITARIO",
                        "CANTIDAD_MAXIMA",
                        "CANTIDAD_EMITIDA",
                        "SALDO_A_EMITIR"
                    FROM "{esquema}"."ejecucion_general"
                    WHERE "SALDO_A_EMITIR" > 0
                    AND "SERVICIO_BENEFICIARIO" = :servicio
                """)
                result = conn.execute(query, {'servicio': servicio})
            else:
                result = conn.execute(query)
                
            items = []
            for row in result:
                items.append({
                    'lote': row[0],
                    'item': row[1],
                    'codigo_insumo': row[2],
                    'codigo_servicio': row[3],
                    'servicio': row[4],
                    'descripcion': row[5],
                    'unidad_medida': row[6],
                    'precio_unitario': row[7],
                    'cantidad_maxima': row[8],
                    'cantidad_emitida': row[9],
                    'saldo_emitir': row[10]
                })
            
            return items
    except Exception as e:
        st.error(f"Error obteniendo datos de items: {e}")
        return []

def obtener_servicios_beneficiarios(esquema):
    """Obtiene la lista de servicios beneficiarios para un esquema"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text(f"""
                SELECT DISTINCT "SERVICIO_BENEFICIARIO"
                FROM "{esquema}"."ejecucion_general"
                WHERE "SERVICIO_BENEFICIARIO" IS NOT NULL
                ORDER BY "SERVICIO_BENEFICIARIO"
            """)
            result = conn.execute(query)
            
            servicios = [row[0] for row in result]
            return servicios
    except Exception as e:
        st.error(f"Error obteniendo servicios beneficiarios: {e}")
        return []

def obtener_proximo_numero_oc(esquema, year=None):
    """
    Obtiene el próximo número de orden de compra secuencial simple
    Formato: 1, 2, 3, ... 100, 101 (sin año, sin barras)
    El parámetro 'year' se mantiene por compatibilidad pero no se usa
    """
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            # Buscar el número máximo actual (extrae solo dígitos)
            query = text(f"""
                SELECT MAX(
                    CAST(
                        REGEXP_REPLACE(COALESCE("NUMERO_ORDEN_DE_COMPRA", '0'), '[^0-9]', '', 'g') 
                        AS INTEGER
                    )
                ) as max_num
                FROM "{esquema}"."orden_de_compra" 
                WHERE "NUMERO_ORDEN_DE_COMPRA" IS NOT NULL 
                  AND "NUMERO_ORDEN_DE_COMPRA" != ''
                  AND "NUMERO_ORDEN_DE_COMPRA" != '-'
            """)
            
            result = conn.execute(query)
            max_num = result.scalar()
            
            if max_num and max_num > 0:
                next_num = max_num + 1
            else:
                next_num = 1
            
            return str(next_num)
            
    except Exception as e:
        # Si hay error, devolver "1"
        return "1"

def crear_orden_compra(esquema, numero_orden, fecha_emision, servicio_beneficiario, simese, items):
    """
    Crea una nueva orden de compra con sus items
    
    Args:
        esquema (str): Esquema de la licitación
        numero_orden (str): Número de la orden de compra
        fecha_emision (datetime): Fecha de emisión
        servicio_beneficiario (str): Servicio beneficiario
        simese (str): Número de SIMESE
        items (list): Lista de items para la orden de compra
    
    Returns:
        tuple: (success, message, orden_id)
    """
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            # Iniciar transacción
            trans = conn.begin()
            try:
                # Insertar cabecera de orden de compra
                query = text("""
                    INSERT INTO ordenes_compra 
                    (numero_orden, fecha_emision, esquema, servicio_beneficiario, simese, usuario_id, estado)
                    VALUES (:numero_orden, :fecha_emision, :esquema, :servicio_beneficiario, :simese, :usuario_id, 'Emitida')
                    RETURNING id
                """)
                
                result = conn.execute(query, {
                    'numero_orden': numero_orden,
                    'fecha_emision': fecha_emision,
                    'esquema': esquema,
                    'servicio_beneficiario': servicio_beneficiario,
                    'simese': simese,
                    'usuario_id': st.session_state.user_id
                })
                
                orden_id = result.scalar()
                
                # Insertar items de la orden
                for item in items:
                    monto_total = item['cantidad'] * item['precio_unitario']
                    
                    query_item = text("""
                        INSERT INTO items_orden_compra
                        (orden_compra_id, lote, item, codigo_insumo, codigo_servicio, 
                         descripcion, cantidad, unidad_medida, precio_unitario, monto_total, observaciones)
                        VALUES
                        (:orden_id, :lote, :item, :codigo_insumo, :codigo_servicio,
                         :descripcion, :cantidad, :unidad_medida, :precio_unitario, :monto_total, :observaciones)
                    """)
                    
                    conn.execute(query_item, {
                        'orden_id': orden_id,
                        'lote': item['lote'],
                        'item': item['item'],
                        'codigo_insumo': item['codigo_insumo'],
                        'codigo_servicio': item['codigo_servicio'],
                        'descripcion': item['descripcion'],
                        'cantidad': item['cantidad'],
                        'unidad_medida': item['unidad_medida'],
                        'precio_unitario': item['precio_unitario'],
                        'monto_total': monto_total,
                        'observaciones': item.get('observaciones', '')
                    })
                    
                    # Actualizar cantidad emitida en la tabla de ejecución general
                    query_update = text(f"""
                        UPDATE "{esquema}"."ejecucion_general"
                        SET "CANTIDAD_EMITIDA" = "CANTIDAD_EMITIDA" + :cantidad,
                            "SALDO_A_EMITIR" = "CANTIDAD_MAXIMA" - ("CANTIDAD_EMITIDA" + :cantidad),
                            "PORCENTAJE_EMITIDO" = 
                                (("CANTIDAD_EMITIDA" + :cantidad) / "CANTIDAD_MAXIMA") * 100
                        WHERE "LOTE" = :lote 
                        AND "ITEM" = :item
                        AND "SERVICIO_BENEFICIARIO" = :servicio
                    """)
                    
                    conn.execute(query_update, {
                        'cantidad': item['cantidad'],
                        'lote': item['lote'],
                        'item': item['item'],
                        'servicio': servicio_beneficiario
                    })
                
                # Actualizar también la tabla orden_de_compra del esquema
                for item in items:
                    query_insert_oc = text(f"""
                        INSERT INTO "{esquema}"."orden_de_compra"
                        ("SIMESE_PEDIDO", "N_ORDEN_DE_COMPRA", "FECHA_DE_EMISION",
                        "CODIGO_DE_REACTIVOS_INSUMOS_CODIGO_DE_SERVICIO_BENEFICIARIO",
                        "CODIGO_DE_REACTIVOS_INSUMOS", "SERVICIO_BENEFICIARIO",
                        "LOTE", "ITEM", "CANTIDAD_SOLICITADA", "UNIDAD_DE_MEDIDA",
                        "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA", "PRECIO_UNITARIO",
                        "MONTO_EMITIDO", "OBSERVACIONES")
                        VALUES
                        (:simese, :numero_orden, :fecha_emision,
                        :codigo_completo, :codigo_insumo, :servicio,
                        :lote, :item, :cantidad, :unidad_medida,
                        :descripcion, :precio_unitario,
                        :monto_total, :observaciones)
                    """)
                    
                    codigo_completo = f"{item['codigo_insumo']}{item['codigo_servicio']}" if item['codigo_servicio'] else item['codigo_insumo']
                    
                    conn.execute(query_insert_oc, {
                        'simese': simese,
                        'numero_orden': numero_orden,
                        'fecha_emision': fecha_emision,
                        'codigo_completo': codigo_completo,
                        'codigo_insumo': item['codigo_insumo'],
                        'servicio': servicio_beneficiario,
                        'lote': item['lote'],
                        'item': item['item'],
                        'cantidad': item['cantidad'],
                        'unidad_medida': item['unidad_medida'],
                        'descripcion': item['descripcion'],
                        'precio_unitario': item['precio_unitario'],
                        'monto_total': item['cantidad'] * item['precio_unitario'],
                        'observaciones': item.get('observaciones', '')
                    })
                
                # Confirmar transacción
                trans.commit()
                registrar_actividad(
                    accion="CREATE",
                    modulo="ORDENES_COMPRA",
                    descripcion=f"Orden de compra {numero_orden} creada",
                    esquema_afectado=esquema
                )
                return True, "Orden de compra creada exitosamente", orden_id
                
            except Exception as e:
                # Revertir transacción en caso de error
                trans.rollback()
                raise e
                
    except Exception as e:
        return False, f"Error al crear orden de compra: {e}", None

def pagina_dashboard():
    """Página de dashboard con análisis consolidado de múltiples licitaciones"""
    st.header("📊 Dashboard - Análisis Consolidado de Licitaciones")
    
    # Obtener todas las licitaciones
    esquemas = obtener_esquemas_postgres()
    
    if not esquemas:
        st.warning("No hay licitaciones cargadas.")
        return
    
    # Obtener datos de empresas para cada esquema
    licitaciones_info = {}
    
    def formatear_nombre_esquema(esquema):
        """Convierte 'lpn_100/2023' a 'LPN 100/2023'"""
        # Reemplazar guiones bajos por espacios
        nombre = esquema.replace('_', ' ')
        # Convertir a mayúsculas
        nombre = nombre.upper()
        return nombre
    
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            for esquema in esquemas:
                try:
                    query = text(f"""
                        SELECT "EMPRESA_ADJUDICADA", "NOMBRE_DEL_LLAMADO", 
                               "NUMERO_DE_LLAMADO", "AÑO_DEL_LLAMADO", "MODALIDAD"
                        FROM "{esquema}"."llamado"
                        LIMIT 1
                    """)
                    result = conn.execute(query)
                    datos = result.fetchone()
                    
                    if datos:
                        empresa = datos[0] if datos[0] else "Sin Empresa"
                        numero = datos[2] if datos[2] else ""
                        anio = datos[3] if datos[3] else ""
                        modalidad = datos[4] if datos[4] else ""
                        
                        # Crear nombre formateado
                        if modalidad and numero and anio:
                            nombre_bonito = f"{modalidad} {numero}/{anio}"
                        else:
                            nombre_bonito = formatear_nombre_esquema(esquema)
                        
                        display_name = f"{nombre_bonito} ({empresa})"
                        
                        licitaciones_info[display_name] = {
                            'esquema': esquema,
                            'empresa': empresa,
                            'numero': numero,
                            'anio': anio,
                            'nombre': datos[1] if datos[1] else "",
                            'nombre_bonito': nombre_bonito
                        }
                except Exception:
                    continue
    
    except Exception as e:
        st.error(f"Error: {e}")
        return
    
    if not licitaciones_info:
        st.error("No se pudieron cargar datos de ninguna licitación.")
        return
    
    # Multi-selector de licitaciones
    st.subheader("🔍 Seleccionar Licitaciones para Análisis")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.info("💡 Seleccione las licitaciones a analizar. Dejar vacío = ver todas.")
    
    with col2:
        ver_todas = st.checkbox("Ver todas", value=True, key="ver_todas_licitaciones")
    
    if ver_todas:
        licitaciones_seleccionadas = list(licitaciones_info.keys())
    else:
        licitaciones_seleccionadas = st.multiselect(
            "Licitaciones:",
            options=list(licitaciones_info.keys()),
            default=list(licitaciones_info.keys())[:3] if len(licitaciones_info) >= 3 else list(licitaciones_info.keys()),
            key="multi_selector_licitaciones"
        )
    
    if not licitaciones_seleccionadas:
        st.warning("Seleccione al menos una licitación.")
        return
    
    st.success(f"📊 Analizando {len(licitaciones_seleccionadas)} licitación(es)")
    st.divider()
    
    # Obtener datos consolidados CON MONTOS
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            datos_consolidados = []
            
            for lic_display in licitaciones_seleccionadas:
                lic_info = licitaciones_info[lic_display]
                esquema = lic_info['esquema']
                empresa = lic_info['empresa']
                numero = lic_info['numero']
                anio = lic_info['anio']
                
                # Obtener modalidad del esquema
                try:
                    query_llamado = text(f"""
                        SELECT "MODALIDAD"
                        FROM "{esquema}".llamado
                        LIMIT 1
                    """)
                    result_llamado = conn.execute(query_llamado)
                    row_llamado = result_llamado.fetchone()
                    modalidad = row_llamado[0] if row_llamado else ""
                    llamado = f"{modalidad} {numero}/{anio}".strip() if modalidad else f"{numero}/{anio}"
                except:
                    llamado = f"{numero}/{anio}"
                
                try:
                    # ⭐ LEER DE ejecucion_general CON CÁLCULO DE MONTOS
                    query_servicios = text(f"""
                        SELECT 
                            "SERVICIO_BENEFICIARIO" as servicio,
                            SUM("CANTIDAD_EMITIDA") as cantidad_emitida,
                            SUM("TOTAL_ADJUDICADO") as cantidad_maxima,
                            SUM("CANTIDAD_EMITIDA" * "PRECIO_UNITARIO") as monto_emitido,
                            SUM("TOTAL_ADJUDICADO" * "PRECIO_UNITARIO") as monto_adjudicado,
                            AVG("PORCENTAJE_EMITIDO") as porcentaje
                        FROM "{esquema}"."ejecucion_general"
                        WHERE "SERVICIO_BENEFICIARIO" IS NOT NULL
                        AND "SERVICIO_BENEFICIARIO" != ''
                        GROUP BY "SERVICIO_BENEFICIARIO"
                    """)
                    
                    result = conn.execute(query_servicios)
                    
                    for row in result:
                        datos_consolidados.append({
                            'Llamado': llamado,
                            'Servicio': row[0],
                            'Emitido': float(row[1]) if row[1] else 0,
                            'Máximo': float(row[2]) if row[2] else 0,
                            'Monto_Emitido': float(row[3]) if row[3] else 0,
                            'Monto_Adjudicado': float(row[4]) if row[4] else 0,
                            'Porcentaje': float(row[5]) if row[5] else 0,
                            'Empresa': empresa,
                            'Esquema': esquema
                        })
                
                except Exception as e:
                    st.warning(f"⚠️ Error en {lic_display}: {str(e)}")
                    continue
            
            if not datos_consolidados:
                st.error("No se encontraron datos.")
                return
            
            # Crear DataFrame
            df_consolidado = pd.DataFrame(datos_consolidados)
            
            # Contar número de empresas únicas
            num_empresas = df_consolidado['Empresa'].nunique()
            
            # Solo mostrar gráfico de barras si hay más de una empresa
            if num_empresas > 1:
                # Gráfico de barras agrupadas
                st.subheader("📊 Consumo por Servicio - Comparativa por Empresa")
                
                # Top 15 servicios
                top_servicios = df_consolidado.groupby('Servicio')['Emitido'].sum().nlargest(15).index
                df_grafico = df_consolidado[df_consolidado['Servicio'].isin(top_servicios)]
                
                # Acortar nombres
                df_grafico['Servicio_Corto'] = df_grafico['Servicio'].apply(
                    lambda x: x[:35] + '...' if len(x) > 35 else x
                )
                
                # Crear gráfico
                import plotly.express as px
                
                fig = px.bar(
                    df_grafico,
                    x='Servicio_Corto',
                    y='Emitido',
                    color='Empresa',
                    barmode='group',
                    title='Cantidad Emitida por Servicio y Empresa',
                    labels={
                        'Servicio_Corto': 'Servicio de Salud',
                        'Emitido': 'Cantidad Emitida (m³)',
                        'Empresa': 'Empresa Proveedora'
                    },
                    height=600,
                    text='Emitido'
                )
                
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(
                    xaxis={'tickangle': -45},
                    showlegend=True,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # Tabla comparativa con ejecución en montos (Guaraníes)
            st.subheader("📋 Detalle Comparativo - Ejecución en Montos (Gs.)")
            
            # Crear tabla con cálculos por servicio y empresa EN MONTOS
            resumen_servicios = []
            
            for llamado in df_consolidado['Llamado'].unique():
                for servicio in df_consolidado['Servicio'].unique():
                    for empresa in df_consolidado['Empresa'].unique():
                        datos_servicio = df_consolidado[
                            (df_consolidado['Llamado'] == llamado) &
                            (df_consolidado['Servicio'] == servicio) & 
                            (df_consolidado['Empresa'] == empresa)
                        ]
                        
                        if not datos_servicio.empty:
                            # ⭐ USAR MONTOS (cantidad × precio)
                            monto_adjudicado = datos_servicio['Monto_Adjudicado'].sum()
                            monto_emitido = datos_servicio['Monto_Emitido'].sum()
                            saldo_monto = monto_adjudicado - monto_emitido
                            porcentaje = (monto_emitido / monto_adjudicado * 100) if monto_adjudicado > 0 else 0
                            
                            resumen_servicios.append({
                                'Llamado': llamado,
                                'Empresa': empresa,
                                'Servicio': servicio,
                                'Total Adjudicado (Gs.)': monto_adjudicado,
                                'Total Emitido (Gs.)': monto_emitido,
                                'Saldo (Gs.)': saldo_monto,
                                '% Ejecución': porcentaje
                            })
            
            if resumen_servicios:
                df_resumen = pd.DataFrame(resumen_servicios)
                
                # Formatear columnas numéricas con separador de miles (punto)
                df_resumen['Total Adjudicado (Gs.)'] = df_resumen['Total Adjudicado (Gs.)'].apply(lambda x: f"Gs. {int(x):,}".replace(',', '.'))
                df_resumen['Total Emitido (Gs.)'] = df_resumen['Total Emitido (Gs.)'].apply(lambda x: f"Gs. {int(x):,}".replace(',', '.'))
                df_resumen['Saldo (Gs.)'] = df_resumen['Saldo (Gs.)'].apply(lambda x: f"Gs. {int(x):,}".replace(',', '.'))
                df_resumen['% Ejecución'] = df_resumen['% Ejecución'].apply(lambda x: f"{x:.1f}%")
                
                st.dataframe(df_resumen, use_container_width=True)
                
                # Mostrar total general
                total_general_adjudicado = sum([r['Total Adjudicado (Gs.)'] for r in resumen_servicios])
                total_general_emitido = sum([r['Total Emitido (Gs.)'] for r in resumen_servicios])
                total_general_saldo = total_general_adjudicado - total_general_emitido
                porcentaje_general = (total_general_emitido / total_general_adjudicado * 100) if total_general_adjudicado > 0 else 0
                
                st.divider()
                col_tot1, col_tot2, col_tot3, col_tot4 = st.columns(4)
                
                with col_tot1:
                    st.metric("💰 Total Adjudicado", f"₲ {int(total_general_adjudicado):,}")
                
                with col_tot2:
                    st.metric("✅ Total Emitido", f"₲ {int(total_general_emitido):,}")
                
                with col_tot3:
                    st.metric("📊 Saldo", f"₲ {int(total_general_saldo):,}")
                
                with col_tot4:
                    st.metric("📈 % Ejecución", f"{porcentaje_general:.1f}%")
            
            # Métricas por empresa - Solo mostrar si hay más de una empresa
            if num_empresas > 1:
                st.divider()
                st.subheader("📈 Resumen por Empresa")
                
                resumen_empresas = df_consolidado.groupby('Empresa').agg({
                    'Emitido': 'sum',
                    'Máximo': 'sum'
                }).round(2)
                
                resumen_empresas['Porcentaje'] = (
                    resumen_empresas['Emitido'] / resumen_empresas['Máximo'] * 100
                ).round(1)
                
                cols = st.columns(len(resumen_empresas))
                
                for idx, (empresa, row) in enumerate(resumen_empresas.iterrows()):
                    with cols[idx]:
                        st.metric(
                            label=f"🏢 {empresa}",
                            value=f"{row['Emitido']:,.0f} m³",
                            delta=f"{row['Porcentaje']:.1f}% ejecutado"
                        )
                        st.caption(f"Total licitado: {row['Máximo']:,.0f} m³")
                
                # Gráfico de porcentajes
                st.divider()
                st.subheader("📊 Porcentaje de Ejecución por Empresa")
                
                fig_pct = px.bar(
                    resumen_empresas.reset_index(),
                    x='Empresa',
                    y='Porcentaje',
                    text='Porcentaje',
                    color='Porcentaje',
                    color_continuous_scale='RdYlGn',
                    labels={
                        'Empresa': 'Empresa Proveedora',
                        'Porcentaje': '% de Ejecución'
                    },
                    height=400
                )
                
                fig_pct.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                fig_pct.update_layout(showlegend=False)
                
                st.plotly_chart(fig_pct, use_container_width=True)
            
            # Exportar
            st.divider()
            if st.button("📥 Exportar a Excel"):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_consolidado.to_excel(writer, sheet_name='Datos', index=False)
                    tabla_pivot.to_excel(writer, sheet_name='Comparativa')
                    resumen_empresas.to_excel(writer, sheet_name='Resumen')
                
                output.seek(0)
                
                st.download_button(
                    label="⬇️ Descargar",
                    data=output,
                    file_name=f"consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
    except Exception as e:
        st.error(f"Error: {e}")
        import traceback
        st.code(traceback.format_exc())
    
    # ====================================================================
    # FILTRO GLOBAL ÚNICO
    # ====================================================================
    st.divider()
    st.subheader("🔍 Filtro Global")
    
    col_filtro1, col_filtro2, col_filtro3 = st.columns([2, 1, 1])
    
    with col_filtro1:
        filtro_global = st.text_input(
            "🔍 Buscar (Separe palabras con espacios para buscar múltiples términos):",
            placeholder="Ej: OXIGENO LIQUIDO, LPN 100 CHACO, Hospital AIR LIQUIDE",
            key="filtro_global_unico",
            help="Busca en: Licitación, Empresa, Servicio, Descripción del Producto, Marca, Procedencia, Lote, Item. Puede escribir múltiples palabras."
        )
    
    with col_filtro2:
        fecha_desde_global = st.date_input(
            "Desde:",
            value=None,
            key="fecha_desde_global"
        )
    
    with col_filtro3:
        fecha_hasta_global = st.date_input(
            "Hasta:",
            value=None,
            key="fecha_hasta_global"
        )
    
    # ====================================================================
    # SECCIÓN 1: DETALLE DE LOTES E ITEMS POR HOSPITAL
    # ====================================================================
    st.divider()
    st.subheader("🏥 Detalle de Lotes e Items por Hospital")

    # Usar filtro global
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        
        # Si es API REST, intentar obtener conexión directa como fallback
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Intentar conexión directa para operaciones complejas
            from urllib.parse import quote_plus
            config = get_db_config_licitaciones()
            host = config['host']
            port = config['port']
            dbname = config['name']
            user = config['user']
            password = config['password']
            
            # Asegurar formato correcto de usuario para Supabase
            if '.supabase.co' in host:
                instance_id = host.split('.')[1] if host.startswith('db.') else host.split('.')[0]
                if not user.startswith(f'postgres.{instance_id}'):
                    user = f"postgres.{instance_id}"
            
            try:
                password_escaped = quote_plus(password)
                conn_str = f"postgresql://{user}:{password_escaped}@{host}:{port}/{dbname}?sslmode=require"
                engine = create_engine(
                    conn_str,
                    connect_args={
                        "client_encoding": "utf8",
                        "connect_timeout": 10,
                        "sslmode": "require"
                    },
                    pool_pre_ping=True
                )
                # Probar conexión
                with engine.connect() as test_conn:
                    test_conn.execute(text("SELECT 1"))
            except Exception as e:
                st.warning(f"El dashboard completo requiere conexión directa. Error: {e}. Algunas funcionalidades pueden estar limitadas.")
                return
        
        with engine.connect() as conn:
            resultados_detalle = []

            # Buscar en todos los esquemas
            for licitacion_nombre, info in licitaciones_info.items():
                esquema = info['esquema']
                nombre_bonito = info.get('nombre_bonito', licitacion_nombre)

                try:
                    # Query base
                    query_detalle = text(f"""
                        SELECT 
                            "LOTE",
                            "ITEM",
                            "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                            "UNIDAD_DE_MEDIDA",
                            "PRECIO_UNITARIO",
                            "CANTIDAD_MAXIMA",
                            "CANTIDAD_EMITIDA",
                            "SALDO_A_EMITIR",
                            "PORCENTAJE_EMITIDO",
                            "SERVICIO_BENEFICIARIO"
                        FROM "{esquema}"."ejecucion_general"
                        WHERE 1=1
                    """)

                    params = {}

                    # Aplicar filtro global
                    if filtro_global and filtro_global.strip():
                        query_detalle = text(f"""
                            SELECT 
                                "LOTE",
                                "ITEM",
                                "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                                "UNIDAD_DE_MEDIDA",
                                "PRECIO_UNITARIO",
                                "CANTIDAD_MAXIMA",
                                "CANTIDAD_EMITIDA",
                                "SALDO_A_EMITIR",
                                "PORCENTAJE_EMITIDO",
                                "SERVICIO_BENEFICIARIO"
                            FROM "{esquema}"."ejecucion_general"
                            WHERE (
                                UPPER("SERVICIO_BENEFICIARIO"::TEXT) LIKE UPPER(:busqueda) OR
                                UPPER("DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA"::TEXT) LIKE UPPER(:busqueda)
                            )
                            ORDER BY "LOTE", "ITEM"
                        """)
                        params['busqueda'] = f"%{filtro_global.strip()}%"
                    else:
                        query_detalle = text(f"""
                            SELECT 
                                "LOTE",
                                "ITEM",
                                "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                                "UNIDAD_DE_MEDIDA",
                                "PRECIO_UNITARIO",
                                "CANTIDAD_MAXIMA",
                                "CANTIDAD_EMITIDA",
                                "SALDO_A_EMITIR",
                                "PORCENTAJE_EMITIDO",
                                "SERVICIO_BENEFICIARIO"
                            FROM "{esquema}"."ejecucion_general"
                            ORDER BY "LOTE", "ITEM"
                        """)

                    result = conn.execute(query_detalle, params)

                    for row in result:
                        # Formatear porcentaje
                        porcentaje_raw = float(row[8]) if row[8] else 0
                        if porcentaje_raw <= 1.0:
                            porcentaje_formatted = f"{porcentaje_raw * 100:.0f}%"
                        else:
                            porcentaje_formatted = f"{porcentaje_raw:.0f}%"

                        resultados_detalle.append({
                            'Llamado': nombre_bonito,
                            'Empresa': empresa,
                            'Servicio': row[9] if row[9] else "-",
                            'Lote': row[0],
                            'Item': row[1],
                            'Descripción': row[2] if row[2] else "-",
                            'Unidad': row[3],
                            'Precio Unit.': f"Gs. {int(row[4]):,}".replace(',', '.') if row[4] else "-",
                            'Cant. Máxima': f"{int(row[5]):,}".replace(',', '.') if row[5] else "-",
                            'Cant. Emitida': f"{int(row[6]):,}".replace(',', '.') if row[6] else "-",
                            'Saldo': f"{int(row[7]):,}".replace(',', '.') if row[7] else "-",
                            '% Emitido': porcentaje_formatted
                        })

                except Exception as e:
                    continue  # <--- ESTA ERA LA INDENTACIÓN QUE FALTABA

            # Mostrar resultados
            if resultados_detalle:
                df_detalle = pd.DataFrame(resultados_detalle)
                st.success(f"✅ {len(resultados_detalle)} item(s) encontrado(s)")
                st.dataframe(df_detalle, use_container_width=True, height=400)

                # Botón de exportar
                if st.button("📥 Exportar detalle a Excel", key="export_detalle"):
                    output_det = io.BytesIO()
                    with pd.ExcelWriter(output_det, engine='openpyxl') as writer:
                        df_detalle.to_excel(writer, sheet_name='Detalle', index=False)

                    output_det.seek(0)

                    st.download_button(
                        label="⬇️ Descargar Excel",
                        data=output_det,
                        file_name=f"detalle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_detalle"
                    )
            else:
                st.info("No se encontraron resultados para la búsqueda")

    except Exception as e:
        st.error(f"Error obteniendo detalle: {e}")

    
    # ====================================================================
    # SECCIÓN 2: TABLA DE ÓRDENES DE COMPRA EMITIDAS
    # ====================================================================
    st.divider()
    st.subheader("📋 Órdenes de Compra Emitidas")
    
    # Usar filtro global
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            ordenes_compra = []
            
            # Buscar en todos los esquemas
            esquemas_a_buscar = [info['esquema'] for info in licitaciones_info.values()]
            
            # Buscar órdenes en cada esquema
            for esquema in esquemas_a_buscar:
                try:
                    # Construir query simple - todas las columnas están en orden_de_compra
                    query_oc = f"""
                        SELECT 
                            "NUMERO_ORDEN_DE_COMPRA" as numero_orden,
                            "FECHA_DE_EMISION" as fecha_emision,
                            "LOTE" as lote,
                            "ITEM" as item,
                            "CANTIDAD_SOLICITADA" as cantidad,
                            "UNIDAD_DE_MEDIDA" as unidad,
                            "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA" as descripcion,
                            "SERVICIO_BENEFICIARIO" as servicio,
                            "I_D" as id_licitacion,
                            "MODALIDAD" as modalidad,
                            "EMPRESA_ADJUDICADA" as empresa
                        FROM "{esquema}".orden_de_compra
                        WHERE 1=1
                    """
                    
                    params = {}
                    
                    # Filtro global de búsqueda
                    if filtro_global and filtro_global.strip():
                        busqueda = f"%{filtro_global.strip()}%"
                        query_oc += ''' AND (
                            UPPER("SERVICIO_BENEFICIARIO"::TEXT) LIKE UPPER(:busqueda) OR
                            UPPER("EMPRESA_ADJUDICADA"::TEXT) LIKE UPPER(:busqueda) OR
                            UPPER("MODALIDAD"::TEXT) LIKE UPPER(:busqueda) OR
                            UPPER("DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA"::TEXT) LIKE UPPER(:busqueda)
                        )'''
                        params['busqueda'] = busqueda
                    
                    # Filtro por fecha desde
                    if fecha_desde_global:
                        query_oc += ' AND "FECHA_DE_EMISION" >= :fecha_desde'
                        params['fecha_desde'] = fecha_desde_global
                    
                    # Filtro por fecha hasta
                    if fecha_hasta_global:
                        query_oc += ' AND "FECHA_DE_EMISION" <= :fecha_hasta'
                        params['fecha_hasta'] = fecha_hasta_global
                    
                    query_oc += ' ORDER BY "FECHA_DE_EMISION" DESC'
                    
                    result_oc = conn.execute(text(query_oc), params)
                    
                    for row in result_oc:
                        # Convertir row a diccionario para acceso por nombre
                        row_dict = dict(row._mapping)
                        
                        # Obtener datos del llamado para este esquema
                        try:
                            query_llamado = text(f"""
                                SELECT "NUMERO_DE_LLAMADO", "AÑO_DEL_LLAMADO"
                                FROM "{esquema}".llamado
                                LIMIT 1
                            """)
                            result_llamado = conn.execute(query_llamado)
                            datos_llamado = result_llamado.fetchone()
                            
                            if datos_llamado:
                                numero = datos_llamado[0]
                                anio = datos_llamado[1]
                                llamado_concat = f"{numero}/{anio}" if numero and anio else "-"
                            else:
                                llamado_concat = "-"
                        except:
                            llamado_concat = "-"
                        
                        # Formatear I_D con punto numérico (433768 → 433.768)
                        try:
                            id_val = row_dict.get('id_licitacion')
                            id_formateado = f"{int(float(id_val)):,}".replace(',', '.') if id_val else "-"
                        except:
                            id_formateado = "-"
                        
                        # Concatenar Modalidad y Llamado
                        modalidad = row_dict.get('modalidad', "") or ""
                        llamado_final = f"{modalidad} {llamado_concat}".strip() if modalidad else llamado_concat
                        
                        # N° Orden: mostrar como texto simple
                        numero_orden = row_dict.get('numero_orden', '-') or '-'
                        if numero_orden and numero_orden != '-':
                            numero_orden = str(numero_orden).strip()
                        
                        # Formatear Lote e Item sin decimales
                        try:
                            lote_val = row_dict.get('lote')
                            lote = f"{int(float(lote_val))}" if lote_val else "-"
                        except:
                            lote = "-"
                        
                        try:
                            item_val = row_dict.get('item')
                            item = f"{int(float(item_val))}" if item_val else "-"
                        except:
                            item = "-"
                        
                        # Formatear Cantidad con punto numérico (71760 → 71.760)
                        try:
                            cant_val = row_dict.get('cantidad')
                            cantidad = f"{int(float(cant_val)):,}".replace(',', '.') if cant_val else "-"
                        except:
                            cantidad = "-"
                        
                        # Formatear fecha
                        fecha_val = row_dict.get('fecha_emision')
                        fecha_str = fecha_val.strftime('%d/%m/%Y') if fecha_val else "-"
                        
                        ordenes_compra.append({
                            'ID': id_formateado,
                            'Llamado': llamado_final,
                            'N° Orden': numero_orden,
                            'Fecha Emisión': fecha_str,
                            'Lote': lote,
                            'Item': item,
                            'Cantidad': cantidad,
                            'Unidad': row_dict.get('unidad', '-') or '-',
                            'Descripción': row_dict.get('descripcion', '-') or '-',
                            'Servicio Beneficiario': row_dict.get('servicio', '-') or '-',
                            'Empresa': row_dict.get('empresa', '-') or '-',
                            '__esquema__': esquema  # Campo oculto para referencia
                        })
                
                except Exception as e:
                    # Si hay error en un esquema, continuar con los demás
                    st.warning(f"⚠️ No se pudieron cargar órdenes del esquema '{esquema}': {e}")
                    continue
            
            if ordenes_compra:
                df_ordenes = pd.DataFrame(ordenes_compra)
                st.success(f"✅ {len(ordenes_compra)} orden(es) de compra encontrada(s)")
                
                # Agregar columna de esquema oculta para referencia
                # Crear un identificador único: esquema + número
                for i, orden in enumerate(ordenes_compra):
                    # Buscar el esquema que generó esta orden
                    for esq in esquemas_a_buscar:
                        if orden.get('__esquema__') == esq:
                            df_ordenes.at[i, '__esquema_ref__'] = esq
                            break
                
                # Mostrar tabla
                st.dataframe(df_ordenes[['ID', 'Llamado', 'N° Orden', 'Fecha Emisión', 'Lote', 'Item', 
                                         'Cantidad', 'Unidad', 'Descripción', 'Servicio Beneficiario', 'Empresa']], 
                            use_container_width=True, height=400)
                
                # Exportar órdenes de compra
                st.markdown("---")
                if st.button("📥 Exportar órdenes a Excel", key="export_ordenes"):
                    output_oc = io.BytesIO()
                    with pd.ExcelWriter(output_oc, engine='openpyxl') as writer:
                        df_ordenes[['ID', 'Llamado', 'N° Orden', 'Fecha Emisión', 'Lote', 'Item', 
                                   'Cantidad', 'Unidad', 'Descripción', 'Servicio Beneficiario', 'Empresa']].to_excel(
                            writer, sheet_name='Ordenes', index=False
                        )
                    
                    output_oc.seek(0)
                    
                    st.download_button(
                        label="⬇️ Descargar Excel",
                        data=output_oc,
                        file_name=f"ordenes_compra_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_ordenes"
                    )
            else:
                st.info("📭 No se encontraron órdenes de compra")
                
    except Exception as e:
        st.error(f"Error obteniendo órdenes de compra: {e}")
        import traceback
        st.code(traceback.format_exc())

def pagina_configurar_logos():
    """Página para configurar los logos del gobierno que aparecerán en los PDFs"""
    st.header("🖼️ Configuración de Logos para PDFs")
    
    st.info("📌 Los logos que cargues aquí aparecerán en todas las Órdenes de Compra en formato PDF.")
    
    # Directorio para guardar logos
    logos_dir = "/home/claude/logos_gobierno"
    os.makedirs(logos_dir, exist_ok=True)
    
    # Tabs para diferentes logos
    tab1, tab2, tab3 = st.tabs(["Logo Principal", "Logo Secundario", "Previsualización"])
    
    with tab1:
        st.subheader("Logo Principal del Gobierno")
        st.write("*Este logo aparecerá en la parte superior izquierda del PDF*")
        
        # Mostrar logo actual si existe
        logo_principal_path = os.path.join(logos_dir, "logo_principal.png")
        if os.path.exists(logo_principal_path):
            col1, col2 = st.columns([1, 2])
            with col1:
                st.image(logo_principal_path, caption="Logo Principal Actual", width=200)
            with col2:
                st.success("✅ Logo principal cargado")
                if st.button("🗑️ Eliminar Logo Principal", key="btn_delete_principal"):
                    os.remove(logo_principal_path)
                    st.success("Logo principal eliminado")
                    time.sleep(1)
                    st.rerun()
        else:
            st.warning("⚠️ No hay logo principal cargado")
        
        st.markdown("---")
        
        # Cargar nuevo logo principal
        logo_principal = st.file_uploader(
            "Cargar Logo Principal",
            type=['png', 'jpg', 'jpeg'],
            key="upload_logo_principal",
            help="Formato recomendado: PNG con fondo transparente, 300x100 píxeles aprox."
        )
        
        if logo_principal:
            # Mostrar preview
            st.image(logo_principal, caption="Vista Previa", width=200)
            
            if st.button("💾 Guardar Logo Principal", key="btn_save_principal"):
                with open(logo_principal_path, "wb") as f:
                    f.write(logo_principal.getbuffer())
                st.success("✅ Logo principal guardado exitosamente!")
                st.balloons()
                time.sleep(2)
                st.rerun()
    
    with tab2:
        st.subheader("Logo Secundario (Opcional)")
        st.write("*Este logo aparecerá en la parte superior derecha del PDF*")
        
        # Mostrar logo actual si existe
        logo_secundario_path = os.path.join(logos_dir, "logo_secundario.png")
        if os.path.exists(logo_secundario_path):
            col1, col2 = st.columns([1, 2])
            with col1:
                st.image(logo_secundario_path, caption="Logo Secundario Actual", width=200)
            with col2:
                st.success("✅ Logo secundario cargado")
                if st.button("🗑️ Eliminar Logo Secundario", key="btn_delete_secundario"):
                    os.remove(logo_secundario_path)
                    st.success("Logo secundario eliminado")
                    time.sleep(1)
                    st.rerun()
        else:
            st.info("ℹ️ No hay logo secundario cargado (opcional)")
        
        st.markdown("---")
        
        # Cargar nuevo logo secundario
        logo_secundario = st.file_uploader(
            "Cargar Logo Secundario",
            type=['png', 'jpg', 'jpeg'],
            key="upload_logo_secundario",
            help="Formato recomendado: PNG con fondo transparente, 300x100 píxeles aprox."
        )
        
        if logo_secundario:
            # Mostrar preview
            st.image(logo_secundario, caption="Vista Previa", width=200)
            
            if st.button("💾 Guardar Logo Secundario", key="btn_save_secundario"):
                with open(logo_secundario_path, "wb") as f:
                    f.write(logo_secundario.getbuffer())
                st.success("✅ Logo secundario guardado exitosamente!")
                st.balloons()
                time.sleep(2)
                st.rerun()
    
    with tab3:
        st.subheader("Previsualización en PDF")
        
        if os.path.exists(logo_principal_path):
            st.write("**Así se verán los logos en el encabezado del PDF:**")
            
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                st.image(logo_principal_path, width=150)
            
            with col2:
                st.markdown("""
                <div style='text-align: center; padding: 20px;'>
                    <h3>REPÚBLICA DEL PARAGUAY</h3>
                    <p>MINISTERIO DE SALUD PÚBLICA Y BIENESTAR SOCIAL</p>
                    <h2>ORDEN DE COMPRA</h2>
                    <h2>N° 001/2025</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                if os.path.exists(logo_secundario_path):
                    st.image(logo_secundario_path, width=150)
                else:
                    st.write("*Sin logo secundario*")
            
            st.success("✅ Los logos se cargarán automáticamente en todos los PDFs de Órdenes de Compra")
        else:
            st.warning("⚠️ Carga al menos el logo principal para ver la previsualización")
    
    # Instrucciones
    st.markdown("---")
    st.subheader("📝 Instrucciones")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Recomendaciones:**
        - Formato: PNG con fondo transparente
        - Tamaño recomendado: 300x100 píxeles
        - Peso máximo: 2 MB
        - Resolución: 300 DPI para impresión
        """)
    
    with col2:
        st.markdown("""
        **Cuándo cambiar logos:**
        - Cambio de gobierno (cada 5 años)
        - Renovación de imagen institucional
        - Nuevas directrices de comunicación
        - Actualización de identidad visual
        """)

def main():
    st.set_page_config(
        page_title="Gestión de Licitaciones",
        page_icon="📊",
        layout="wide"
    )
    
    # Configurar tablas si no existen
    configurar_tabla_usuarios()
    configurar_tabla_ordenes_compra()
    configurar_tabla_cargas()
    configurar_tabla_proveedores()
    configurar_tabla_auditoria()
    crear_tabla_usuario_servicio()  # Nueva tabla para servicios de usuarios
    
    # Inicializar el estado de sesión si es necesario
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    
    # Verificar si el usuario está autenticado
    if not st.session_state.logged_in:
        pagina_login()
        return
    
    # Verificar si el usuario requiere cambio de contraseña
    if 'requiere_cambio_password' in st.session_state and st.session_state.requiere_cambio_password:
        pagina_cambiar_password()
        return
    
    # Si llega aquí, el usuario está autenticado
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        st.sidebar.success(f"✅ Conectado a PostgreSQL | Usuario: {st.session_state.username}")
        
        # ⚠️ INDICADOR DE ACTUALIZACIÓN AUTOMÁTICA DESACTIVADO
        # Se desactivó para mejorar el rendimiento del navegador
        # st.sidebar.info(f"🔄 Próxima actualización auto: {int(tiempo_restante)} min")
    except Exception as e:
        st.sidebar.error(f"❌ Error de conexión: {e}")
        st.error("No se pudo conectar a la base de datos PostgreSQL. Verifique la configuración y que el servidor esté en funcionamiento.")
        return
    
    # Título principal
    st.title("Sistema de Gestión de Llamados de Gases Medicinales")
    
    # Opciones de menú según el rol (SIN historial_actividades por ahora)
    if st.session_state.user_role == 'admin':
        menu_options = {
            "dashboard": "📈 Dashboard",
            "cargar_archivo": "📥 Cargar Archivo", 
            "ver_cargas": "📋 Ver Cargas",
            "ordenes_compra": "📝 Órdenes de Compra",
            "gestionar_proveedores": "🏭 Gestión de Proveedores",
            "configurar_logos": "🖼️ Configurar Logos",
            "eliminar_esquemas": "🗑️ Eliminar Esquemas",
            "admin_usuarios": "👥 Administrar Usuarios",
            "cambiar_password": "🔑 Cambiar Contraseña",
            "logout": "🚪 Cerrar Sesión"
        }
    else:
        menu_options = {
            "dashboard": "📈 Dashboard",
            "cargar_archivo": "📥 Cargar Archivo", 
            "ver_cargas": "📋 Ver Cargas",
            "ordenes_compra": "📝 Órdenes de Compra",
            "gestionar_proveedores": "🏭 Gestión de Proveedores",
            "historial_actividades": "📋 Historial de Actividades",
            "cambiar_password": "🔑 Cambiar Contraseña",
            "logout": "🚪 Cerrar Sesión"
        }
    
    # Crear menú de navegación
    menu = st.sidebar.radio(
        "Menú de Navegación", 
        list(menu_options.keys()),
        format_func=lambda x: menu_options[x]
    )
    
    # Mostrar la página seleccionada (SIN elif historial_actividades)

    if menu == "dashboard":
        pagina_dashboard()
    elif menu == "cargar_archivo":
        pagina_cargar_archivo()
    elif menu == "ver_cargas":
        pagina_ver_cargas()
    elif menu == "ordenes_compra":
        pagina_ordenes_compra()
    elif menu == "gestionar_proveedores":
        pagina_gestionar_proveedores()
    elif menu == "configurar_logos" and st.session_state.user_role == 'admin':
        pagina_configurar_logos()
    elif menu == "eliminar_esquemas" and st.session_state.user_role == 'admin':
        pagina_eliminar_esquemas()
    elif menu == "admin_usuarios" and st.session_state.user_role == 'admin':
        pagina_administrar_usuarios()
    elif menu == "historial_actividades":
            pagina_historial_actividades()
    elif menu == "cambiar_password":
        pagina_cambiar_password()
    elif menu == "logout":
        # Cerrar sesión
        st.session_state.logged_in = False
        st.session_state.user_id = None
        st.session_state.user_role = None
        st.session_state.username = None
        st.success("Sesión cerrada correctamente. Redirigiendo...")
        time.sleep(1)
        st.rerun()

def obtener_ordenes_compra(esquema=None):
    """Obtiene órdenes de compra leyendo DIRECTAMENTE de las tablas de cada esquema
    NO usa tabla central - lee de lpn_xxx.orden_de_compra"""
    
    try:
        ordenes = []
        
        # Obtener esquemas activos
        esquemas = obtener_esquemas_postgres()
        
        # Si se especificó un esquema, usar solo ese
        if esquema:
            esquemas = [esquema]
        
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            for esq in esquemas:
                try:
                    # Leer órdenes del esquema
                    query = text(f"""
                        SELECT DISTINCT
                            "NUMERO_ORDEN_DE_COMPRA",
                            "FECHA_DE_EMISION",
                            "SERVICIO_BENEFICIARIO",
                            COUNT(*) as cantidad_items,
                            SUM("CANTIDAD_SOLICITADA" * "PRECIO_UNITARIO") as monto_total
                        FROM "{esq}"."orden_de_compra"
                        GROUP BY "NUMERO_ORDEN_DE_COMPRA", "FECHA_DE_EMISION", "SERVICIO_BENEFICIARIO"
                        ORDER BY "FECHA_DE_EMISION" DESC
                    """)
                    
                    result = conn.execute(query)
                    
                    for row in result:
                        ordenes.append({
                            'id': f"{esq}_{row[0]}",  # ID único: esquema_numero
                            'numero_orden': row[0],
                            'fecha_emision': row[1],
                            'esquema': esq,
                            'servicio_beneficiario': row[2],
                            'simese': None,
                            'estado': 'Emitida',
                            'usuario': 'Sistema',
                            'fecha_creacion': row[1],
                            'cantidad_items': row[3],
                            'monto_total': row[4] if row[4] else 0
                        })
                except Exception as e:
                    # Si el esquema no tiene tabla orden_de_compra, continuar
                    continue
        
        return ordenes
        
    except Exception as e:
        st.error(f"Error obteniendo órdenes: {e}")
        return []
        return []

def obtener_detalles_orden_compra(orden_id):
    """
    Obtiene los detalles completos de una orden de compra desde el esquema
    orden_id formato: esquema_numero (ej: lpn_100/2023_1)
    """
    try:
        # Separar esquema y número de orden
        partes = orden_id.rsplit('_', 1)
        if len(partes) != 2:
            return None
        
        esquema = partes[0]
        numero_orden = partes[1]
        
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            # Obtener todas las líneas de la orden
            query_items = text(f"""
                SELECT 
                    "NUMERO_ORDEN_DE_COMPRA",
                    "FECHA_DE_EMISION",
                    "SERVICIO_BENEFICIARIO",
                    "LOTE",
                    "ITEM",
                    "CANTIDAD_SOLICITADA",
                    "UNIDAD_DE_MEDIDA",
                    "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                    "PRECIO_UNITARIO",
                    "EMPRESA_ADJUDICADA",
                    "I_D",
                    "MODALIDAD"
                FROM "{esquema}"."orden_de_compra"
                WHERE "NUMERO_ORDEN_DE_COMPRA" = :numero_orden
                ORDER BY "LOTE", "ITEM"
            """)
            
            result = conn.execute(query_items, {'numero_orden': numero_orden})
            rows = result.fetchall()
            
            if not rows:
                return None
            
            # Primer row tiene info general
            primera_fila = rows[0]
            
            # Formatear esquema bonito
            esquema_bonito = esquema.replace('lpn_', 'LPN ').replace('_', ' ').upper()
            
            # Armar items
            items = []
            monto_total = 0
            for row in rows:
                item_monto = float(row[5] or 0) * float(row[8] or 0)
                monto_total += item_monto
                
                items.append({
                    'lote': row[3],
                    'item': row[4],
                    'descripcion': row[7],
                    'cantidad': row[5],
                    'unidad_medida': row[6] or 'UNIDAD',
                    'precio_unitario': row[8],
                    'monto_total': item_monto
                })
            
            # Armar respuesta
            orden = {
                'id': orden_id,
                'numero_orden': primera_fila[0],
                'fecha_emision': primera_fila[1],
                'esquema': esquema,
                'esquema_bonito': esquema_bonito,
                'servicio_beneficiario': primera_fila[2],
                'empresa_adjudicada': primera_fila[9],
                'id_licitacion': primera_fila[10],
                'modalidad': primera_fila[11],
                'estado': 'Emitida',
                'usuario': 'Sistema',
                'items': items,
                'monto_total': monto_total,
                'cantidad_items': len(items)
            }
            
            return orden
            
    except Exception as e:
        st.error(f"Error obteniendo detalles de orden: {e}")
        return None

def cambiar_estado_orden_compra(orden_id, nuevo_estado):
    """Cambia el estado de una orden de compra"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text("""
                UPDATE oxigeno.ordenes_compra
                SET estado = :estado
                WHERE id = :orden_id
                RETURNING numero_orden
            """)
            
            result = conn.execute(query, {'estado': nuevo_estado, 'orden_id': orden_id})
            numero_orden = result.scalar()
            
            if numero_orden:
                return True, f"Estado de orden {numero_orden} cambiado a '{nuevo_estado}'"
            else:
                return False, "Orden de compra no encontrada"
    except Exception as e:
        return False, f"Error al cambiar estado: {e}"

def numero_a_letras(numero):
    """Convierte un número a letras en guaraníes"""
    unidades = ["", "UN", "DOS", "TRES", "CUATRO", "CINCO", "SEIS", "SIETE", "OCHO", "NUEVE"]
    decenas = ["", "DIEZ", "VEINTE", "TREINTA", "CUARENTA", "CINCUENTA", "SESENTA", "SETENTA", "OCHENTA", "NOVENTA"]
    especiales = ["DIEZ", "ONCE", "DOCE", "TRECE", "CATORCE", "QUINCE", "DIECISEIS", "DIECISIETE", "DIECIOCHO", "DIECINUEVE"]
    centenas = ["", "CIENTO", "DOSCIENTOS", "TRESCIENTOS", "CUATROCIENTOS", "QUINIENTOS", "SEISCIENTOS", "SETECIENTOS", "OCHOCIENTOS", "NOVECIENTOS"]
    
    if numero == 0:
        return "CERO GUARANIES"
    
    def convertir_grupo(n):
        if n == 0:
            return ""
        elif n < 10:
            return unidades[n]
        elif n < 20:
            return especiales[n - 10]
        elif n < 100:
            d = n // 10
            u = n % 10
            if u == 0:
                return decenas[d]
            else:
                return decenas[d] + " Y " + unidades[u]
        else:
            c = n // 100
            resto = n % 100
            if c == 1 and resto == 0:
                return "CIEN"
            elif resto == 0:
                return centenas[c]
            else:
                return centenas[c] + " " + convertir_grupo(resto)
    
    # Convertir número
    if numero < 1000:
        texto = convertir_grupo(numero)
    elif numero < 1000000:
        miles = numero // 1000
        resto = numero % 1000
        if miles == 1:
            texto = "MIL"
        else:
            texto = convertir_grupo(miles) + " MIL"
        if resto > 0:
            texto += " " + convertir_grupo(resto)
    elif numero < 1000000000:
        millones = numero // 1000000
        resto = numero % 1000000
        if millones == 1:
            texto = "UN MILLON"
        else:
            texto = convertir_grupo(millones) + " MILLONES"
        if resto >= 1000:
            miles = resto // 1000
            resto_final = resto % 1000
            if miles > 0:
                if miles == 1:
                    texto += " MIL"
                else:
                    texto += " " + convertir_grupo(miles) + " MIL"
            if resto_final > 0:
                texto += " " + convertir_grupo(resto_final)
        elif resto > 0:
            texto += " " + convertir_grupo(resto)
    else:
        return "MONTO DEMASIADO GRANDE"
    
    return texto + " GUARANIES"

def generar_pdf_orden_compra(esquema, numero_oc, anio_oc, fecha_emision, servicio, items, usuario):
    """Genera PDF de orden de compra con diseño profesional"""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch, cm
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
        
        # Crear buffer
        buffer = io.BytesIO()
        
        # Configurar documento A4
        doc = SimpleDocTemplate(
            buffer, 
            pagesize=A4,
            rightMargin=1.5*cm,
            leftMargin=1.5*cm,
            topMargin=1*cm,
            bottomMargin=1.5*cm
        )
        
        # Estilos
        styles = getSampleStyleSheet()
        
        style_title = ParagraphStyle(
            'Title',
            parent=styles['Heading1'],
            fontSize=12,
            textColor=colors.black,
            spaceAfter=4,
            spaceBefore=4,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        style_subtitle = ParagraphStyle(
            'Subtitle',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            spaceAfter=2,
            spaceBefore=2,
            alignment=TA_CENTER,
            fontName='Helvetica'
        )
        
        style_section_title = ParagraphStyle(
            'SectionTitle',
            parent=styles['Heading2'],
            fontSize=10,
            textColor=colors.white,
            spaceAfter=0,
            spaceBefore=0,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            backColor=colors.grey
        )
        
        style_normal = ParagraphStyle(
            'Normal',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.black,
            alignment=TA_LEFT,
            fontName='Helvetica'
        )
        
        style_bold = ParagraphStyle(
            'Bold',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.black,
            alignment=TA_LEFT,
            fontName='Helvetica-Bold'
        )
        
        # Contenido
        story = []
        
        # ========== SECCIÓN 1: LOGOS Y ENCABEZADO ==========
        logos_dir = "/home/claude/logos_gobierno"
        logo_principal_path = os.path.join(logos_dir, "logo_principal.png")
        
        # Logo ANCHO (no alto)
        if os.path.exists(logo_principal_path):
            logo = RLImage(logo_principal_path, width=12*cm, height=2*cm)
            header_data = [[logo]]
            header_widths = [18*cm]
            
            tabla_header = Table(header_data, colWidths=header_widths)
            tabla_header.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOX', (0, 0), (-1, -1), 1, colors.black),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ]))
            story.append(tabla_header)
            story.append(Spacer(1, 0.2*cm))
        
        # Título ORDEN DE COMPRA con formato Nº/AÑO
        numero_completo = f"{numero_oc}/{anio_oc}"
        titulo_oc = Table([[Paragraph(f"<b>ORDEN DE COMPRA N° {numero_completo}</b>", style_title)]], colWidths=[18*cm])
        titulo_oc.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOX', (0, 0), (-1, -1), 1.5, colors.black),
            ('BACKGROUND', (0, 0), (-1, -1), colors.lightgrey),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(titulo_oc)
        story.append(Spacer(1, 0.15*cm))
        
        # Obtener datos de licitación
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text(f"""
                SELECT "NUMERO_DE_LLAMADO", "AÑO_DEL_LLAMADO", "NOMBRE_DEL_LLAMADO", 
                       "I_D", "EMPRESA_ADJUDICADA", "RUC"
                FROM "{esquema}"."llamado"
                LIMIT 1
            """)
            result = conn.execute(query)
            licitacion = result.fetchone()
        
        # ========== SECCIÓN 2: DATOS GENERALES ==========
        datos_generales_header = [[Paragraph("<b>DATOS GENERALES</b>", style_section_title)]]
        tabla_dg_header = Table(datos_generales_header, colWidths=[18*cm])
        tabla_dg_header.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('BACKGROUND', (0, 0), (-1, -1), colors.grey),
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(tabla_dg_header)
        
        # Fecha: manejar tanto string como datetime
        if isinstance(fecha_emision, str):
            fecha_texto = fecha_emision
        else:
            fecha_texto = fecha_emision.strftime("%d/%m/%Y")
        
        # Formatear ID con punto de miles
        id_llamado = licitacion[3]
        id_formateado = f"{id_llamado:,}".replace(",", ".") if id_llamado else "N/A"
        
        datos_generales = [
            [Paragraph("<b>Fecha de Emisión:</b>", style_bold), Paragraph(fecha_texto, style_normal)],
            [Paragraph("<b>Servicio Beneficiario:</b>", style_bold), Paragraph(servicio, style_normal)],
            [Paragraph("<b>Usuario Responsable de Emisión de OC:</b>", style_bold), Paragraph(usuario, style_normal)],
            [Paragraph("<b>Licitación:</b>", style_bold), 
             Paragraph(f"LPN {licitacion[0]}/{licitacion[1]} - {licitacion[2]}", style_normal)],
            [Paragraph("<b>I.D. del Llamado:</b>", style_bold), 
             Paragraph(id_formateado, style_normal)]
        ]
        
        tabla_dg = Table(datos_generales, colWidths=[4.5*cm, 13.5*cm])
        tabla_dg.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(tabla_dg)
        story.append(Spacer(1, 0.15*cm))
        
        # ========== SECCIÓN 3: PROVEEDOR ==========
        proveedor_header = [[Paragraph("<b>DATOS DEL PROVEEDOR</b>", style_section_title)]]
        tabla_prov_header = Table(proveedor_header, colWidths=[18*cm])
        tabla_prov_header.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('BACKGROUND', (0, 0), (-1, -1), colors.grey),
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(tabla_prov_header)
        
        proveedor_data = [
            [Paragraph("<b>Empresa:</b>", style_bold), 
             Paragraph(licitacion[4] if licitacion[4] else "N/A", style_normal)],
            [Paragraph("<b>RUC:</b>", style_bold), 
             Paragraph(licitacion[5] if licitacion[5] else "N/A", style_normal)]
        ]
        
        tabla_prov = Table(proveedor_data, colWidths=[4.5*cm, 13.5*cm])
        tabla_prov.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(tabla_prov)
        story.append(Spacer(1, 0.15*cm))
        
        # ========== SECCIÓN 4: DETALLE DE ITEMS ==========
        items_header = [[Paragraph("<b>DETALLES DE ORDEN DE COMPRA</b>", style_section_title)]]
        tabla_items_header = Table(items_header, colWidths=[18*cm])
        tabla_items_header.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('BACKGROUND', (0, 0), (-1, -1), colors.grey),
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(tabla_items_header)
        
        # Encabezado de tabla de items con UNIDAD DE MEDIDA
        items_table_data = [[
            Paragraph("<b>LOTE</b>", style_bold),
            Paragraph("<b>ITEM</b>", style_bold),
            Paragraph("<b>CANT.</b>", style_bold),
            Paragraph("<b>U.M.</b>", style_bold),
            Paragraph("<b>DESCRIPCIÓN // PRESENTACIÓN // MARCA // PROCEDENCIA</b>", style_bold),
            Paragraph("<b>PRECIO UNIT.</b>", style_bold),
            Paragraph("<b>SUBTOTAL</b>", style_bold)
        ]]
        
        monto_total = 0
        for item in items:
            # Convertir lote e item manejando decimales
            try:
                lote_int = int(float(item['lote'])) if item['lote'] else 0
            except:
                lote_int = 0
            
            try:
                item_int = int(float(item['item'])) if item['item'] else 0
            except:
                item_int = 0
            
            try:
                cantidad_val = int(float(item['cantidad'])) if item['cantidad'] else 0
            except:
                cantidad_val = 0
            
            subtotal = cantidad_val * float(item['precio_unitario'])
            monto_total += subtotal
            
            # Descripción completa sin cortar
            descripcion_completa = item['descripcion']
            
            # Unidad de medida (si no existe en item, usar "UNIDAD")
            unidad_medida = item.get('unidad_medida', 'UNIDAD')
            
            items_table_data.append([
                Paragraph(str(lote_int), style_normal),
                Paragraph(str(item_int), style_normal),
                Paragraph(f"{cantidad_val:,}".replace(",", "."), style_normal),
                Paragraph(unidad_medida, style_normal),
                Paragraph(descripcion_completa, style_normal),
                Paragraph(f"Gs. {float(item['precio_unitario']):,.0f}".replace(",", "."), style_normal),
                Paragraph(f"Gs. {subtotal:,.0f}".replace(",", "."), style_normal)
            ])
        
        tabla_items = Table(items_table_data, colWidths=[1.2*cm, 1.2*cm, 1.2*cm, 1.5*cm, 7*cm, 2.5*cm, 2.5*cm])
        tabla_items.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('ALIGN', (0, 0), (3, -1), 'CENTER'),
            ('ALIGN', (5, 1), (6, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('RIGHTPADDING', (0, 0), (-1, -1), 2),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(tabla_items)
        story.append(Spacer(1, 0.1*cm))
        
        # ========== SECCIÓN 5: MONTO TOTAL ==========
        monto_letras = numero_a_letras(int(monto_total))
        
        total_data = [
            [Paragraph("<b>MONTO TOTAL:</b>", style_bold), 
             Paragraph(f"<b>Gs. {monto_total:,.0f}</b>".replace(",", "."), style_bold)],
            [Paragraph("<b>MONTO EN LETRAS:</b>", style_bold), 
             Paragraph(monto_letras, style_normal)]
        ]
        
        tabla_total = Table(total_data, colWidths=[4.5*cm, 13.5*cm])
        tabla_total.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(tabla_total)
        story.append(Spacer(1, 0.15*cm))
        
        # ========== SECCIÓN 6: FIRMAS ==========
        firmas_header = [[Paragraph("<b>FIRMAS Y AUTORIZACIONES</b>", style_section_title)]]
        tabla_firmas_header = Table(firmas_header, colWidths=[18*cm])
        tabla_firmas_header.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('BACKGROUND', (0, 0), (-1, -1), colors.grey),
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(tabla_firmas_header)
        
        # 2 cuadros grandes para firmar y sellar
        firmas_data = [
            [Paragraph("", style_normal), Paragraph("", style_normal)],
            [Paragraph("<b>DIRECTOR ADMINISTRATIVO</b>", style_bold), 
             Paragraph("<b>DIRECTOR GENERAL Y/O REGIONAL</b>", style_bold)]
        ]
        
        tabla_firmas = Table(firmas_data, colWidths=[9*cm, 9*cm], rowHeights=[2.5*cm, 0.6*cm])
        tabla_firmas.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('BOX', (0, 0), (0, -1), 1, colors.black),  # Cuadro izquierdo
            ('BOX', (1, 0), (1, -1), 1, colors.black),  # Cuadro derecho
            ('LINEABOVE', (0, 1), (-1, 1), 0.5, colors.black),  # Línea arriba de nombres
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
            ('VALIGN', (0, 1), (-1, 1), 'MIDDLE'),
            ('BACKGROUND', (0, 1), (-1, 1), colors.lightgrey),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(tabla_firmas)
        story.append(Spacer(1, 0.15*cm))
        
        # ========== PIE DE PÁGINA: Recepción ==========
        recepcion_data = [
            [Paragraph("<b>RECEPCIÓN DE ORDEN DE COMPRA</b>", style_section_title)],
            [Paragraph(f"<b>Empresa:</b> {licitacion[4] if licitacion[4] else 'N/A'}", style_normal)],
            [Paragraph(f"<b>RUC:</b> {licitacion[5] if licitacion[5] else 'N/A'}", style_normal)],
            [Paragraph("<b>Fecha de Recepción:</b> _____ / _____ / __________", style_normal)]
        ]
        
        tabla_recepcion = Table(recepcion_data, colWidths=[18*cm])
        tabla_recepcion.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('BACKGROUND', (0, 0), (0, 0), colors.grey),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        story.append(tabla_recepcion)
        
        # Construir PDF
        doc.build(story)
        
        # Retornar bytes
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        return pdf_bytes, None
        
    except Exception as e:
        return None, f"Error generando PDF: {e}"

def generar_numero_acta():
    """Genera el siguiente número de acta correlativo del año actual"""
    try:
        anio_actual = datetime.now().year
        
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            # Obtener el último número del año
            query = text("""
                SELECT numero_acta 
                FROM public.actas_recepcion 
                WHERE anio = :anio 
                ORDER BY numero_acta DESC 
                LIMIT 1
            """)
            result = conn.execute(query, {"anio": anio_actual})
            ultimo = result.fetchone()
            
            if ultimo:
                # Extraer número (001/2025 -> 001)
                num = int(ultimo[0].split('/')[0])
                siguiente = num + 1
            else:
                siguiente = 1
            
            # Formatear como 001/2025
            numero_acta = f"{siguiente:03d}/{anio_actual}"
            return numero_acta
            
    except Exception as e:
        st.error(f"Error generando número de acta: {e}")
        return None

def guardar_acta_recepcion(esquema, numero_orden, numero_acta, fecha_recepcion, 
                           fecha_emision_remision, fecha_emision_factura,
                           numero_remision, numero_factura, fiscalizador, 
                           observaciones, items_recibidos, usuario):
    """Guarda un acta de recepción en la BD"""
    try:
        import json
        from decimal import Decimal
        
        # Convertir Decimals a float en items
        def decimal_to_float(obj):
            if isinstance(obj, list):
                return [decimal_to_float(item) for item in obj]
            elif isinstance(obj, dict):
                return {key: decimal_to_float(value) for key, value in obj.items()}
            elif isinstance(obj, Decimal):
                return float(obj)
            return obj
        
        items_json = decimal_to_float(items_recibidos)
        
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            # Verificar si el número de acta ya existe
            check_query = text("""
                SELECT id FROM public.actas_recepcion 
                WHERE numero_acta = :numero_acta
            """)
            existe = conn.execute(check_query, {"numero_acta": numero_acta}).fetchone()
            
            if existe:
                st.error(f"⚠️ El número de acta {numero_acta} ya existe. Use otro número.")
                return None
            
            query = text("""
                INSERT INTO public.actas_recepcion (
                    numero_acta, anio, esquema, numero_orden,
                    fecha_recepcion, fecha_emision_remision, fecha_emision_factura,
                    numero_remision, numero_factura, fiscalizador,
                    observaciones, items_recibidos, pdf_generado,
                    usuario_creacion
                ) VALUES (
                    :numero_acta, :anio, :esquema, :numero_orden,
                    :fecha_recepcion, :fecha_rem, :fecha_fac,
                    :num_rem, :num_fac, :fiscalizador,
                    :observaciones, CAST(:items AS jsonb), TRUE,
                    :usuario
                )
                RETURNING id
            """)
            
            result = conn.execute(query, {
                "numero_acta": numero_acta,
                "anio": int(numero_acta.split('/')[1]),
                "esquema": esquema,
                "numero_orden": numero_orden,
                "fecha_recepcion": fecha_recepcion,
                "fecha_rem": fecha_emision_remision,
                "fecha_fac": fecha_emision_factura,
                "num_rem": numero_remision,
                "num_fac": numero_factura,
                "fiscalizador": fiscalizador,
                "observaciones": observaciones,
                "items": json.dumps(items_json),
                "usuario": usuario
            })
            
            conn.commit()
            acta_id = result.fetchone()[0]
            return acta_id
            
    except Exception as e:
        st.error(f"Error guardando acta: {e}")
        import traceback
        st.error(traceback.format_exc())
        return None

def obtener_actas_orden(esquema, numero_orden=None):
    """Obtiene todas las actas de una orden, o todas las actas del esquema si numero_orden es None"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            if numero_orden:
                query = text("""
                    SELECT id, numero_acta, fecha_recepcion, fiscalizador,
                           estado, fecha_creacion, usuario_creacion
                    FROM public.actas_recepcion
                    WHERE esquema = :esquema AND numero_orden = :numero_orden
                    ORDER BY fecha_creacion DESC
                """)
                result = conn.execute(query, {"esquema": esquema, "numero_orden": numero_orden})
            else:
                query = text("""
                    SELECT id, numero_acta, fecha_recepcion, numero_orden, estado, 
                           fecha_creacion, usuario_creacion
                    FROM public.actas_recepcion
                    WHERE esquema = :esquema
                    ORDER BY fecha_creacion DESC
                """)
                result = conn.execute(query, {"esquema": esquema})
            return result.fetchall()
    except Exception as e:
        st.error(f"Error obteniendo actas: {e}")
        return []

def obtener_acta_por_id(acta_id):
    """Obtiene datos completos de un acta"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text("""
                SELECT id, numero_acta, anio, numero_orden, fecha_recepcion, 
                       fecha_emision_remision, fecha_emision_factura,
                       numero_remision, numero_factura, fiscalizador, observaciones,
                       items_recibidos, esquema
                FROM public.actas_recepcion
                WHERE id = :acta_id
            """)
            result = conn.execute(query, {"acta_id": acta_id})
            return result.fetchone()
    except Exception as e:
        st.error(f"Error obteniendo acta: {e}")
        return None

def modificar_acta_recepcion(acta_id, numero_acta, fecha_recepcion, fecha_emision_remision,
                             fecha_emision_factura, numero_remision, numero_factura,
                             fiscalizador, observaciones, items_recibidos, usuario):
    """Modifica un acta existente"""
    try:
        import json
        
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text("""
                UPDATE public.actas_recepcion SET
                    numero_acta = :numero_acta,
                    fecha_recepcion = :fecha_recepcion,
                    fecha_emision_remision = :fecha_rem,
                    fecha_emision_factura = :fecha_fac,
                    numero_remision = :num_rem,
                    numero_factura = :num_fac,
                    fiscalizador = :fiscalizador,
                    observaciones = :observaciones,
                    items_recibidos = CAST(:items AS jsonb),
                    fecha_modificacion = CURRENT_TIMESTAMP,
                    usuario_modificacion = :usuario,
                    estado = 'MODIFICADA'
                WHERE id = :acta_id
            """)
            
            conn.execute(query, {
                "acta_id": acta_id,
                "numero_acta": numero_acta,
                "fecha_recepcion": fecha_recepcion,
                "fecha_rem": fecha_emision_remision,
                "fecha_fac": fecha_emision_factura,
                "num_rem": numero_remision,
                "num_fac": numero_factura,
                "fiscalizador": fiscalizador,
                "observaciones": observaciones,
                "items": json.dumps(items_recibidos),
                "usuario": usuario
            })
            conn.commit()
            return True
            
    except Exception as e:
        st.error(f"Error modificando acta: {e}")
        return False

def generar_pdf_acta_recepcion(esquema, numero_oc, fecha_recepcion, servicio, empresa, 
                                items, lugar_entrega, numero_remision, numero_factura, receptor_nombre, 
                                observaciones, usuario, fecha_remision=None, fecha_factura=None, numero_acta=None):
    """
    Genera PDF HORIZONTAL de Acta de Recepción - VERSIÓN ACTUALIZADA BRUNO
    
    CAMBIOS IMPLEMENTADOS:
    - Logo institucional ancho en lugar de texto
    - Nombre del llamado en el texto intro
    - I.D. del llamado formateado
    - Fechas de emisión para remisión y factura
    - Número de acta (001/2025)
    - 5 cuadros con firmas
    """
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
        import os
        
        # Crear buffer
        buffer = io.BytesIO()
        
        # Configurar documento A4 HORIZONTAL (landscape) - Márgenes reducidos
        doc = SimpleDocTemplate(
            buffer, 
            pagesize=landscape(A4),
            rightMargin=2*cm,
            leftMargin=2*cm,
            topMargin=0.8*cm,   # Reducido
            bottomMargin=0.9*cm  # Reducido
        )
        
        # Definir colores
        GRIS_TABLA = colors.Color(0.80, 0.80, 0.80)       # #CCCCCC
        GRIS_SUAVE = colors.Color(0.93, 0.93, 0.93)       # #EDEDED
        
        # Estilos
        styles = getSampleStyleSheet()
        
        style_titulo_acta = ParagraphStyle(
            'TituloActa',
            parent=styles['Heading1'],
            fontSize=14,
            textColor=colors.black,
            spaceAfter=0,
            spaceBefore=0,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        style_normal = ParagraphStyle(
            'Normal',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            alignment=TA_JUSTIFY,
            fontName='Helvetica',
            leading=11
        )
        
        style_bold = ParagraphStyle(
            'Bold',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.black,
            alignment=TA_LEFT,
            fontName='Helvetica-Bold'
        )
        
        style_campo = ParagraphStyle(
            'Campo',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.black,
            alignment=TA_LEFT,
            fontName='Helvetica'
        )
        
        style_tabla_header = ParagraphStyle(
            'TablaHeader',
            parent=styles['Normal'],
            fontSize=7,
            textColor=colors.black,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        style_tabla_cell = ParagraphStyle(
            'TablaCell',
            parent=styles['Normal'],
            fontSize=7,
            textColor=colors.black,
            alignment=TA_CENTER,
            fontName='Helvetica'
        )
        
        style_firma = ParagraphStyle(
            'Firma',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.black,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        style_pie = ParagraphStyle(
            'Pie',
            parent=styles['Normal'],
            fontSize=6,
            textColor=colors.black,
            alignment=TA_CENTER,
            fontName='Helvetica'
        )
        
        # Contenido
        story = []
        
        # Obtener datos de licitación
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text(f"""
                SELECT "NUMERO_DE_LLAMADO", "AÑO_DEL_LLAMADO", "NOMBRE_DEL_LLAMADO", 
                       "I_D", "EMPRESA_ADJUDICADA", "RUC"
                FROM "{esquema}"."llamado"
                LIMIT 1
            """)
            result = conn.execute(query)
            licitacion = result.fetchone()
        
        # Extraer datos
        llamado_numero = licitacion[0] if licitacion else "N/A"
        llamado_anio = licitacion[1] if licitacion else "N/A"
        llamado_nombre = licitacion[2] if licitacion else "N/A"
        id_llamado = licitacion[3] if licitacion else 0
        empresa_adjudicada = licitacion[4] if licitacion and licitacion[4] else empresa
        ruc_empresa = licitacion[5] if licitacion and licitacion[5] else "N/A"
        
        # Formatear I.D. con puntos (123456 → 123.456)
        id_formateado = f"{id_llamado:,}".replace(",", ".") if id_llamado else "N/A"
        
        # Formatear fecha de recepción
        if isinstance(fecha_recepcion, str):
            try:
                from datetime import datetime
                fecha_obj = datetime.strptime(fecha_recepcion, "%Y-%m-%d")
                dia = fecha_obj.day
                mes = fecha_obj.strftime("%B")
                anio = fecha_obj.year
                fecha_mostrar = fecha_obj.strftime("%d/%m/%Y")
            except:
                dia = "___"
                mes = "___________"
                anio = "____"
                fecha_mostrar = fecha_recepcion
        else:
            dia = fecha_recepcion.day
            meses_es = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                       "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
            mes = meses_es[fecha_recepcion.month - 1]
            anio = fecha_recepcion.year
            fecha_mostrar = fecha_recepcion.strftime("%d/%m/%Y")
        
        # Fechas de remisión y factura (usar fecha_recepcion si no se proporcionan)
        if fecha_remision:
            if isinstance(fecha_remision, str):
                fecha_remision_mostrar = fecha_remision
            else:
                fecha_remision_mostrar = fecha_remision.strftime("%d/%m/%Y")
        else:
            fecha_remision_mostrar = fecha_mostrar
        
        if fecha_factura:
            if isinstance(fecha_factura, str):
                fecha_factura_mostrar = fecha_factura
            else:
                fecha_factura_mostrar = fecha_factura.strftime("%d/%m/%Y")
        else:
            fecha_factura_mostrar = fecha_mostrar
        
        # ========== ENCABEZADO CON LOGO ==========
        # Intentar cargar logo institucional
        logos_dir = "/home/claude/logos_gobierno"
        logo_path = os.path.join(logos_dir, "logo_principal.png")
        
        if os.path.exists(logo_path):
            # Logo existe - usarlo ANCHO y BAJO
            logo = RLImage(logo_path, width=25.7*cm, height=1.5*cm, kind='proportional')
            header_data = [[logo]]
        else:
            # No hay logo - usar texto simple
            header_data = [[Paragraph(
                "<b>MINISTERIO DE SALUD PÚBLICA Y BIENESTAR SOCIAL</b><br/>" +
                "DIRECCIÓN GENERAL DE GESTIÓN DE INSUMOS ESTRATÉGICOS EN SALUD",
                style_titulo_acta
            )]]
        
        tabla_header = Table(header_data, colWidths=[25.7*cm])
        tabla_header.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOX', (0, 0), (-1, -1), 0.5, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(tabla_header)
        story.append(Spacer(1, 0.2*cm))
        
        # Título ACTA (sin DEFINITIVA)
        titulo_text = "<b>📋 ACTA DE RECEPCIÓN</b>"
        if numero_acta:
            titulo_text += f"<br/><b>N° {numero_acta}</b>"
        
        titulo_data = [[Paragraph(titulo_text, style_titulo_acta)]]
        tabla_titulo = Table(titulo_data, colWidths=[25.7*cm])
        tabla_titulo.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        story.append(tabla_titulo)
        story.append(Spacer(1, 0.2*cm))
        
        # ========== TEXTO INTRODUCCIÓN (sin DEFINITIVA) ==========
        texto_intro = f"""A los <b>{dia}</b> días del mes de <b>{mes}</b> del año <b>{anio}</b> en el <b>{servicio.upper()}</b> 
dependiente del Ministerio de Salud Pública y Bienestar Social, se procede a la elaboración del Acta de 
Recepción, de lo adjudicado a la empresa <b>{empresa_adjudicada}</b> correspondiente al llamado 
<b>LPN {llamado_numero}/{llamado_anio} "{llamado_nombre}" I.D. N° {id_formateado}</b> - los cuales se describen 
en el siguiente cuadro y firman al pie conformes los responsables por parte de la Institución y la Empresa Adjudicada."""
        
        intro_data = [[Paragraph(texto_intro, style_normal)]]
        tabla_intro = Table(intro_data, colWidths=[25.7*cm])
        tabla_intro.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(tabla_intro)
        story.append(Spacer(1, 0.15*cm))
        
        # ========== TABLA DE ITEMS (COMPACTA) ==========
        items_table_data = [[
            Paragraph("<b>LOTE</b>", style_tabla_header),
            Paragraph("<b>ITEM</b>", style_tabla_header),
            Paragraph("<b>CANT.</b>", style_tabla_header),
            Paragraph("<b>UNIDAD</b>", style_tabla_header),
            Paragraph("<b>DESCRIPCIÓN // PRESENTACIÓN // MARCA // PROCEDENCIA</b>", style_tabla_header),
            Paragraph("<b>PRECIO UNIT.</b>", style_tabla_header),
            Paragraph("<b>TOTAL</b>", style_tabla_header)
        ]]
        
        monto_total = 0
        for item in items:
            try:
                lote_int = int(float(item['lote'])) if item['lote'] else 0
            except:
                lote_int = 0
            
            try:
                item_int = int(float(item['item'])) if item['item'] else 0
            except:
                item_int = 0
            
            try:
                cantidad_val = int(float(item['cantidad'])) if item['cantidad'] else 0
            except:
                cantidad_val = 0
            
            try:
                precio_unit = float(item['precio_unitario']) if item['precio_unitario'] else 0
            except:
                precio_unit = 0
            
            subtotal = cantidad_val * precio_unit
            monto_total += subtotal
            
            unidad_medida = item.get('unidad_medida', 'UNIDAD')
            
            items_table_data.append([
                Paragraph(str(lote_int), style_tabla_cell),
                Paragraph(str(item_int), style_tabla_cell),
                Paragraph(f"{cantidad_val:,}".replace(",", "."), style_tabla_cell),
                Paragraph(unidad_medida, style_tabla_cell),
                Paragraph(item['descripcion'], style_tabla_cell),
                Paragraph(f"Gs. {precio_unit:,.0f}".replace(",", "."), style_tabla_cell),
                Paragraph(f"Gs. {subtotal:,.0f}".replace(",", "."), style_tabla_cell)
            ])
        
        # Fila de TOTAL - combinar columnas LOTE a UNIDAD
        items_table_data.append([
            Paragraph("<b>TOTAL:</b>", style_tabla_header),
            Paragraph("", style_tabla_cell),
            Paragraph("", style_tabla_cell),
            Paragraph("", style_tabla_cell),
            Paragraph("", style_tabla_cell),
            Paragraph("", style_tabla_header),
            Paragraph(f"<b>Gs. {monto_total:,.0f}</b>".replace(",", "."), style_tabla_header)
        ])
        
        tabla_items = Table(items_table_data, colWidths=[1.3*cm, 1.3*cm, 1.8*cm, 1.8*cm, 12*cm, 3.25*cm, 3.25*cm])
        tabla_items.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('BACKGROUND', (0, 0), (-1, 0), GRIS_TABLA),
            ('ALIGN', (0, 0), (3, -1), 'CENTER'),
            ('ALIGN', (4, 1), (4, -2), 'LEFT'),
            ('ALIGN', (5, 0), (-1, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('RIGHTPADDING', (0, 0), (-1, -1), 2),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ('BACKGROUND', (0, -1), (-1, -1), GRIS_SUAVE),
            # Combinar celdas en última fila (LOTE a UNIDAD)
            ('SPAN', (0, -1), (4, -1)),
        ]))
        story.append(tabla_items)
        story.append(Spacer(1, 0.1*cm))
        
        # ========== MONTO EN LETRAS (COMPACTO) ==========
        monto_letras = numero_a_letras(int(monto_total))
        letras_data = [[Paragraph(f"<b>Son Guaraníes:</b> {monto_letras}", style_campo)]]
        tabla_letras = Table(letras_data, colWidths=[25.7*cm])
        tabla_letras.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.5, colors.black),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        story.append(tabla_letras)
        story.append(Spacer(1, 0.1*cm))
        
        # ========== OBSERVACIONES (COMPACTO) ==========
        if observaciones:
            obs_data = [
                [Paragraph("<b>OBSERVACIONES</b>", style_bold)],
                [Paragraph(observaciones, style_campo)]
            ]
            tabla_obs = Table(obs_data, colWidths=[25.7*cm])
            tabla_obs.setStyle(TableStyle([
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('BACKGROUND', (0, 0), (-1, 0), GRIS_SUAVE),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            story.append(tabla_obs)
            story.append(Spacer(1, 0.1*cm))
        
        # ========== SECCIÓN DE FIRMAS - 5 CUADROS EN LÍNEA ==========
        # Separar remisiones y facturas si vienen múltiples
        remisiones = numero_remision.split(',') if numero_remision else []
        facturas = numero_factura.split(',') if numero_factura else []
        
        # Construir filas para tabla interna
        datos_internos = []
        
        # ORDEN DE COMPRA
        datos_internos.append([
            Paragraph("<b>N° ORDEN DE COMPRA:</b>", style_campo), 
            Paragraph("<b>FECHA DE EMISION:</b>", style_campo)
        ])
        datos_internos.append([
            Paragraph(numero_oc, style_campo), 
            Paragraph(fecha_mostrar, style_campo)
        ])
        
        # REMISIONES
        datos_internos.append([
            Paragraph("<b>N° DE REMISION:</b>", style_campo), 
            Paragraph("<b>FECHA DE EMISION:</b>", style_campo)
        ])
        if remisiones:
            for rem in remisiones:
                datos_internos.append([
                    Paragraph(rem.strip(), style_campo), 
                    Paragraph(fecha_remision_mostrar, style_campo)
                ])
        else:
            datos_internos.append([
                Paragraph("N/A", style_campo), 
                Paragraph(fecha_remision_mostrar, style_campo)
            ])
        
        # FACTURAS
        datos_internos.append([
            Paragraph("<b>N° DE FACTURA:</b>", style_campo), 
            Paragraph("<b>FECHA DE EMISION:</b>", style_campo)
        ])
        if facturas:
            for fac in facturas:
                datos_internos.append([
                    Paragraph(f"• {fac.strip()}", style_campo), 
                    Paragraph(fecha_factura_mostrar, style_campo)
                ])
        else:
            datos_internos.append([
                Paragraph("N/A", style_campo), 
                Paragraph(fecha_factura_mostrar, style_campo)
            ])
        
        tabla_datos_internos = Table(datos_internos, colWidths=[3*cm, 3*cm])
        tabla_datos_internos.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ]))
        
        firmas_data = [
            # Fila 1: Tabla interna con datos en 2 columnas
            [tabla_datos_internos, "", "", "", ""],
            # Fila 2: Espacio firma
            ["", "", "", "", ""],
            # Fila 3: Headers
            ["", 
             Paragraph("<b>EMPRESA ADJUDICADA</b>", style_firma),
             Paragraph("<b>RESPONSABLE/JEFE<br/>SUMINISTROS</b>", style_firma),
             Paragraph("<b>FISCALIZADOR</b>", style_firma),
             Paragraph("<b>DIRECTOR GENERAL<br/>O REGIONAL</b>", style_firma)]
        ]
        
        tabla_firmas = Table(firmas_data, colWidths=[6*cm, 4.92*cm, 4.92*cm, 4.93*cm, 4.93*cm])
        tabla_firmas.setStyle(TableStyle([
            # Bordes externos
            ('BOX', (0, 0), (0, -1), 1, colors.black),
            ('BOX', (1, 0), (1, -1), 1, colors.black),
            ('BOX', (2, 0), (2, -1), 1, colors.black),
            ('BOX', (3, 0), (3, -1), 1, colors.black),
            ('BOX', (4, 0), (4, -1), 1, colors.black),
            
            # LÍNEA CONTINUA atravesando columnas 1-4 (antes de headers)
            ('LINEABOVE', (1, -1), (4, -1), 1, colors.black),
            
            # Alineación
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),
            ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -2), 'TOP'),
            ('VALIGN', (0, -1), (-1, -1), 'BOTTOM'),
            
            # Tamaño
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            
            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(tabla_firmas)
        story.append(Spacer(1, 0.1*cm))
        
        # ========== NOTA LEGAL PIE (MUY COMPACTO) ==========
        pie_data = [[Paragraph(
            "La veracidad de los datos consignados en el presente documento son de expresa " +
            "responsabilidad de los firmantes y tendrá carácter de DD.JJ., quedarán sin validez " +
            "ante la falta de firmas o si presenta enmiendas y/o tachaduras.",
            style_pie
        )]]
        tabla_pie = Table(pie_data, colWidths=[25.7*cm])
        tabla_pie.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.5, colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        story.append(tabla_pie)
        
        # Construir PDF
        doc.build(story)
        
        # Retornar bytes
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        return pdf_bytes
        
    except Exception as e:
        st.error(f"Error generando PDF del Acta: {e}")
        import traceback
        st.error(traceback.format_exc())
        return None
def pagina_ordenes_compra():
    """Página principal de gestión de órdenes de compra"""
    st.header("Gestión de Órdenes de Compra")
    
    # Obtener esquemas existentes
    esquemas = obtener_esquemas_postgres()

    # Pestañas para diferentes funciones
    tab1, tab2, tab3 = st.tabs(["Lista de Órdenes", "Emitir Nueva Orden", "Actas de Recepción"])
    
    with tab1:
        st.subheader("Órdenes de Compra Emitidas")
        
        # Obtener órdenes de compra
        ordenes = obtener_ordenes_compra()
        
        if ordenes and len(ordenes) > 0:
            # Función para remover acentos y normalizar texto
            def normalizar_texto(texto):
                """Remueve acentos y convierte a minúsculas para búsqueda flexible"""
                import unicodedata
                if not texto:
                    return ""
                texto = str(texto).lower()
                # Remover acentos
                texto = ''.join(
                    c for c in unicodedata.normalize('NFD', texto)
                    if unicodedata.category(c) != 'Mn'
                )
                return texto
            
            # BUSCADOR Y FILTROS
            col1, col2 = st.columns([3, 1])
            with col1:
                busqueda = st.text_input("🔍 Buscar (sin acentos, flexible):", 
                                        key="buscar_orden",
                                        placeholder="Ej: hospital, oxigena, san lorenzo...")
            with col2:
                # Formatear esquemas para el selector
                esquemas_formateados = {}
                for esq in esquemas:
                    esq_bonito = esq.replace('lpn_', 'LPN ').replace('_', ' ').upper()
                    esquemas_formateados[esq_bonito] = esq
                
                opciones_filtro = ["Todos"] + list(esquemas_formateados.keys())
                esquema_seleccionado = st.selectbox(
                    "Filtrar por Licitación:",
                    options=opciones_filtro,
                    key="filtro_esquema_ordenes"
                )
            
            # Filtrar órdenes
            ordenes_filtradas = ordenes
            
            # Filtro por esquema
            if esquema_seleccionado != "Todos":
                esquema_raw = esquemas_formateados[esquema_seleccionado]
                ordenes_filtradas = [o for o in ordenes_filtradas if o['esquema'] == esquema_raw]
            
            # Búsqueda flexible (sin acentos)
            if busqueda and busqueda.strip():
                busqueda_norm = normalizar_texto(busqueda)
                ordenes_filtradas = [
                    o for o in ordenes_filtradas 
                    if busqueda_norm in normalizar_texto(o.get('numero_orden', ''))
                    or busqueda_norm in normalizar_texto(o.get('servicio_beneficiario', ''))
                    or busqueda_norm in normalizar_texto(o.get('empresa', ''))
                ]
            
            if ordenes_filtradas:
                # Mostrar contador compacto
                st.write(f"**Total: {len(ordenes_filtradas)} órdenes encontradas**")
                
                for orden in ordenes_filtradas:
                    # Formatear fecha (YYYY-MM-DD → DD/MM/YYYY)
                    fecha_obj = orden.get('fecha_emision')
                    if fecha_obj:
                        if isinstance(fecha_obj, str):
                            try:
                                partes = fecha_obj.split('-')
                                fecha_mostrar = f"{partes[2]}/{partes[1]}/{partes[0]}"
                            except:
                                fecha_mostrar = str(fecha_obj)
                        else:
                            fecha_mostrar = fecha_obj.strftime('%d/%m/%Y')
                    else:
                        fecha_mostrar = 'N/A'
                    
                    # Formatear esquema (lpn_100/2023 → LPN 100/2023)
                    esquema_raw = orden.get('esquema', 'N/A')
                    esquema_mostrar = esquema_raw.replace('lpn_', 'LPN ').replace('_', ' ').upper()
                    
                    # Obtener empresa del esquema
                    try:
                        with engine.connect() as conn_empresa:
                            query_empresa = text(f"""
                                SELECT "EMPRESA_ADJUDICADA"
                                FROM "{esquema_raw}"."llamado"
                                LIMIT 1
                            """)
                            result_empresa = conn_empresa.execute(query_empresa)
                            empresa = result_empresa.scalar() or 'N/A'
                    except:
                        empresa = 'N/A'
                    
                    # Obtener detalles completos de la orden
                    orden_completa = obtener_detalles_orden_compra(orden['id'])
                    
                    # Expander con TODO adentro
                    with st.expander(f"📋 {orden['numero_orden']} - {orden.get('servicio_beneficiario', 'N/A')}", expanded=False):
                        if orden_completa:
                            # === SECCIÓN 1: INFO GENERAL ===
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.markdown(f"**📅 Fecha Emisión:** {fecha_mostrar}")
                                st.markdown(f"**📂 Licitación:** {esquema_mostrar}")
                                st.markdown(f"**🏢 Empresa:** {empresa}")
                                st.markdown(f"**📋 Estado:** {orden.get('estado', 'N/A')}")
                            
                            with col2:
                                st.markdown(f"**👤 Usuario:** {orden.get('usuario', 'N/A')}")
                                st.markdown(f"**🏥 Servicio:** {orden.get('servicio_beneficiario', 'N/A')}")
                                st.markdown(f"**📦 Items:** {orden.get('cantidad_items', 0)}")
                                st.markdown(f"**💰 Monto Total:** ₲ {orden.get('monto_total', 0):,.0f}".replace(",", "."))
                            
                            # === SECCIÓN 2: DETALLE DE ITEMS ===
                            st.markdown("---")
                            st.markdown("**📦 Detalle de Items:**")
                            
                            # Formatear items
                            items_display = []
                            for item in orden_completa['items']:
                                # Limpiar decimales
                                try:
                                    lote_val = int(float(item['lote']))
                                except:
                                    lote_val = item['lote']
                                
                                try:
                                    item_val = int(float(item['item']))
                                except:
                                    item_val = item['item']
                                
                                try:
                                    cantidad_val = int(float(item['cantidad']))
                                    cantidad_fmt = f"{cantidad_val:,}".replace(',', '.')
                                except:
                                    cantidad_fmt = str(item['cantidad'])
                                
                                try:
                                    precio_val = int(float(item['precio_unitario']))
                                    precio_fmt = f"₲ {precio_val:,}".replace(',', '.')
                                except:
                                    precio_fmt = str(item['precio_unitario'])
                                
                                try:
                                    monto_val = int(float(item['monto_total']))
                                    monto_fmt = f"₲ {monto_val:,}".replace(',', '.')
                                except:
                                    monto_fmt = str(item['monto_total'])
                                
                                items_display.append({
                                    'Lote': lote_val,
                                    'Item': item_val,
                                    'Descripción': item['descripcion'],
                                    'Cantidad': cantidad_fmt,
                                    'Unidad de Medida': item.get('unidad_medida', 'UNIDAD'),
                                    'Precio Unitario': precio_fmt,
                                    'Monto Total': monto_fmt
                                })
                            
                            df_items = pd.DataFrame(items_display)
                            st.dataframe(df_items, use_container_width=True, height=200)
                            
                            # === SECCIÓN 3: BOTONES ===
                            st.markdown("---")
                            col_btn1, col_btn2 = st.columns(2)
                            
                            with col_btn1:
                                modificar_clicked = st.button("✏️ Modificar", key=f"modificar_{orden['id']}", use_container_width=True)
                            
                            with col_btn2:
                                pdf_clicked = st.button("📄 Orden Compra", key=f"pdf_{orden['id']}", use_container_width=True)
                            
                            # === PANEL DE MODIFICACIÓN ===
                            # Usar session_state para mantener el panel abierto
                            if modificar_clicked:
                                st.session_state[f'mostrar_modificar_{orden["id"]}'] = True
                            
                            if st.session_state.get(f'mostrar_modificar_{orden["id"]}', False):
                                st.markdown("---")
                                st.markdown("### ✏️ Modificar Orden")
                                
                                # Si ya hay confirmación pendiente, mostrar SOLO la confirmación
                                if f'confirmar_mod_{orden["id"]}' in st.session_state:
                                    st.warning("⚠️ **¿Está seguro de guardar estos cambios?**")
                                    col_c1, col_c2 = st.columns(2)
                                    
                                    with col_c1:
                                        if st.button("✅ SÍ, GUARDAR", type="primary", key=f"si_{orden['id']}", use_container_width=True):
                                            datos = st.session_state[f'confirmar_mod_{orden["id"]}']
                                            nueva_cantidad = datos['nueva_cantidad']
                                            nueva_fecha = datos['nueva_fecha']
                                            cantidad_actual_orden = datos['cantidad_actual']
                                            fecha_actual = datos['fecha_actual']
                                            lote_real = datos['lote']
                                            item_real = datos['item']
                                            
                                            if nueva_cantidad == 0:
                                                with engine.connect() as conn_del:
                                                    update_ejec = text(f"""
                                                        UPDATE "{orden_completa['esquema']}"."ejecucion_general"
                                                        SET "CANTIDAD_EMITIDA" = "CANTIDAD_EMITIDA" - :cantidad,
                                                            "SALDO_A_EMITIR" = "CANTIDAD_MAXIMA" - ("CANTIDAD_EMITIDA" - :cantidad)
                                                        WHERE "LOTE" = :lote AND "ITEM" = :item AND "SERVICIO_BENEFICIARIO" = :servicio
                                                    """)
                                                    conn_del.execute(update_ejec, {
                                                        'cantidad': cantidad_actual_orden,
                                                        'lote': lote_real,
                                                        'item': item_real,
                                                        'servicio': orden_completa['servicio_beneficiario']
                                                    })
                                                    
                                                    delete_query = text(f"""
                                                        DELETE FROM "{orden_completa['esquema']}".orden_de_compra
                                                        WHERE "NUMERO_ORDEN_DE_COMPRA" = :numero AND "SERVICIO_BENEFICIARIO" = :servicio
                                                    """)
                                                    conn_del.execute(delete_query, {
                                                        'numero': orden_completa['numero_orden'],
                                                        'servicio': orden_completa['servicio_beneficiario']
                                                    })
                                                    conn_del.commit()
                                                
                                                st.success("✅ Orden eliminada")
                                                del st.session_state[f'confirmar_mod_{orden["id"]}']
                                                del st.session_state[f'mostrar_modificar_{orden["id"]}']
                                                time.sleep(1)
                                                st.rerun()
                                            
                                            elif nueva_cantidad != cantidad_actual_orden or nueva_fecha != fecha_actual:
                                                diferencia = nueva_cantidad - cantidad_actual_orden
                                                nueva_fecha_str = nueva_fecha.strftime('%Y-%m-%d')
                                                
                                                with engine.connect() as conn_upd:
                                                    update_query = text(f"""
                                                        UPDATE "{orden_completa['esquema']}".orden_de_compra
                                                        SET "CANTIDAD_SOLICITADA" = :cantidad, "FECHA_DE_EMISION" = :fecha
                                                        WHERE "NUMERO_ORDEN_DE_COMPRA" = :numero AND "SERVICIO_BENEFICIARIO" = :servicio
                                                    """)
                                                    conn_upd.execute(update_query, {
                                                        'cantidad': nueva_cantidad,
                                                        'fecha': nueva_fecha_str,
                                                        'numero': orden_completa['numero_orden'],
                                                        'servicio': orden_completa['servicio_beneficiario']
                                                    })
                                                    
                                                    if diferencia != 0:
                                                        update_ejec = text(f"""
                                                            UPDATE "{orden_completa['esquema']}"."ejecucion_general"
                                                            SET "CANTIDAD_EMITIDA" = "CANTIDAD_EMITIDA" + :diferencia,
                                                                "SALDO_A_EMITIR" = "CANTIDAD_MAXIMA" - ("CANTIDAD_EMITIDA" + :diferencia)
                                                            WHERE "LOTE" = :lote AND "ITEM" = :item AND "SERVICIO_BENEFICIARIO" = :servicio
                                                        """)
                                                        conn_upd.execute(update_ejec, {
                                                            'diferencia': diferencia,
                                                            'lote': lote_real,
                                                            'item': item_real,
                                                            'servicio': orden_completa['servicio_beneficiario']
                                                        })
                                                    
                                                    conn_upd.commit()
                                                
                                                st.success("✅ Orden modificada")
                                                del st.session_state[f'confirmar_mod_{orden["id"]}']
                                                del st.session_state[f'mostrar_modificar_{orden["id"]}']
                                                time.sleep(1)
                                                st.rerun()
                                            else:
                                                st.warning("⚠️ No hay cambios")
                                                del st.session_state[f'confirmar_mod_{orden["id"]}']
                                    
                                    with col_c2:
                                        if st.button("❌ NO, CANCELAR", key=f"no_{orden['id']}", use_container_width=True):
                                            del st.session_state[f'confirmar_mod_{orden["id"]}']
                                            st.rerun()
                                
                                # Si NO hay confirmación pendiente, mostrar el formulario
                                else:
                                    try:
                                        with engine.connect() as conn_mod:
                                            query_orden = text(f"""
                                                SELECT "LOTE", "ITEM"
                                                FROM "{orden_completa['esquema']}".orden_de_compra
                                                WHERE "NUMERO_ORDEN_DE_COMPRA" = :numero
                                                  AND "SERVICIO_BENEFICIARIO" = :servicio
                                                LIMIT 1
                                            """)
                                            
                                            result_orden = conn_mod.execute(query_orden, {
                                                'numero': orden_completa['numero_orden'],
                                                'servicio': orden_completa['servicio_beneficiario']
                                            })
                                            
                                            row_orden = result_orden.fetchone()
                                            
                                            if row_orden:
                                                lote_real = row_orden[0]
                                                item_real = row_orden[1]
                                                
                                                query_disponible = text(f"""
                                                    SELECT 
                                                        "CANTIDAD_MAXIMA",
                                                        "CANTIDAD_EMITIDA",
                                                        "SALDO_A_EMITIR"
                                                    FROM "{orden_completa['esquema']}"."ejecucion_general"
                                                    WHERE "LOTE" = :lote
                                                      AND "ITEM" = :item
                                                      AND "SERVICIO_BENEFICIARIO" = :servicio
                                                    LIMIT 1
                                                """)
                                                
                                                result = conn_mod.execute(query_disponible, {
                                                    'lote': lote_real,
                                                    'item': item_real,
                                                    'servicio': orden_completa['servicio_beneficiario']
                                                })
                                                
                                                row = result.fetchone()
                                                
                                                if row:
                                                    cantidad_maxima = float(row[0]) if row[0] else 0
                                                    cantidad_emitida = float(row[1]) if row[1] else 0
                                                    saldo_a_emitir = float(row[2]) if row[2] else 0
                                                    porcentaje_emitido = (cantidad_emitida / cantidad_maxima * 100) if cantidad_maxima > 0 else 0
                                                    cantidad_actual_orden = float(orden_completa['items'][0]['cantidad'])
                                                    
                                                    col_info1, col_info2, col_info3 = st.columns(3)
                                                    with col_info1:
                                                        st.metric("Máximo", f"{int(cantidad_maxima):,}".replace(',', '.'))
                                                    with col_info2:
                                                        st.metric("Emitido", f"{int(cantidad_emitida):,}".replace(',', '.'))
                                                    with col_info3:
                                                        st.metric("Disponible", f"{int(saldo_a_emitir):,}".replace(',', '.'))
                                                    
                                                    st.info(f"📊 Porcentaje emitido: {porcentaje_emitido:.1f}%")
                                                    
                                                    if porcentaje_emitido >= 100:
                                                        st.error("⚠️ Item al 100%. No se puede modificar.")
                                                    else:
                                                        with st.form(key=f"form_modificar_{orden['id']}"):
                                                            col_mod1, col_mod2 = st.columns(2)
                                                            
                                                            with col_mod1:
                                                                st.write("**Cantidad Actual:**", f"{int(cantidad_actual_orden):,}".replace(',', '.'))
                                                                nueva_cantidad = st.number_input(
                                                                    "Nueva Cantidad:",
                                                                    min_value=0,
                                                                    max_value=int(saldo_a_emitir + cantidad_actual_orden),
                                                                    value=int(cantidad_actual_orden),
                                                                    step=1,
                                                                    key=f"cant_{orden['id']}"
                                                                )
                                                            
                                                            with col_mod2:
                                                                st.write("**Fecha Actual:**", fecha_mostrar)
                                                                try:
                                                                    partes = fecha_mostrar.split('/')
                                                                    fecha_actual = date(int(partes[2]), int(partes[1]), int(partes[0]))
                                                                except:
                                                                    fecha_actual = date.today()
                                                                
                                                                nueva_fecha = st.date_input("Nueva Fecha:", value=fecha_actual, key=f"fecha_{orden['id']}")
                                                            
                                                            submit_mod = st.form_submit_button("📝 Confirmar Cambios", type="primary", use_container_width=True)
                                                        
                                                        if submit_mod:
                                                            st.session_state[f'confirmar_mod_{orden["id"]}'] = {
                                                                'nueva_cantidad': nueva_cantidad,
                                                                'nueva_fecha': nueva_fecha,
                                                                'cantidad_actual': cantidad_actual_orden,
                                                                'fecha_actual': fecha_actual,
                                                                'lote': lote_real,
                                                                'item': item_real
                                                            }
                                                            st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")
                                
                                # Botón para cerrar el panel
                                if st.button("🔙 Cerrar", key=f"cerrar_mod_{orden['id']}", use_container_width=True):
                                    if f'mostrar_modificar_{orden["id"]}' in st.session_state:
                                        del st.session_state[f'mostrar_modificar_{orden["id"]}']
                                    if f'confirmar_mod_{orden["id"]}' in st.session_state:
                                        del st.session_state[f'confirmar_mod_{orden["id"]}']
                                    st.rerun()
                            
                            # === PANEL DE PDF ===
                            if pdf_clicked:
                                try:
                                    items_pdf = []
                                    for item in orden_completa['items']:
                                        try:
                                            lote_clean = int(float(item['lote']))
                                        except:
                                            lote_clean = item['lote']
                                        
                                        try:
                                            item_clean = int(float(item['item']))
                                        except:
                                            item_clean = item['item']
                                        
                                        try:
                                            cantidad_clean = int(float(item['cantidad']))
                                        except:
                                            cantidad_clean = item['cantidad']
                                        
                                        items_pdf.append({
                                            'lote': lote_clean,
                                            'item': item_clean,
                                            'descripcion': item['descripcion'],
                                            'cantidad': cantidad_clean,
                                            'unidad_medida': item.get('unidad_medida', 'UNIDAD'),
                                            'precio_unitario': float(item['precio_unitario'])
                                        })
                                    
                                    pdf_bytes, error_pdf = generar_pdf_orden_compra(
                                        orden_completa['esquema'],
                                        orden_completa['numero_orden'].split('/')[0] if '/' in orden_completa['numero_orden'] else orden_completa['numero_orden'],
                                        orden_completa['numero_orden'].split('/')[1] if '/' in orden_completa['numero_orden'] else str(datetime.now().year),
                                        fecha_mostrar,
                                        orden_completa['servicio_beneficiario'],
                                        items_pdf,
                                        orden_completa.get('usuario', 'Sistema')
                                    )
                                    
                                    if pdf_bytes:
                                        st.download_button(
                                            label="💾 Descargar Orden de Compra PDF",
                                            data=pdf_bytes,
                                            file_name=f"OC_{orden['numero_orden']}.pdf",
                                            mime="application/pdf",
                                            key=f"download_{orden['id']}",
                                            use_container_width=True
                                        )
                                    else:
                                        st.error(f"Error: {error_pdf}")
                                except Exception as e:
                                    st.error(f"Error: {e}")
                            
                        else:
                            st.error("No se pudieron cargar los detalles")
            else:
                st.warning("📭 No se encontraron órdenes de compra con esos filtros")
        else:
            st.info("📭 No se encontraron órdenes de compra")
        
    
    with tab2:
        st.subheader("Emitir Nueva Orden de Compra")
        
        # Obtener usuario actual de session_state
        if 'usuario_actual' not in st.session_state:
            st.session_state.usuario_actual = "BRUNO GOMEZ"  # Admin por defecto
            st.session_state.servicio_usuario = None  # Admin ve todos
        
        # Mostrar usuario logueado
        st.info(f"👤 Usuario: **{st.session_state.usuario_actual}**")
        
        # Selector de esquema (licitación)
        esquema_seleccionado = st.selectbox(
            "Seleccionar Licitación:",
            options=esquemas,
            key="selector_esquema_nueva_orden"
        )
        
        if esquema_seleccionado:
            # Obtener información de la licitación
            engine = get_engine()
            if engine is None:
                st.error("No se pudo conectar a la base de datos")
                return
            with engine.connect() as conn:
                try:
                    query = text(f"""
                        SELECT "NUMERO_DE_LLAMADO", "AÑO_DEL_LLAMADO", "NOMBRE_DEL_LLAMADO", 
                               "EMPRESA_ADJUDICADA"
                        FROM "{esquema_seleccionado}"."llamado"
                        LIMIT 1
                    """)
                    result = conn.execute(query)
                    licitacion = result.fetchone()
                    
                    if licitacion:
                        st.write(f"**Licitación:** {licitacion[0]}/{licitacion[1]} - {licitacion[2]}")
                        st.write(f"**Empresa:** {licitacion[3]}")
                except Exception as e:
                    st.error(f"Error obteniendo datos de licitación: {e}")
            
            # Obtener lista de servicios beneficiarios
            servicios_todos = obtener_servicios_beneficiarios(esquema_seleccionado)
            
            # Filtrar servicios según el usuario
            if st.session_state.servicio_usuario:
                # Usuario normal: solo su servicio
                servicios = [st.session_state.servicio_usuario]
                servicio_seleccionado = st.session_state.servicio_usuario
                st.info(f"🏥 Tu servicio: **{servicio_seleccionado}**")
            else:
                # Admin: todos los servicios
                servicios = servicios_todos
                if servicios:
                    servicio_seleccionado = st.selectbox(
                        "Seleccionar Servicio Beneficiario:",
                        options=servicios,
                        key="selector_servicio_beneficiario"
                    )
                else:
                    servicio_seleccionado = None
            
            if servicios and servicio_seleccionado:
                st.markdown("---")
                
                # Obtener productos disponibles para este servicio
                engine = get_engine()
                if engine is None:
                    st.error("No se pudo conectar a la base de datos")
                    return
                with engine.connect() as conn:
                    try:
                        query = text(f"""
                            SELECT 
                                "LOTE", 
                                "ITEM", 
                                "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                                "CANTIDAD_MAXIMA", 
                                "CANTIDAD_EMITIDA", 
                                "PRECIO_UNITARIO",
                                ROUND(("CANTIDAD_EMITIDA" / NULLIF("CANTIDAD_MAXIMA", 0)) * 100, 2) as PORCENTAJE_EJECUCION,
                                "UNIDAD_DE_MEDIDA"
                            FROM "{esquema_seleccionado}"."ejecucion_general"
                            WHERE "SERVICIO_BENEFICIARIO" = :servicio
                              AND ("CANTIDAD_MAXIMA" - COALESCE("CANTIDAD_EMITIDA", 0)) > 0
                            ORDER BY "LOTE", "ITEM"
                        """)
                        result = conn.execute(query, {"servicio": servicio_seleccionado})
                        productos = result.fetchall()
                        
                        if productos:
                            st.subheader(f"📦 Productos Disponibles para {servicio_seleccionado}")
                            
                            # Crear formulario para la orden de compra
                            with st.form(key="form_nueva_orden"):
                                # Datos de cabecera de la orden
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    fecha_emision = st.date_input(
                                        "Fecha de Emisión (DD/MM/YYYY):",
                                        value=datetime.now(),
                                        key="input_fecha_emision",
                                        format="DD/MM/YYYY"
                                    )
                                
                                with col2:
                                    # Generar número de orden secuencial simple
                                    numero_sugerido = obtener_proximo_numero_oc(esquema_seleccionado)
                                    numero_oc = st.text_input(
                                        "N° Orden:",
                                        value=numero_sugerido,
                                        key="input_numero_oc",
                                        help="Número secuencial simple (ej: 1, 2, 3, ...)"
                                    )
                                
                                with col3:
                                    anio_oc = st.text_input(
                                        "Año:",
                                        value=str(datetime.now().year),
                                        key="input_anio_oc",
                                        help="Año de la orden (ej: 2025)"
                                    )
                                
                                st.markdown("---")
                                st.subheader("Selección de Items")
                                
                                # Crear tabla de productos con cantidades
                                items_seleccionados = []
                                
                                for idx, producto in enumerate(productos):
                                    lote, item, descripcion, cant_max, cant_emitida, precio, porc_ejecucion, unidad_medida = producto
                                    cant_disponible = cant_max - (cant_emitida or 0)
                                    
                                    # Convertir a enteros
                                    lote_int = int(float(lote)) if lote else 0
                                    item_int = int(float(item)) if item else 0
                                    
                                    with st.expander(f"LOTE {lote_int} - ITEM {item_int}: {descripcion[:60]}...", expanded=False):
                                        col1, col2, col3 = st.columns([2, 1, 1])
                                        
                                        with col1:
                                            st.write(f"**Descripción:** {descripcion}")
                                            st.write(f"**Precio Unitario:** ₲ {precio:,.0f}".replace(",", "."))
                                        
                                        with col2:
                                            st.write(f"**Cantidad Máxima:** {cant_max:,.0f}".replace(",", "."))
                                            st.write(f"**Cantidad Emitida:** {cant_emitida or 0:,.0f}".replace(",", "."))
                                            st.write(f"**Disponible:** {cant_disponible:,.0f}".replace(",", "."))
                                            st.write(f"**% Ejecución:** {porc_ejecucion or 0:.2f}%")
                                        
                                        with col3:
                                            cantidad = st.number_input(
                                                "Cantidad a solicitar:",
                                                min_value=0,
                                                max_value=int(cant_disponible),
                                                value=0,
                                                key=f"cant_item_{idx}",
                                                help="Ingresa 0 para no incluir este item"
                                            )
                                            
                                            if cantidad > 0:
                                                items_seleccionados.append({
                                                    'lote': lote,
                                                    'item': item,
                                                    'descripcion': descripcion,
                                                    'cantidad': cantidad,
                                                    'unidad_medida': unidad_medida if unidad_medida else 'UNIDAD',
                                                    'precio_unitario': precio,
                                                    'monto_total': cantidad * precio
                                                })
                                
                                # Botón de envío
                                st.markdown("---")
                                
                                if items_seleccionados:
                                    # Calcular monto total
                                    monto_total_orden = sum(item['monto_total'] for item in items_seleccionados)
                                    st.metric("Monto Total de la Orden", f"₲ {monto_total_orden:,.0f}".replace(",", "."))
                                
                                submit_button = st.form_submit_button("🚀 Emitir Orden de Compra")
                            
                            # FUERA DEL FORMULARIO - Procesar submit
                            if submit_button:
                                if not items_seleccionados:
                                    st.error("Debe ingresar cantidad mayor a 0 en al menos un item")
                                else:
                                    # Crear la orden de compra
                                    engine = get_engine()
                                    if engine is None:
                                        st.error("No se pudo conectar a la base de datos")
                                        return
                                    with engine.connect() as conn:
                                        try:
                                            # Iniciar transacción
                                            trans = conn.begin()
                                            
                                            # GUARDAR SOLO EN TABLA DEL ESQUEMA (NO en tabla central)
                                            # Si eliminas el esquema, las OC se eliminan automáticamente
                                            
                                            # Insertar en tabla orden_de_compra del esquema
                                            for item in items_seleccionados:
                                                insert_query = text(f"""
                                                    INSERT INTO "{esquema_seleccionado}"."orden_de_compra" (
                                                        "NUMERO_ORDEN_DE_COMPRA", 
                                                        "FECHA_DE_EMISION",
                                                        "SERVICIO_BENEFICIARIO",
                                                        "LOTE",
                                                        "ITEM",
                                                        "CANTIDAD_SOLICITADA",
                                                        "UNIDAD_DE_MEDIDA",
                                                        "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA",
                                                        "EMPRESA_ADJUDICADA",
                                                        "PRECIO_UNITARIO",
                                                        "I_D",
                                                        "MODALIDAD"
                                                    ) VALUES (
                                                        :numero_oc,
                                                        :fecha_emision,
                                                        :servicio,
                                                        :lote,
                                                        :item,
                                                        :cantidad,
                                                        :unidad_medida,
                                                        :descripcion,
                                                        (SELECT "EMPRESA_ADJUDICADA" FROM "{esquema_seleccionado}"."ejecucion_general" 
                                                         WHERE "LOTE" = :lote AND "ITEM" = :item AND "SERVICIO_BENEFICIARIO" = :servicio LIMIT 1),
                                                        :precio_unitario,
                                                        (SELECT "I_D" FROM "{esquema_seleccionado}"."ejecucion_general" 
                                                         WHERE "LOTE" = :lote AND "ITEM" = :item AND "SERVICIO_BENEFICIARIO" = :servicio LIMIT 1),
                                                        (SELECT "MODALIDAD" FROM "{esquema_seleccionado}"."ejecucion_general" 
                                                         WHERE "LOTE" = :lote AND "ITEM" = :item AND "SERVICIO_BENEFICIARIO" = :servicio LIMIT 1)
                                                    )
                                                """)
                                                
                                                conn.execute(insert_query, {
                                                    'numero_oc': numero_oc,
                                                    'fecha_emision': fecha_emision,
                                                    'servicio': servicio_seleccionado,
                                                    'lote': item['lote'],
                                                    'item': item['item'],
                                                    'cantidad': item['cantidad'],
                                                    'unidad_medida': item['unidad_medida'],
                                                    'descripcion': item['descripcion'],
                                                    'precio_unitario': item['precio_unitario']
                                                })
                                            
                                            # Actualizar cantidades emitidas en ejecucion_general
                                            for item in items_seleccionados:
                                                update_query = text(f"""
                                                    UPDATE "{esquema_seleccionado}"."ejecucion_general"
                                                    SET 
                                                        "CANTIDAD_EMITIDA" = COALESCE("CANTIDAD_EMITIDA", 0) + :cantidad,
                                                        "PORCENTAJE_EMITIDO" = CASE 
                                                            WHEN "CANTIDAD_MAXIMA" > 0 
                                                            THEN ((COALESCE("CANTIDAD_EMITIDA", 0) + :cantidad) / "CANTIDAD_MAXIMA") * 100
                                                            ELSE 0 
                                                        END
                                                    WHERE "SERVICIO_BENEFICIARIO" = :servicio
                                                      AND "LOTE" = :lote
                                                      AND "ITEM" = :item
                                                """)
                                                
                                                conn.execute(update_query, {
                                                    'cantidad': item['cantidad'],
                                                    'servicio': servicio_seleccionado,
                                                    'lote': item['lote'],
                                                    'item': item['item']
                                                })
                                            
                                            # Confirmar transacción
                                            trans.commit()
                                            
                                            st.success(f"✅ Orden de Compra {numero_oc}/{anio_oc} emitida exitosamente por {st.session_state.usuario_actual}!")
                                            
                                            # Generar PDF automáticamente
                                            pdf_bytes, error_pdf = generar_pdf_orden_compra(
                                                esquema_seleccionado,
                                                numero_oc,
                                                anio_oc,
                                                fecha_emision,
                                                servicio_seleccionado,
                                                items_seleccionados,
                                                st.session_state.usuario_actual
                                            )
                                            
                                            if pdf_bytes:
                                                st.download_button(
                                                    label="📥 Descargar PDF de Orden de Compra",
                                                    data=pdf_bytes,
                                                    file_name=f"OC_{numero_oc.replace('/', '_')}.pdf",
                                                    mime="application/pdf",
                                                    key="btn_download_pdf"
                                                )
                                            else:
                                                st.warning(f"Orden emitida pero error en PDF: {error_pdf}")
                                            
                                            st.balloons()
                                            time.sleep(3)
                                            
                                        except Exception as e:
                                            trans.rollback()
                                            st.error(f"Error al emitir orden de compra: {e}")
                        else:
                            st.warning(f"No hay productos disponibles para {servicio_seleccionado} en esta licitación.")
                    
                    except Exception as e:
                        st.error(f"Error obteniendo productos: {e}")
            else:
                st.warning(f"No hay servicios beneficiarios definidos para la licitación seleccionada.")
        else:
            st.info("Seleccione una licitación para emitir una orden de compra.")
    
    # ========================================
    # TAB 3: ACTAS DE RECEPCIÓN DEFINITIVA
    # ========================================
    with tab3:
        st.subheader("📋 Actas de Recepción Definitiva")
        
        # Selector de esquema
        st.markdown("### Seleccionar Licitación")
        esquema_acta = st.selectbox(
            "Licitación:",
            options=esquemas,
            key="selector_esquema_acta"
        )
        
        if esquema_acta:
            # Obtener info de licitación
            engine = get_engine()
            if engine is None:
                st.error("No se pudo conectar a la base de datos")
                return
            with engine.connect() as conn:
                try:
                    query = text(f"""
                        SELECT "NUMERO_DE_LLAMADO", "AÑO_DEL_LLAMADO", "NOMBRE_DEL_LLAMADO"
                        FROM "{esquema_acta}"."llamado"
                        LIMIT 1
                    """)
                    result = conn.execute(query)
                    licitacion = result.fetchone()
                    if licitacion:
                        st.info(f"📄 **LPN {licitacion[0]}/{licitacion[1]}** - {licitacion[2]}")
                except:
                    pass
            
            # Sub-tabs para las funciones
            sub_tab1, sub_tab2 = st.tabs(["📋 Historial de Actas", "➕ Nueva Acta"])
            
            # ========== SUB-TAB 1: HISTORIAL ==========
            with sub_tab1:
                st.markdown("### 📋 Historial de Actas")
                
                # Obtener todas las actas del esquema
                actas_todas = obtener_actas_orden(esquema_acta, None)  # None = todas
                
                if actas_todas:
                    # Agrupar por orden
                    from collections import defaultdict
                    actas_por_orden = defaultdict(list)
                    
                    for acta in actas_todas:
                        numero_orden = acta[3]
                        actas_por_orden[numero_orden].append(acta)
                    
                    # Mostrar por orden
                    for numero_orden, actas_orden in actas_por_orden.items():
                        with st.expander(f"🧾 Orden: {numero_orden} ({len(actas_orden)} actas)"):
                            for acta in actas_orden:
                                col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
                                
                                with col1:
                                    estado_emoji = "✅" if acta[4] == "ACTIVA" else "🔄"
                                    st.write(f"{estado_emoji} **Acta {acta[1]}**")
                                
                                with col2:
                                    st.write(f"📅 {acta[2].strftime('%d/%m/%Y')}")
                                
                                with col3:
                                    if st.button("📄 PDF", key=f"pdf_hist_{acta[0]}"):
                                        st.session_state[f'generar_pdf_{acta[0]}'] = True
                                        st.rerun()
                                
                                with col4:
                                    if st.button("✏️ Editar", key=f"edit_hist_{acta[0]}"):
                                        # Alternar abrir/cerrar como modificar orden
                                        current_state = st.session_state.get(f'mostrar_editar_acta_{acta[0]}', False)
                                        st.session_state[f'mostrar_editar_acta_{acta[0]}'] = not current_state
                                        st.rerun()
                                
                                # === PANEL DE EDICIÓN (CERRADO POR DEFECTO) ===
                                if st.session_state.get(f'mostrar_editar_acta_{acta[0]}', False):
                                    st.markdown("---")
                                    st.markdown("### ✏️ Modificar Acta")
                                    
                                    acta_completa = obtener_acta_por_id(acta[0])
                                    
                                    if acta_completa:
                                        import json
                                        
                                        # Si ya hay confirmación pendiente
                                        if f'confirmar_edit_acta_{acta[0]}' in st.session_state:
                                            st.warning("⚠️ **¿Está seguro de guardar estos cambios?**")
                                            
                                            col_c1, col_c2 = st.columns(2)
                                            with col_c1:
                                                if st.button("✅ SÍ, GUARDAR", type="primary", key=f"si_acta_{acta[0]}", use_container_width=True):
                                                    datos = st.session_state[f'confirmar_edit_acta_{acta[0]}']
                                                    
                                                    try:
                                                        exito = modificar_acta_recepcion(
                                                            acta[0],
                                                            datos['numero_acta'],
                                                            datos['fecha_rec'],
                                                            datos['fecha_rem'],
                                                            datos['fecha_fac'],
                                                            datos['num_rem'],
                                                            datos['num_fac'],
                                                            st.session_state.usuario_actual,
                                                            datos['obs'],
                                                            datos['items'],
                                                            st.session_state.usuario_actual
                                                        )
                                                        
                                                        if exito:
                                                            st.success("✅ Acta actualizada!")
                                                            del st.session_state[f'confirmar_edit_acta_{acta[0]}']
                                                            del st.session_state[f'mostrar_editar_acta_{acta[0]}']
                                                            time.sleep(1)
                                                            st.rerun()
                                                        else:
                                                            st.error("❌ Error actualizando")
                                                    except Exception as e:
                                                        st.error(f"❌ Error: {e}")
                                            
                                            with col_c2:
                                                if st.button("❌ NO, CANCELAR", key=f"no_acta_{acta[0]}", use_container_width=True):
                                                    del st.session_state[f'confirmar_edit_acta_{acta[0]}']
                                                    st.rerun()
                                        
                                        else:
                                            # Parsear items
                                            try:
                                                items_json = acta_completa[11]
                                                items_actuales = json.loads(items_json) if isinstance(items_json, str) else items_json
                                            except:
                                                items_actuales = []
                                            
                                            # Parsear remisiones y facturas
                                            num_remision = acta_completa[7]
                                            num_factura = acta_completa[8]
                                            
                                            if num_remision and isinstance(num_remision, str):
                                                remisiones_actuales = [r.strip() for r in num_remision.split(',') if r.strip()]
                                            else:
                                                remisiones_actuales = []
                                            
                                            if num_factura and isinstance(num_factura, str):
                                                facturas_actuales = [f.strip() for f in num_factura.split(',') if f.strip()]
                                            else:
                                                facturas_actuales = []
                                            
                                            # Inicializar contadores
                                            if f'num_rem_edit_{acta[0]}' not in st.session_state:
                                                st.session_state[f'num_rem_edit_{acta[0]}'] = max(len(remisiones_actuales), 1)
                                            if f'num_fac_edit_{acta[0]}' not in st.session_state:
                                                st.session_state[f'num_fac_edit_{acta[0]}'] = max(len(facturas_actuales), 1)
                                            
                                            # Mostrar datos actuales
                                            st.info(f"📋 **Acta:** {acta_completa[1]}")
                                            
                                            col_data1, col_data2 = st.columns(2)
                                            with col_data1:
                                                st.write(f"**Fecha Recepción:** {acta_completa[4].strftime('%d/%m/%Y') if isinstance(acta_completa[4], date) else acta_completa[4]}")
                                                st.write(f"**Remisiones:** {acta_completa[7] or 'N/A'}")
                                            with col_data2:
                                                st.write(f"**Orden:** {acta_completa[3]}")
                                                st.write(f"**Facturas:** {acta_completa[8] or 'N/A'}")
                                            
                                            # Formulario de edición
                                            with st.form(key=f"form_editar_acta_{acta[0]}"):
                                                st.markdown("**Editar datos:**")
                                                
                                                # N° Acta y Año
                                                col_acta1, col_acta2 = st.columns(2)
                                                with col_acta1:
                                                    numero_actual = acta_completa[1].split('/')[0] if '/' in acta_completa[1] else acta_completa[1]
                                                    numero_edit = st.text_input("N° Acta:", value=numero_actual, key=f"num_acta_edit_{acta[0]}")
                                                with col_acta2:
                                                    anio_actual = acta_completa[1].split('/')[1] if '/' in acta_completa[1] else str(date.today().year)
                                                    anio_edit = st.text_input("Año:", value=anio_actual, key=f"anio_acta_edit_{acta[0]}")
                                                
                                                fecha_rec_edit = st.date_input(
                                                    "Fecha de Recepción:",
                                                    value=acta_completa[4] if isinstance(acta_completa[4], date) else date.today()
                                                )
                                                
                                                st.markdown("**📋 Remisiones**")
                                                remisiones_edit = []
                                                fechas_remisiones_edit = []
                                                for i in range(st.session_state[f'num_rem_edit_{acta[0]}']):
                                                    col_r1, col_r2 = st.columns(2)
                                                    with col_r1:
                                                        valor = remisiones_actuales[i] if i < len(remisiones_actuales) else ""
                                                        num_rem = st.text_input(f"N° Remisión {i+1}:", value=valor, key=f"rem_edit_{acta[0]}_{i}")
                                                    with col_r2:
                                                        fecha_rem_input = st.date_input(f"Fecha {i+1}:", value=acta_completa[5] if isinstance(acta_completa[5], date) else date.today(), key=f"fecha_rem_edit_{acta[0]}_{i}")
                                                    
                                                    if num_rem:
                                                        remisiones_edit.append(num_rem.strip())
                                                        fechas_remisiones_edit.append(fecha_rem_input)
                                                
                                                st.markdown("**🧾 Facturas**")
                                                facturas_edit = []
                                                fechas_facturas_edit = []
                                                for i in range(st.session_state[f'num_fac_edit_{acta[0]}']):
                                                    col_f1, col_f2 = st.columns(2)
                                                    with col_f1:
                                                        valor = facturas_actuales[i] if i < len(facturas_actuales) else ""
                                                        num_fac = st.text_input(f"N° Factura {i+1}:", value=valor, key=f"fac_edit_{acta[0]}_{i}")
                                                    with col_f2:
                                                        fecha_fac_input = st.date_input(f"Fecha {i+1}:", value=acta_completa[6] if isinstance(acta_completa[6], date) else date.today(), key=f"fecha_fac_edit_{acta[0]}_{i}")
                                                    
                                                    if num_fac:
                                                        facturas_edit.append(num_fac.strip())
                                                        fechas_facturas_edit.append(fecha_fac_input)
                                                
                                                st.markdown("**📦 Items Recibidos:**")
                                                items_editados = []
                                                for idx, item in enumerate(items_actuales):
                                                    st.markdown(f"**Lote {item['lote']} - Ítem {item['item']}**")
                                                    st.caption(f"{item['descripcion']}")
                                                    cant = st.number_input(
                                                        "Cantidad Recepcionada:", 
                                                        min_value=0, 
                                                        value=int(item['cantidad']), 
                                                        step=1, 
                                                        key=f"cant_edit_{acta[0]}_{idx}"
                                                    )
                                                    
                                                    items_editados.append({
                                                        'lote': item['lote'],
                                                        'item': item['item'],
                                                        'descripcion': item['descripcion'],
                                                        'cantidad': cant,
                                                        'unidad_medida': item.get('unidad_medida', 'UNIDAD'),
                                                        'precio_unitario': item['precio_unitario']
                                                    })
                                                    st.markdown("---")
                                                
                                                obs_edit = st.text_area("Observaciones:", value=acta_completa[10] or "", height=80)
                                                
                                                confirmar = st.form_submit_button("📝 Confirmar Cambios", type="primary", use_container_width=True)
                                                
                                                if confirmar:
                                                    errores = []
                                                    if not remisiones_edit:
                                                        errores.append("Ingrese al menos un número de remisión")
                                                    if not facturas_edit:
                                                        errores.append("Ingrese al menos un número de factura")
                                                    
                                                    if errores:
                                                        for error in errores:
                                                            st.error(f"⚠️ {error}")
                                                    else:
                                                        numero_acta_actualizado = f"{numero_edit}/{anio_edit}"
                                                        st.session_state[f'confirmar_edit_acta_{acta[0]}'] = {
                                                            'numero_acta': numero_acta_actualizado,
                                                            'fecha_rec': fecha_rec_edit,
                                                            'fecha_rem': fechas_remisiones_edit[0] if fechas_remisiones_edit else date.today(),
                                                            'fecha_fac': fechas_facturas_edit[0] if fechas_facturas_edit else date.today(),
                                                            'num_rem': ", ".join(remisiones_edit),
                                                            'num_fac': ", ".join(facturas_edit),
                                                            'obs': obs_edit,
                                                            'items': items_editados
                                                        }
                                                        st.rerun()
                                            
                                            # Botones fuera del form
                                            col_b1, col_b2, col_b3, col_b4 = st.columns(4)
                                            with col_b1:
                                                if st.button("➕ Remisión", key=f"add_rem_{acta[0]}"):
                                                    st.session_state[f'num_rem_edit_{acta[0]}'] += 1
                                                    st.rerun()
                                            with col_b2:
                                                if st.button("➕ Factura", key=f"add_fac_{acta[0]}"):
                                                    st.session_state[f'num_fac_edit_{acta[0]}'] += 1
                                                    st.rerun()
                                            with col_b3:
                                                if st.button("📄 PDF Actualizado", key=f"pdf_edit_{acta[0]}"):
                                                    # Generar PDF con datos actuales
                                                    engine = get_engine()
                                                    if engine is None:
                                                        st.error("No se pudo conectar a la base de datos")
                                                        return
                                                    with engine.connect() as conn:
                                                        query_orden = text(f"""
                                                            SELECT "SERVICIO_BENEFICIARIO"
                                                            FROM "{esquema_acta}".orden_de_compra
                                                            WHERE "NUMERO_ORDEN_DE_COMPRA" = :numero
                                                            LIMIT 1
                                                        """)
                                                        result_orden = conn.execute(query_orden, {"numero": acta_completa[3]})
                                                        orden_data = result_orden.fetchone()
                                                        servicio = orden_data[0] if orden_data else "DGGIES"
                                                        
                                                        pdf_bytes = generar_pdf_acta_recepcion(
                                                            esquema_acta,
                                                            acta_completa[3],
                                                            acta_completa[4],
                                                            servicio,
                                                            "N/A",
                                                            items_actuales,
                                                            servicio,
                                                            acta_completa[7],
                                                            acta_completa[8],
                                                            acta_completa[9],
                                                            acta_completa[10],
                                                            st.session_state.usuario_actual,
                                                            fecha_remision=acta_completa[5],
                                                            fecha_factura=acta_completa[6],
                                                            numero_acta=acta_completa[1]
                                                        )
                                                        
                                                        if pdf_bytes:
                                                            st.download_button(
                                                                "💾 Descargar PDF",
                                                                data=pdf_bytes,
                                                                file_name=f"Acta_{acta_completa[1].replace('/', '-')}.pdf",
                                                                mime="application/pdf",
                                                                key=f"dl_pdf_edit_{acta[0]}"
                                                            )
                                            with col_b4:
                                                if st.button("🔙 Cerrar", key=f"cerrar_edit_{acta[0]}", use_container_width=True):
                                                    del st.session_state[f'mostrar_editar_acta_{acta[0]}']
                                                    if f'confirmar_edit_acta_{acta[0]}' in st.session_state:
                                                        del st.session_state[f'confirmar_edit_acta_{acta[0]}']
                                                    st.rerun()
                            
                            # Mostrar PDFs generados para esta orden
                            for acta in actas_orden:
                                if st.session_state.get(f'generar_pdf_{acta[0]}', False):
                                    acta_completa = obtener_acta_por_id(acta[0])
                                    
                                    if acta_completa:
                                        import json
                                        
                                        try:
                                            items_json = acta_completa[11]
                                            items_acta = json.loads(items_json) if isinstance(items_json, str) else items_json
                                        except:
                                            items_acta = []
                                        
                                        if items_acta:
                                            engine = get_engine()
                                            if engine is None:
                                                st.error("No se pudo conectar a la base de datos")
                                                return
                                            with engine.connect() as conn:
                                                query_orden = text(f"""
                                                    SELECT "SERVICIO_BENEFICIARIO"
                                                    FROM "{esquema_acta}".orden_de_compra
                                                    WHERE "NUMERO_ORDEN_DE_COMPRA" = :numero
                                                    LIMIT 1
                                                """)
                                                result_orden = conn.execute(query_orden, {"numero": acta_completa[3]})
                                                orden_data = result_orden.fetchone()
                                                servicio = orden_data[0] if orden_data else "DGGIES"
                                                
                                                pdf_bytes = generar_pdf_acta_recepcion(
                                                    esquema_acta,
                                                    acta_completa[3],
                                                    acta_completa[4],
                                                    servicio,
                                                    "N/A",
                                                    items_acta,
                                                    servicio,
                                                    acta_completa[7],
                                                    acta_completa[8],
                                                    acta_completa[9],
                                                    acta_completa[10],
                                                    st.session_state.usuario_actual,
                                                    fecha_remision=acta_completa[5],
                                                    fecha_factura=acta_completa[6],
                                                    numero_acta=acta_completa[1]
                                                )
                                                
                                                if pdf_bytes:
                                                    st.success(f"✅ PDF de Acta {acta_completa[1]} generado")
                                                    st.download_button(
                                                        label=f"💾 Descargar Acta {acta_completa[1]}",
                                                    data=pdf_bytes,
                                                    file_name=f"Acta_{acta_completa[1].replace('/', '-')}.pdf",
                                                    mime="application/pdf",
                                                    key=f"download_hist_{acta[0]}",
                                                    use_container_width=True
                                                )
                                                
                                                if st.button("🔙 Cerrar", key=f"cerrar_pdf_{acta[0]}"):
                                                    del st.session_state[f'generar_pdf_{acta[0]}']
                                                    st.rerun()
                    
                else:
                    st.info("📭 No hay actas registradas en esta licitación")
            
            # ========== SUB-TAB 2: NUEVA ACTA ==========
            with sub_tab2:
                st.markdown("### ➕ Generar Nueva Acta")
                
                # Selector de orden
                engine = get_engine()
                if engine is None:
                    st.error("No se pudo conectar a la base de datos")
                    return
                with engine.connect() as conn:
                    query_ordenes = text(f"""
                        SELECT DISTINCT "NUMERO_ORDEN_DE_COMPRA"
                        FROM "{esquema_acta}".orden_de_compra
                        ORDER BY "NUMERO_ORDEN_DE_COMPRA" DESC
                    """)
                    result = conn.execute(query_ordenes)
                    ordenes_disponibles = [row[0] for row in result.fetchall()]
                
                if ordenes_disponibles:
                    orden_seleccionada = st.selectbox(
                        "Seleccionar Orden de Compra:",
                        options=ordenes_disponibles,
                        key="selector_orden_acta"
                    )
                    
                    if orden_seleccionada:
                        # Obtener items de la orden
                        engine = get_engine()
                        if engine is None:
                            st.error("No se pudo conectar a la base de datos")
                            return
                        with engine.connect() as conn:
                            query_items = text(f"""
                                SELECT "LOTE", "ITEM", "DESCRIPCION_DEL_PRODUCTO_MARCA_PROCEDENCIA", 
                                       "CANTIDAD_SOLICITADA", "UNIDAD_DE_MEDIDA", "PRECIO_UNITARIO", 
                                       "SERVICIO_BENEFICIARIO"
                                FROM "{esquema_acta}".orden_de_compra
                                WHERE "NUMERO_ORDEN_DE_COMPRA" = :numero
                            """)
                            result = conn.execute(query_items, {"numero": orden_seleccionada})
                            items_orden = result.fetchall()
                        
                        if items_orden:
                            st.success(f"✅ Orden {orden_seleccionada} - {len(items_orden)} ítems")
                            
                            # FORMULARIO
                            with st.form(key="form_nueva_acta"):
                                st.markdown("#### 📝 Datos del Acta")
                                
                                # Generar número automático
                                numero_acta_sugerido = generar_numero_acta()
                                if numero_acta_sugerido:
                                    partes = numero_acta_sugerido.split('/')
                                    sugerido_num = partes[0]
                                    sugerido_anio = partes[1]
                                else:
                                    sugerido_num = "001"
                                    sugerido_anio = str(datetime.now().year)
                                
                                col_n1, col_n2 = st.columns(2)
                                with col_n1:
                                    numero_acta_num = st.text_input("N° Acta:", value=sugerido_num, max_chars=3)
                                with col_n2:
                                    anio_acta = st.text_input("Año:", value=sugerido_anio, max_chars=4)
                                
                                fecha_recepcion = st.date_input("Fecha de Recepción:", value=date.today())
                                
                                st.text_input(
                                    "Usuario Emisor:",
                                    value=st.session_state.usuario_actual,
                                    disabled=True
                                )
                                
                                st.markdown("---")
                                st.markdown("#### 📦 Items a Recibir")
                                st.info("💡 Indique la cantidad que está recibiendo de cada ítem")
                                
                                items_recibidos = []
                                for idx, item in enumerate(items_orden):
                                    lote, item_num, desc, cant, unidad, precio, servicio = item
                                    
                                    st.markdown(f"**Lote {int(float(lote))} - Ítem {int(float(item_num))}**")
                                    st.caption(f"{desc[:80]}...")
                                    
                                    col1, col2, col3 = st.columns([2, 2, 1])
                                    
                                    with col1:
                                        st.metric("En Orden", f"{int(cant):,}".replace(",", "."))
                                    
                                    with col2:
                                        cant_recibir = st.number_input(
                                            "Cantidad a Recibir:",
                                            min_value=0,
                                            max_value=int(cant),
                                            value=int(cant),
                                            step=1,
                                            key=f"cant_nueva_{idx}"
                                        )
                                    
                                    with col3:
                                        incluir = st.checkbox("Incluir", value=True, key=f"inc_nueva_{idx}")
                                    
                                    if incluir and cant_recibir > 0:
                                        items_recibidos.append({
                                            'lote': int(float(lote)),
                                            'item': int(float(item_num)),
                                            'descripcion': desc,
                                            'cantidad': cant_recibir,
                                            'unidad_medida': unidad if unidad else 'UNIDAD',
                                            'precio_unitario': float(precio)
                                        })
                                
                                st.markdown("---")
                                st.markdown("#### 📄 Documentos")
                                
                                # REMISIONES - Inicializar contador
                                if 'num_remisiones_acta' not in st.session_state:
                                    st.session_state.num_remisiones_acta = 1
                                
                                st.markdown("**📋 Remisiones**")
                                remisiones_data = []
                                for i in range(st.session_state.num_remisiones_acta):
                                    st.markdown(f"*Remisión {i+1}:*")
                                    col_r1, col_r2 = st.columns(2)
                                    with col_r1:
                                        num_rem = st.text_input(f"N° Remisión {i+1}:", key=f"num_rem_nueva_{i}")
                                    with col_r2:
                                        fecha_rem = st.date_input(f"Fecha {i+1}:", value=date.today(), key=f"fecha_rem_nueva_{i}")
                                    
                                    if num_rem:  # Solo agregar si tiene número
                                        remisiones_data.append({'numero': num_rem, 'fecha': fecha_rem})
                                
                                # FACTURAS - Inicializar contador
                                if 'num_facturas_acta' not in st.session_state:
                                    st.session_state.num_facturas_acta = 1
                                
                                st.markdown("**🧾 Facturas**")
                                facturas_data = []
                                for i in range(st.session_state.num_facturas_acta):
                                    st.markdown(f"*Factura {i+1}:*")
                                    col_f1, col_f2 = st.columns(2)
                                    with col_f1:
                                        num_fac = st.text_input(f"N° Factura {i+1}:", key=f"num_fac_nueva_{i}")
                                    with col_f2:
                                        fecha_fac = st.date_input(f"Fecha {i+1}:", value=date.today(), key=f"fecha_fac_nueva_{i}")
                                    
                                    if num_fac:  # Solo agregar si tiene número
                                        facturas_data.append({'numero': num_fac, 'fecha': fecha_fac})
                                
                                observaciones = st.text_area("Observaciones:", height=100)
                                
                                # Botón generar
                                generar = st.form_submit_button("📄 Generar Acta PDF", type="primary", use_container_width=True)
                                
                                if generar:
                                    if not items_recibidos:
                                        st.error("⚠️ Debe seleccionar al menos un ítem")
                                    elif not remisiones_data:
                                        st.error("⚠️ Ingrese al menos un número de remisión")
                                    elif not facturas_data:
                                        st.error("⚠️ Ingrese al menos un número de factura")
                                    else:
                                        try:
                                            numero_acta_completo = f"{numero_acta_num}/{anio_acta}"
                                            
                                            # Consolidar remisiones y facturas
                                            numero_remision = ", ".join([r['numero'] for r in remisiones_data])
                                            numero_factura = ", ".join([f['numero'] for f in facturas_data])
                                            fecha_remision = remisiones_data[0]['fecha']
                                            fecha_factura = facturas_data[0]['fecha']
                                            
                                            # Generar PDF
                                            pdf_bytes = generar_pdf_acta_recepcion(
                                                esquema_acta,
                                                orden_seleccionada,
                                                fecha_recepcion,
                                                items_orden[0][6],  # servicio
                                                "N/A",
                                                items_recibidos,
                                                items_orden[0][6],
                                                numero_remision,
                                                numero_factura,
                                                st.session_state.usuario_actual,
                                                observaciones,
                                                st.session_state.usuario_actual,
                                                fecha_remision=fecha_remision,
                                                fecha_factura=fecha_factura,
                                                numero_acta=numero_acta_completo
                                            )
                                            
                                            if pdf_bytes:
                                                # Guardar en BD
                                                acta_id = guardar_acta_recepcion(
                                                    esquema_acta,
                                                    orden_seleccionada,
                                                    numero_acta_completo,
                                                    fecha_recepcion,
                                                    fecha_remision,
                                                    fecha_factura,
                                                    numero_remision,
                                                    numero_factura,
                                                    st.session_state.usuario_actual,
                                                    observaciones,
                                                    items_recibidos,
                                                    st.session_state.usuario_actual
                                                )
                                                
                                                if acta_id:
                                                    # Guardar PDF en session_state para mostrarlo fuera del form
                                                    st.session_state['pdf_acta_generada'] = {
                                                        'pdf_bytes': pdf_bytes,
                                                        'numero_acta': numero_acta_completo
                                                    }
                                                    st.success(f"✅ Acta {numero_acta_completo} generada exitosamente!")
                                                    st.balloons()
                                                else:
                                                    st.error("❌ Error guardando acta en BD")
                                            else:
                                                st.error("❌ Error generando PDF")
                                        
                                        except Exception as e:
                                            st.error(f"❌ Error: {e}")
                                            import traceback
                                            st.error(traceback.format_exc())
                            
                            # FUERA DEL FORM - Mostrar botón de descarga si hay PDF generado
                            if 'pdf_acta_generada' in st.session_state:
                                pdf_data = st.session_state['pdf_acta_generada']
                                
                                st.success(f"🎉 ¡Acta {pdf_data['numero_acta']} lista para descargar!")
                                st.info("👇 Haz clic en el botón verde para descargar el PDF")
                                
                                st.download_button(
                                    label="💾 DESCARGAR ACTA PDF",
                                    data=pdf_data['pdf_bytes'],
                                    file_name=f"Acta_{pdf_data['numero_acta'].replace('/', '-')}.pdf",
                                    mime="application/pdf",
                                    use_container_width=True,
                                    type="primary",
                                    key=f"download_acta_{pdf_data['numero_acta']}"
                                )
                                
                                st.markdown("---")
                                
                                # Botón para limpiar y generar otra acta
                                if st.button("📄 Generar Nueva Acta", use_container_width=True):
                                    del st.session_state['pdf_acta_generada']
                                    st.session_state.num_remisiones_acta = 1
                                    st.session_state.num_facturas_acta = 1
                                    st.rerun()
                            
                            # Botones fuera del form para agregar más remisiones/facturas
                            col_btn1, col_btn2 = st.columns(2)
                            with col_btn1:
                                if st.button("➕ Agregar Remisión", key="add_rem"):
                                    st.session_state.num_remisiones_acta += 1
                                    st.rerun()
                            with col_btn2:
                                if st.button("➕ Agregar Factura", key="add_fac"):
                                    st.session_state.num_facturas_acta += 1
                                    st.rerun()
                        else:
                            st.warning("No hay ítems en esta orden")
                else:
                    st.warning("No hay órdenes de compra disponibles en esta licitación")

# =============================================================================
# FUNCIONES PARA GESTIÓN DE SERVICIOS POR USUARIO
# =============================================================================

def crear_tabla_usuario_servicio():
    """Crea la tabla de relación usuario-servicio si no existe"""
    try:
        engine = get_engine()
        if engine is None:
            return False
        if isinstance(engine, dict) and engine.get('type') == 'api_rest':
            # Para API REST, las tablas se crean manualmente en Supabase
            return True
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oxigeno.usuario_servicio (
                    id SERIAL PRIMARY KEY,
                    usuario_id INTEGER NOT NULL,
                    servicio VARCHAR(200) NOT NULL,
                    puede_crear_oc BOOLEAN DEFAULT TRUE,
                    puede_crear_acta BOOLEAN DEFAULT TRUE,
                    fecha_asignacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (usuario_id) REFERENCES oxigeno.usuarios(id) ON DELETE CASCADE,
                    UNIQUE(usuario_id, servicio)
                );
            """))
            conn.commit()
            return True
    except Exception as e:
        print(f"Error creando tabla usuario_servicio: {e}")
        return False

def obtener_servicios_disponibles():
    """Obtiene lista de servicios únicos de todas las licitaciones"""
    servicios = set()
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            esquemas = obtener_esquemas_postgres()
            for esquema in esquemas:
                try:
                    query = text(f"""
                        SELECT DISTINCT "SERVICIO_BENEFICIARIO"
                        FROM "{esquema}".ejecucion_general
                        WHERE "SERVICIO_BENEFICIARIO" IS NOT NULL
                        AND "SERVICIO_BENEFICIARIO" != ''
                    """)
                    result = conn.execute(query)
                    for row in result:
                        servicios.add(row[0])
                except:
                    continue
        return sorted(list(servicios))
    except:
        return []

def asignar_servicio_usuario(usuario_id, servicio, puede_oc=True, puede_acta=True):
    """Asigna un servicio a un usuario con permisos específicos"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text("""
                INSERT INTO oxigeno.usuario_servicio 
                (usuario_id, servicio, puede_crear_oc, puede_crear_acta)
                VALUES (:usuario_id, :servicio, :puede_oc, :puede_acta)
                ON CONFLICT (usuario_id, servicio) 
                DO UPDATE SET 
                    puede_crear_oc = :puede_oc,
                    puede_crear_acta = :puede_acta
            """)
            conn.execute(query, {
                'usuario_id': usuario_id,
                'servicio': servicio,
                'puede_oc': puede_oc,
                'puede_acta': puede_acta
            })
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error asignando servicio: {e}")
        return False

def obtener_servicios_usuario(usuario_id):
    """Obtiene los servicios asignados a un usuario"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text("""
                SELECT servicio, puede_crear_oc, puede_crear_acta
                FROM oxigeno.usuario_servicio
                WHERE usuario_id = :usuario_id
                ORDER BY servicio
            """)
            result = conn.execute(query, {'usuario_id': usuario_id})
            servicios = []
            for row in result:
                servicios.append({
                    'servicio': row[0],
                    'puede_crear_oc': row[1],
                    'puede_crear_acta': row[2]
                })
            return servicios
    except Exception as e:
        return []

def quitar_servicio_usuario(usuario_id, servicio):
    """Elimina la asignación de un servicio a un usuario"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("No se pudo conectar a la base de datos")
            return
        with engine.connect() as conn:
            query = text("""
                DELETE FROM oxigeno.usuario_servicio
                WHERE usuario_id = :usuario_id AND servicio = :servicio
            """)
            conn.execute(query, {
                'usuario_id': usuario_id,
                'servicio': servicio
            })
            conn.commit()
            return True
    except Exception as e:
        return False

def get_servicios_permitidos_usuario():
    """Obtiene lista de servicios que el usuario actual puede ver"""
    if st.session_state.user_role == 'admin':
        return obtener_servicios_disponibles()
    else:
        servicios = obtener_servicios_usuario(st.session_state.user_id)
        return [s['servicio'] for s in servicios]

if __name__ == "__main__":
    main()