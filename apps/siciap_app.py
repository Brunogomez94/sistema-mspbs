import streamlit as st
import pandas as pd
import numpy as np
try:
    import psycopg2
except ImportError:
    # psycopg2-binary se importa como psycopg2
    try:
        import psycopg2_binary as psycopg2
    except ImportError:
        psycopg2 = None
import os
import re
import logging
import sys
from datetime import datetime, timedelta
from io import BytesIO
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
import traceback
import time

st.set_page_config(
    page_title="SICIAP Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)
# PASO 1: Agregar esta función al INICIO del archivo, después de todos los imports
# Busca la línea donde terminen los imports (después de "import time") y agrega esto:

def safe_date_conversion(date_series, format_hint=None):
    """
    Convierte fechas de forma segura sin warnings, con soporte mejorado para formatos españoles
    """
    if date_series is None or (isinstance(date_series, pd.Series) and len(date_series) == 0):
        return date_series

    # Suprimir warnings temporalmente para esta función
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
        # Si date_series es una serie, procesar toda la serie
        if isinstance(date_series, pd.Series):
            # Limpiar valores problemáticos
            date_series = date_series.astype(str).replace(['nan', 'NaT', 'None', '', 'NaN'], None)
            
            # Métodos por orden de preferencia
            methods = [
                # 1. Intentar con dayfirst=True para formato español DD/MM/YYYY
                lambda: pd.to_datetime(date_series, dayfirst=True, errors='coerce'),
                # 2. Formato específico DD/MM/YYYY
                lambda: pd.to_datetime(date_series, format='%d/%m/%Y', errors='coerce'),
                # 3. Formato específico DD-MM-YYYY
                lambda: pd.to_datetime(date_series, format='%d-%m-%Y', errors='coerce'),
                # 4. Formato con horas DD/MM/YYYY HH:MM:SS
                lambda: pd.to_datetime(date_series, format='%d/%m/%Y %H:%M:%S', errors='coerce'),
                # 5. Último recurso: pd.to_datetime genérico
                lambda: pd.to_datetime(date_series, errors='coerce')
            ]
            
            # Intentar cada método
            for method in methods:
                try:
                    result = method()
                    # Si tenemos al menos una fecha válida, usar este método
                    if not result.isna().all():
                        return result
                except Exception:
                    continue
            
            # Si todo falla, usar el método más genérico
            return pd.to_datetime(date_series, errors='coerce')
        else:
            # Si es un valor único, convertirlo a string y procesarlo
            if pd.isna(date_series) or date_series is None:
                return None
            
            # Comprobar si contiene "CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES"
            date_str = str(date_series)
            if "CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES" in date_str.upper() or "cumplimiento total de las obligaciones" in date_str.lower():
                # Mantener la cadena original para este caso especial
                return date_str
                
            if date_str.strip() in ['nan', 'NaT', 'None', '', 'NaN']:
                return None
                
            try:
                return pd.to_datetime(date_str, dayfirst=True)
            except Exception:
                try:
                    return pd.to_datetime(date_str, format='%d/%m/%Y')
                except Exception:
                    try:
                        return pd.to_datetime(date_str, format='%d-%m-%Y')
                    except Exception:
                        try:
                            return pd.to_datetime(date_str)
                        except Exception:
                            return None

def format_numeric_value(value, is_percentage=False, use_currency=False):
    """
    Formatea valores numéricos de forma consistente: 
    - Elimina decimales innecesarios (.0, .00, etc.)
    - Usa puntos como separadores de miles si es necesario
    - Formatea porcentajes correctamente
    - Opcionalmente añade símbolo de moneda ($)
    """
    if pd.isna(value) or value is None:
        return "-"
        
    try:
        # Convertir a float para asegurar que es un número
        num_value = float(value)
        
        # Comprobar si es un número entero
        is_integer = num_value == int(num_value)
        
        if is_percentage:
            # Para porcentajes, mostrar 1 decimal si es necesario
            if is_integer:
                return f"{int(num_value)}%"
            else:
                return f"{num_value:.1f}%"
        else:
            # Para valores normales
            if is_integer:
                # Si es entero grande, usar separador de miles
                int_value = int(num_value)
                if abs(num_value) >= 1000:
                    formatted = f"{int_value:,}".replace(',', '.')
                else:
                    formatted = f"{int_value}"
            else:
                # Si tiene decimales, mostrar hasta 2 decimales y eliminar ceros finales
                formatted = f"{num_value:.2f}".rstrip('0').rstrip('.')
                
                # Si es un valor grande, usar separador de miles
                if abs(num_value) >= 1000:
                    # Formatear la parte entera con separador de miles
                    parts = formatted.split('.')
                    parts[0] = f"{int(parts[0]):,}".replace(',', '.')
                    # Unir de nuevo con el decimal si existe
                    if len(parts) > 1:
                        formatted = f"{parts[0]}.{parts[1]}"
                    else:
                        formatted = parts[0]
            
            # Añadir símbolo de moneda si se solicita
            if use_currency:
                return f"${formatted}"
            else:
                return formatted
    except Exception:
        # Si hay error, devolver el valor original como string
        return str(value)

# Configurar logging para todas las funcionalidades
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("siciap.log", encoding='utf-8'),
        logging.StreamHandler(stream=sys.stdout)  # Salida a consola
    ]
)

# Crear un logger específico para este módulo
logger = logging.getLogger(__name__)

def sincronizar_datosejecucion(conn):
    """
    Sincroniza datosejecucion con ejecucion después de cargar datos nuevos
    Preserva información crítica existente y agrega nuevos registros
    """
    try:
        logger.info("Iniciando sincronización de datosejecucion...")
        
        with conn.conn.cursor() as cursor:
            # 1. Obtener id_llamados que existen en ejecucion pero no en datosejecucion
            cursor.execute("""
                INSERT INTO siciap.datosejecucion (id_llamado, licitacion)
                SELECT DISTINCT e.id_llamado, e.licitacion
                FROM siciap.ejecucion e
                WHERE NOT EXISTS (
                    SELECT 1 FROM siciap.datosejecucion d 
                    WHERE d.id_llamado = e.id_llamado
                )
                AND e.id_llamado IS NOT NULL
            """)
            
            nuevos_registros = cursor.rowcount
            conn.conn.commit()
            
            logger.info(f"Sincronización completada: {nuevos_registros} nuevos registros agregados")
            return True
            
    except Exception as e:
        logger.error(f"Error en sincronización: {str(e)}")
        conn.conn.rollback()
        return False

# Tablas predefinidas - Ahora definidas a nivel global
TABLES = {
    'ordenes': 'siciap.ordenes',
    'ejecucion': 'siciap.ejecucion',
    'stock': 'siciap.stock_critico',
    'pedidos': 'siciap.pedidos'
}

# Función para obtener la configuración de la BD desde secrets o variables de entorno
def get_db_config():
    """Obtiene configuración de BD desde secrets o variables de entorno"""
    try:
        if hasattr(st, 'secrets') and 'db_config' in st.secrets:
            return {
                'host': st.secrets['db_config']['host'],
                'port': int(st.secrets['db_config']['port']),
                'dbname': st.secrets['db_config']['dbname'],
                'user': st.secrets['db_config']['user'],
                'password': st.secrets['db_config']['password']
            }
    except Exception:
        pass
    
    # Fallback a variables de entorno o valores por defecto
    import os
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'dbname': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'Dggies12345')
    }

# Clase para manejar la conexión a PostgreSQL
class PostgresConnection:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        self.engine = None

    def connect(self):
        try:
            # Conexión con psycopg2
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.conn.autocommit = True  # Para crear esquemas
            
            # Conexión con SQLAlchemy para pandas to_sql
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            self.engine = create_engine(connection_string)
            
            logger.info("Conexión a PostgreSQL establecida correctamente")
            return True
        except Exception as e:
            logger.error(f"Error al conectar a PostgreSQL: {str(e)}")
            return False
    
    def test_connection(self):
        if self.conn is None:
            return False
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
        except Exception as e:
            logger.error(f"Error al probar la conexión: {str(e)}")
            return False

    def close(self):
        if self.conn is not None:
            self.conn.close()
        if self.engine is not None:
            self.engine.dispose()
        logger.info("Conexión a PostgreSQL cerrada")

    def get_cursor(self):
        """Devuelve un cursor de la conexión actual"""
        if self.conn is None:
            self.connect()
        return self.conn.cursor()

    def execute_sql(self, sql_query, params=None):
        """Ejecuta una consulta SQL directamente"""
        if self.conn is None:
            self.connect()
        with self.conn.cursor() as cursor:
            if params:
                cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)
            self.conn.commit()
            return True

# Funciones generales para limpiar y normalizar datos
def clean_column_name(col_name):
    """Limpia y normaliza nombres de columnas para PostgreSQL"""
    if not col_name:
        return "column"

    # Si no es string, convertir a string
    if not isinstance(col_name, str):
        col_name = str(col_name)

    # Reemplazar caracteres acentuados
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'ñ': 'n', 'Ñ': 'N',
        'ü': 'u', 'Ü': 'U'
    }
    
    for orig, repl in replacements.items():
        col_name = col_name.replace(orig, repl)

    # Reemplazar caracteres no alfanuméricos con guiones bajos
    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', str(col_name).strip())
    
    # Eliminar guiones bajos múltiples
    clean_name = re.sub(r'_+', '_', clean_name)
    
    # Eliminar guiones bajos al inicio o final
    clean_name = clean_name.strip('_')
    
    # Convertir a minúsculas
    clean_name = clean_name.lower()
    
    # Si el nombre comienza con un número, añadir 'col_' al inicio
    if re.match(r'^\d', clean_name):
        clean_name = 'col_' + clean_name

    # Si el nombre está vacío después de la limpieza, usar 'column'
    if not clean_name:
        clean_name = 'column'

    # Nombres reservados en PostgreSQL
    reserved_words = ['user', 'order', 'table', 'column', 'group', 'by', 'select', 'from', 'where', 'having']
    if clean_name.lower() in reserved_words:
        clean_name = 'x_' + clean_name

    return clean_name

def sanitize_text_for_postgres(text):
    """Sanitiza texto para PostgreSQL."""
    if text is None:
        return None

    # Convertir a string si no lo es ya
    if not isinstance(text, str):
        text = str(text)
    
    # Eliminar caracteres nulos que pueden causar problemas
    text = text.replace('\x00', '')
    
    # Escapar comillas simples duplicándolas (estándar SQL)
    text = text.replace("'", "''")
    
    # Truncar textos demasiado largos para evitar errores
    max_length = 990  # Un poco menos que VARCHAR(1000)
    if len(text) > max_length:
        text = text[:max_length]
    
    return text

def format_date_str(date_str):
    """
    Formatea una fecha como string para que PostgreSQL la entienda,
    pero mantiene el formato original visible.
    """
    # Si es una Serie, devolver None para evitar errores
    if isinstance(date_str, pd.Series):
        return None
    
    if pd.isna(date_str) or date_str is None:
        return None
    
    try:
        # Si es una fecha de pandas/numpy, convertirla a string con formato ISO
        if isinstance(date_str, (pd.Timestamp, np.datetime64)):
            return date_str.strftime('%Y-%m-%d')
        
        # Si ya es string, intentar normalizar el formato
        date_str = str(date_str).strip()
        
        # Ignorar strings vacíos
        if not date_str or date_str.lower() == 'nan':
            return None
        
        # Detectar formatos comunes
        if re.match(r'^\d{1,2}/\d{1,2}/\d{2,4}$', date_str):
            # Formato dd/mm/yyyy o mm/dd/yyyy
            parts = date_str.split('/')
            if len(parts[2]) == 2:  # Año con 2 dígitos
                parts[2] = '20' + parts[2] if int(parts[2]) < 50 else '19' + parts[2]
            return date_str  # Mantener formato original
        
        elif re.match(r'^\d{1,2}-\d{1,2}-\d{2,4}$', date_str):
            # Formato dd-mm-yyyy o mm-dd-yyyy
            return date_str  # Mantener formato original
        
        elif re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', date_str):
            # Formato ISO yyyy-mm-dd
            return date_str  # Ya está en formato ISO
        elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', date_str):
            # Formato dd/mm/yyyy hh:mm:ss
            return date_str  # Mantener formato original
        
        # Intentar convertir con pandas si no coincide con ningún formato conocido
        try:
            parsed_date = pd.to_datetime(date_str, errors='raise')
            return parsed_date.strftime('%Y-%m-%d')
        except:
            # Si hay error en la conversión, devolver el string original
            return date_str
    
    except Exception as e:
        logger.warning(f"Error al formatear fecha '{date_str}': {str(e)}")
        # Si hay algún error, devolver None para evitar errores
        return None

def safe_to_numeric(value, default=None):
    """Convierte de forma segura un valor a numérico, manejando errores"""
    if pd.isna(value):
        return default
    
    try:
        # Intentar convertir a float
        return float(value)
    except (ValueError, TypeError):
        # Si es string, intentar limpiar y convertir
        if isinstance(value, str):
            # Eliminar caracteres no numéricos excepto punto y signos
            clean_value = re.sub(r'[^\d\-\+\.]', '', value)
            try:
                return float(clean_value)
            except (ValueError, TypeError):
                return default
        return default

def handle_null_value(value):
    """Maneja valores nulos o vacíos de forma consistente"""
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() in ('', 'NULL', 'null', 'None', 'none', 'NaN', 'nan', 'NA', 'na'):
        return None
    return value

# PROBLEMAS COMUNES CON EXCEL DESCARGADOS Y SOLUCIONES

# 1. MEJORAR LA FUNCIÓN read_excel_robust para manejar archivos problemáticos
def read_excel_robust(file_content, file_name):
    """Intenta leer un archivo Excel con múltiples métodos - VERSIÓN MEJORADA"""
    logger.info(f"Intentando leer el archivo {file_name} con métodos robustos...")
    
    # Asegurar que file_content es un BytesIO
    if isinstance(file_content, BytesIO):
        buffer = file_content
        buffer.seek(0)
    else:
        try:
            buffer = BytesIO(file_content)
        except Exception as e:
            logger.error(f"Error al convertir contenido a BytesIO: {str(e)}")
            return None
    
    # Métodos priorizados para archivos Excel
    methods = [
        # Método 1: openpyxl con data_only=True (mejor para fórmulas)
        lambda: pd.read_excel(buffer, engine='openpyxl', engine_kwargs={'data_only': True}),
        # Método 2: pandas estándar (más simple)
        lambda: pd.read_excel(buffer),
        # Método 3: openpyxl normal
        lambda: pd.read_excel(buffer, engine='openpyxl'),
        # Método 4: Intentar con xlrd para .xls
        lambda: pd.read_excel(buffer, engine='xlrd') if file_name.lower().endswith('.xls') else None,
        # Método 5: Ignorar filas vacías al inicio
        lambda: pd.read_excel(buffer, skiprows=range(1, 10)),
        # Método 6: Buscar hojas con datos
        lambda: find_sheet_with_data(buffer),
        # Método 7: CSV fallback
        lambda: try_read_as_csv(buffer, file_name)
    ]

    # Intentar cada método
    for i, method in enumerate(methods):
        try:
            logger.info(f"Intentando método {i+1} para leer Excel...")
            buffer.seek(0)  # Resetear posición del buffer
            df = method()
            
            if df is None or df.empty:
                logger.warning(f"El método {i+1} devolvió un DataFrame vacío")
                continue
            
            # Validar si hay datos útiles
            non_null_data = df.dropna(how='all')
            if non_null_data.shape[0] == 0:
                logger.warning(f"El método {i+1} devolvió solo headers sin datos")
                continue

            # Limpieza básica del DataFrame exitoso
            df = clean_downloaded_excel(df)
            
            logger.info(f"ÉXITO con método {i+1}. Encontradas {len(df.columns)} columnas y {df.shape[0]} filas.")
            return df
        
        except Exception as e:
            logger.warning(f"Error con método {i+1}: {str(e)}")
            buffer.seek(0)  # Resetear posición del buffer para siguiente intento

    # Si todos los métodos fallan
    logger.error(f"TODOS LOS MÉTODOS FALLARON para leer el archivo Excel {file_name}")
    return None

def try_read_as_csv(file_content, file_name):
    """Intenta leer como CSV si el Excel está mal formateado"""
    try:
        if hasattr(file_content, 'seek'):
            file_content.seek(0)

        # Leer como texto y verificar si parece CSV
        content_bytes = file_content.read()
        # Detectar encoding
        import chardet
        encoding_result = chardet.detect(content_bytes)
        encoding = encoding_result['encoding'] if encoding_result['confidence'] > 0.7 else 'utf-8'
        
        try:
            content_text = content_bytes.decode(encoding)
        except:
            content_text = content_bytes.decode('utf-8', errors='ignore')
        
        # Si tiene separadores CSV comunes, intentar leer como CSV
        if ',' in content_text or ';' in content_text or '\t' in content_text:
            logger.info("Archivo parece ser CSV disfrazado de Excel, intentando lectura CSV...")
            
            import io
            # Probar diferentes separadores
            for sep in [',', ';', '\t', '|']:
                try:
                    df = pd.read_csv(io.StringIO(content_text), sep=sep, encoding=encoding)
                    if df.shape[1] > 1:  # Si tiene múltiples columnas, probablemente sea correcto
                        return df
                except:
                    continue
        
        return None
    except Exception as e:
        logger.warning(f"Error al intentar leer como CSV: {str(e)}")
        return None

def find_sheet_with_data(file_content):
    """Encuentra la hoja con datos reales en un archivo multi-sheet"""
    try:
        if hasattr(file_content, 'seek'):
            file_content.seek(0)

        # Leer todas las hojas
        all_sheets = pd.read_excel(file_content, sheet_name=None, engine='openpyxl')
        for sheet_name, df in all_sheets.items():
            # Buscar hoja con datos reales
            if not df.empty and df.dropna(how='all').shape[0] > 0:
                logger.info(f"Encontrados datos en la hoja: {sheet_name}")
                return df
        
        return None
    except Exception as e:
        logger.warning(f"Error al buscar hojas con datos: {str(e)}")
        return None

def clean_downloaded_excel(df):
    """Limpia DataFrames de archivos Excel descargados que suelen tener problemas"""
    try:
        # 1. Eliminar filas completamente vacías
        df = df.dropna(how='all')
        
        # 2. Eliminar columnas completamente vacías
        df = df.dropna(axis=1, how='all')
        
        # 3. Limpiar nombres de columnas problemáticos
        new_columns = []
        for col in df.columns:
            if pd.isna(col) or str(col).strip() == '':
                # Generar nombre para columna sin nombre
                new_columns.append(f"columna_{len(new_columns) + 1}")
            else:
                # Limpiar el nombre de la columna
                clean_col = str(col).strip()
                # Remover caracteres problemáticos comunes en descargas
                clean_col = clean_col.replace('\n', ' ').replace('\r', ' ')
                clean_col = ' '.join(clean_col.split())  # Normalizar espacios
                new_columns.append(clean_col)
        
        df.columns = new_columns
        
        # 4. Eliminar filas que son headers repetidos (común en descargas web)
        if len(df) > 1:
            # Si la primera fila es igual a los headers, eliminarla
            first_row_as_str = [str(x).strip().lower() for x in df.iloc[0].values]
            headers_as_str = [str(x).strip().lower() for x in df.columns]
            
            if first_row_as_str == headers_as_str:
                df = df.iloc[1:].reset_index(drop=True)
                logger.info("Eliminada fila de headers duplicados")
        
        # 5. Limpiar celdas con valores problemáticos
        df = df.replace(['', ' ', 'NULL', 'null', 'None', '#N/A', '#REF!'], pd.NA)
        
        # 6. Reset index
        df = df.reset_index(drop=True)
        
        logger.info(f"Limpieza completada: {df.shape[0]} filas, {df.shape[1]} columnas")
        return df
    except Exception as e:
        logger.warning(f"Error durante limpieza: {str(e)}")
        return df

def manual_excel_parsing(file_content, file_name):
    """Último recurso: parsing manual para archivos muy problemáticos"""
    try:
        logger.info("Intentando parsing manual como último recurso...")
        if hasattr(file_content, 'seek'):
            file_content.seek(0)
        
        # Leer el contenido como bytes
        content_bytes = file_content.read()
        
        # Intentar diferentes encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
            try:
                content_text = content_bytes.decode(encoding)
                lines = content_text.split('\n')
                
                # Buscar líneas que parezcan datos tabulares
                data_lines = []
                for line in lines:
                    # Si la línea tiene múltiples campos separados
                    if line.count('\t') > 2 or line.count(',') > 2 or line.count(';') > 2:
                        data_lines.append(line)
                
                if len(data_lines) > 1:
                    # Crear DataFrame manual
                    import io
                    # Detectar separador más común
                    first_line = data_lines[0]
                    sep = '\t' if first_line.count('\t') > first_line.count(',') else ','
                    
                    df = pd.read_csv(io.StringIO('\n'.join(data_lines)), sep=sep)
                    if df.shape[0] > 0 and df.shape[1] > 1:
                        logger.info(f"Parsing manual exitoso con encoding {encoding}")
                        return df
            except Exception as e:
                continue
        
        return None
    except Exception as e:
        logger.warning(f"Error en parsing manual: {str(e)}")
        return None

# Configuraciones específicas para cada funcionalidad

# 1. Configuración para Órdenes de Compra
# Actualizar ORDENES_REQUIRED_COLUMNS con los nombres correctos
# Configuración para Órdenes de Compra
ORDENES_REQUIRED_COLUMNS = [
    "id_llamado", "llamado", "p_unit", "fec_contrato", "oc", "item",
    "codigo", "producto", "cant_oc", "monto_oc", "monto_recepcion",
    "cant_recep", "monto_saldo", "dias_de_atraso", "estado", "stock",
    "referencia", "proveedor", "lugar_entrega_oc", "fec_ult_recep",
    "fecha_recibido_proveedor", "fecha_oc", "saldo", "plazo_entrega",
    "tipo_vigencia", "vigencia", "det_recep"
]

ORDENES_COLUMN_TYPES = {
    "id_llamado": "BIGINT",
    "oc": "VARCHAR(500)",
    "p_unit": "NUMERIC(18,6)",
    "cant_oc": "NUMERIC(18,6)",
    "cant_recep": "NUMERIC(18,6)",
    "monto_oc": "NUMERIC(18,6)",
    "monto_recepcion": "NUMERIC(18,6)",
    "monto_saldo": "NUMERIC(18,6)",
    "fec_contrato": "VARCHAR(1000)",
    "fec_ult_recep": "VARCHAR(1000)",
    "fecha_oc": "VARCHAR(1000)",
    "fecha_recibido_proveedor": "VARCHAR(1000)",
    "dias_de_atraso": "BIGINT",
    "item": "VARCHAR(500)",
    "codigo": "VARCHAR(500)",
    "producto": "TEXT",
    "saldo": "NUMERIC(18,6)",
    "estado": "VARCHAR(500)",
    "stock": "VARCHAR(500)",
    "referencia": "VARCHAR(500)",
    "proveedor": "VARCHAR(500)",
    "lugar_entrega_oc": "VARCHAR(500)",
    "plazo_entrega": "VARCHAR(500)",
    "tipo_vigencia": "VARCHAR(500)",
    "vigencia": "VARCHAR(500)",
    "det_recep": "TEXT",
    "llamado": "VARCHAR(500)"
}

# 2. Configuración para Ejecución
# Configuración para Ejecución
EJECUCION_REQUIRED_COLUMNS = [
    "id_llamado", "licitacion", "proveedor", "codigo", "medicamento", "item", 
    "cantidad_maxima", "cantidad_emitida", "cantidad_recepcionada", "cantidad_distribuida", 
    "monto_adjudicado", "monto_emitido", "saldo", "porcentaje_emitido", 
    "ejecucion_mayor_al_50", "estado_stock", "estado_contrato", 
    "cantidad_ampliacion", "porcentaje_ampliado", "porcentaje_ampliacion_emitido", "obs"
]

EJECUCION_COLUMN_TYPES = {
    "id_llamado": "INTEGER",
    "licitacion": "VARCHAR(255)",
    "proveedor": "VARCHAR(255)",
    "codigo": "VARCHAR(255)",
    "medicamento": "TEXT",
    "item": "INTEGER",
    "cantidad_maxima": "NUMERIC",
    "cantidad_emitida": "NUMERIC",
    "cantidad_recepcionada": "NUMERIC",
    "cantidad_distribuida": "NUMERIC",
    "monto_adjudicado": "NUMERIC",
    "monto_emitido": "NUMERIC",
    "saldo": "NUMERIC",
    "porcentaje_emitido": "NUMERIC",
    "ejecucion_mayor_al_50": "VARCHAR(255)",
    "estado_stock": "VARCHAR(255)",
    "estado_contrato": "VARCHAR(255)",
    "cantidad_ampliacion": "NUMERIC",
    "porcentaje_ampliado": "NUMERIC",
    "porcentaje_ampliacion_emitido": "NUMERIC",
    "obs": "TEXT"
}

# 3. Configuración para Stock
# Configuración para Stock
STOCK_REQUIRED_COLUMNS = [
    "codigo", "producto", "concentracion", "forma_farmaceutica", 
    "presentacion", "clasificacion", "meses_en_movimiento", "cantidad_distribuida", 
    "stock_actual", "stock_reservado", "stock_disponible", "dmp", "estado_stock", 
    "stock_hosp", "oc"
]

STOCK_COLUMN_TYPES = {
    "codigo": "VARCHAR(50)",
    "producto": "TEXT",
    "concentracion": "TEXT",
    "forma_farmaceutica": "TEXT",
    "presentacion": "TEXT",
    "clasificacion": "TEXT",
    "meses_en_movimiento": "INTEGER",
    "cantidad_distribuida": "NUMERIC",
    "stock_actual": "NUMERIC",
    "stock_reservado": "NUMERIC",
    "stock_disponible": "NUMERIC",
    "dmp": "NUMERIC",
    "estado_stock": "VARCHAR(50)",
    "stock_hosp": "NUMERIC",
    "oc": "VARCHAR(255)"
}

# Definición de las columnas requeridas para Pedidos
PEDIDOS_REQUIRED_COLUMNS = [
    "nro_pedido", "simese", "fecha_pedido", "codigo", "medicamento", 
    "stock", "dmp", "cantidad", "meses_cantidad", "dias_transcurridos", 
    "estado", "prioridad", "nro_oc", "fecha_oc", "opciones"
]

# Tipos de datos para cada columna de Pedidos
PEDIDOS_COLUMN_TYPES = {
    "nro_pedido": "VARCHAR(255)",
    "simese": "VARCHAR(255)", 
    "fecha_pedido": "DATE",
    "codigo": "VARCHAR(50)",
    "medicamento": "TEXT",
    "stock": "NUMERIC",
    "dmp": "NUMERIC", 
    "cantidad": "NUMERIC",
    "meses_cantidad": "NUMERIC",
    "dias_transcurridos": "INTEGER",
    "estado": "VARCHAR(255)",
    "prioridad": "VARCHAR(255)",
    "nro_oc": "VARCHAR(255)",
    "fecha_oc": "DATE",
    "opciones": "VARCHAR(255)"
}

def dashboard_page():
    """Dashboard SICIAP completo - VERSION FINAL MEJORADA"""
    
    st.markdown("""
    <div style="width:100%; background-color:#f0f2f6; padding:10px; margin-bottom:20px; border-radius:5px; text-align:center;">
        <h1 style="font-size:2.5rem; font-weight:bold; margin:0; color:#262730;">SICIAP Dashboard</h1>
    </div>
    """, unsafe_allow_html=True)

    # CSS mejorado para evitar que el texto choque con los bordes
    st.markdown("""
    <style>
    /* Ajustes para usar todo el espacio disponible */
    .main-container {
        padding: 0 !important;
        max-width: 100% !important;
        width: 100% !important;
        margin: 0 !important;
        overflow-x: hidden !important;
    }

    .block-container {
        padding: 0 !important;
        max-width: 100% !important;
        width: 100% !important;
        overflow-x: hidden !important;
    }

    /* Ajustes para tablas y dataframes */
    .stDataFrame {
        width: 100% !important;
        overflow-x: auto !important;
    }

    [data-testid="stDataFrame"] {
        width: 100% !important;
        overflow-x: auto !important;
    }

    [data-testid="stDataFrame"] > div > iframe {
        min-height: 400px !important; 
        height: auto !important;
        width: 100% !important;
        max-width: 100% !important;
    }

    .element-container:has([data-testid="stDataFrame"]) {
        width: 100% !important;
        max-width: 100% !important;
        left: 0 !important;
        margin-left: 0 !important;
        padding-left: 0 !important;
        overflow-x: auto !important;
    }

    /* Estilos para tablas */
    .dataframe {
        border-collapse: collapse !important;
        width: 100% !important;
        border: 1px solid #ddd !important;
        font-size: 13px !important;
        display: block !important;
        overflow-x: auto !important;
        white-space: nowrap !important;
    }

    .dataframe thead {
        background-color: #4682B4 !important;
        color: white !important;
    }

    .dataframe thead th {
        padding: 8px 6px !important;
        text-align: left !important;
        font-weight: bold !important;
    }

    .dataframe tbody tr:nth-child(even) {
        background-color: #f2f2f2 !important;
    }

    .dataframe tbody tr:hover {
        background-color: #dbeeff !important;
    }

    /* Ajustes para que texto no choque con paredes */
    h1, h2, h3, p, div {
        max-width: 100% !important;
        word-wrap: break-word !important;
        overflow-wrap: break-word !important;
        white-space: normal !important;
        padding-right: 10px !important;
    }

    /* Ajustes para selectbox y multiselect */
    .stSelectbox, .stMultiselect {
        min-height: 0px !important;
        margin-bottom: 0.5rem !important;
        max-width: 100% !important;
    }

    /* Ajustes para metrics */
    [data-testid="stMetric"] {
        background-color: rgba(28, 131, 225, 0.1);
        border-radius: 4px;
        padding: 8px 4px;
        margin: 4px 0;
        word-wrap: break-word !important;
        overflow-wrap: break-word !important;
        width: 100% !important;
    }

    [data-testid="stMetricLabel"] {
        white-space: normal !important;
        overflow-wrap: break-word !important;
        font-size: 12px !important;
    }

    [data-testid="stMetricValue"] {
        white-space: normal !important;
        overflow-wrap: break-word !important;
    }

    /* Mejoras para expansores */
    .streamlit-expanderHeader {
        overflow-wrap: break-word !important;
        word-wrap: break-word !important;
        word-break: break-word !important;
        white-space: normal !important;
        padding-right: 10px !important;
    }

    /* Contener textos largos */
    div[data-testid="stHorizontalBlock"] > div {
        overflow-x: hidden !important;
        word-wrap: break-word !important;
        width: 100% !important;
    }

    /* Ajustes para que el contenido ocupe todo el ancho */
    .css-18e3th9 {
        padding: 1rem 1rem 1rem !important;
        width: 100% !important;
    }

    .css-1d391kg {
        width: 100% !important;
    }

    /* Asegurar que los widgets de Streamlit ocupen todo el ancho disponible */
    .stApp {
        width: 100% !important;
        max-width: 100% !important;
    }

    /* Ajustar contenedor principal para que use todo el espacio */
    .reportview-container {
        width: 100% !important;
        max-width: 100% !important;
        padding: 0 !important;
        margin: 0 !important;
    }

    /* Evitar que la interfaz se encoja */
    .dashboard-container {
        display: flex !important;
        flex-direction: column !important;
        width: 100% !important;
        min-height: 100vh !important;
    }

    /* Corregir columnas para que se distribuyan correctamente */
    .row-widget.stHorizontalBlock {
        width: 100% !important;
        max-width: 100% !important;
        flex-wrap: nowrap !important;
    }

    /* Asegurar que las métricas no se encojan */
    .stMetric {
        width: 100% !important;
        min-width: 150px !important;
    }

    /* Corregir vista general de dashboard */
    .dashboard {
        width: 100% !important;
        display: flex !important;
        flex-direction: column !important;
    }

    /* Eliminar padding excesivo en los widgets */
    .stWidgetBox {
        padding: 0 !important;
        margin: 0 !important;
        max-width: 100% !important;
        width: 100% !important;
    }

    /* Ajustar sidebar para evitar problemas de espacio */
    [data-testid="stSidebar"] {
        min-width: 200px !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # Conexión a la base de datos
    db_config = get_db_config()
    conn = PostgresConnection(**db_config)
    
    if not conn.connect():
        st.error("❌ No se pudo conectar a PostgreSQL.")
        return

    try:
        # Filtros superiores con mejor distribución para pantallas grandes
        col1, col2, col3, col4 = st.columns([3, 2, 2, 3])
        
        with col1:
            search_query = st.text_input("🔍 Buscar por código o descripción")
        
        with col2:
            # Obtener estados únicos para filtrar
            try:
                estados_query = """
                SELECT DISTINCT estado FROM siciap.ordenes
                WHERE estado IS NOT NULL
                ORDER BY estado
                """
                estados_df = pd.read_sql_query(text(estados_query), conn.engine)
                estados = ["Todos"] + estados_df['estado'].tolist()
            except Exception as e:
                st.error(f"Error al obtener estados: {str(e)}")
                estados = ["Todos"]
            
            estado_seleccionado = st.selectbox("Estado", estados)
        
        with col3:
            # Filtro por nivel de stock - AGREGADO "Sin DMP"
            niveles_stock = ["Todos", "Atención", "Precaución", "Óptimo", "Sin DMP"]
            nivel_seleccionado = st.selectbox("Nivel Stock", niveles_stock)
        
        with col4:
            # Vista completa sin límites
            vista_completa = st.checkbox("Vista Completa", value=True, help="Mostrar todos los registros sin límite")
        
        # Mostrar información de filtro activo
        if search_query:
            st.info(f"🔍 Filtros activos: '{search_query}' | Estado: {estado_seleccionado} | Nivel: {nivel_seleccionado}")
        
        # 1. SECCIÓN: DATOS DE ÓRDENES DE COMPRA - Con selector como en Ejecución
        st.markdown("### 📋 Órdenes de Compra - Agrupadas por Estado (Más Recientes Primero)")

        try:
            # Base para los filtros SQL
            filtro_base = ""
            params = {}
            
            if search_query:
                search_value = f"%{search_query.replace('%', '%%').replace('\'', '\'\'')}%"
                filtro_base = " AND (CAST(codigo AS TEXT) ILIKE :search_value OR CAST(producto AS TEXT) ILIKE :search_value)"
                params["search_value"] = search_value
            
            # Obtener conteos filtrados por estado
            estados_conteo_query = f"""
            SELECT 
                estado, 
                COUNT(*) as cantidad
            FROM 
                siciap.ordenes
            WHERE 
                estado IS NOT NULL
                {filtro_base}
            GROUP BY 
                estado
            ORDER BY 
                estado
            """
            
            estados_conteo_df = pd.read_sql_query(
                sql=text(estados_conteo_query), 
                con=conn.engine, 
                params=params
            )
            
            # Mostrar conteos por estado como métricas
            if not estados_conteo_df.empty:
                # Determinar cuántas columnas necesitamos para los estados
                num_estados = len(estados_conteo_df)
                cols_needed = min(5, num_estados)  # Máximo 5 columnas
                
                # Crear columnas dinámicamente
                cols = st.columns(cols_needed)
                
                # Distribuir los estados entre las columnas
                for i, (_, row) in enumerate(estados_conteo_df.iterrows()):
                    col_idx = i % cols_needed
                    with cols[col_idx]:
                        estado = row['estado']
                        cantidad = row['cantidad']
                        
                        # Determinar color del delta según el estado
                        if estado.lower() in ['entregado', 'finalizado', 'completado']:
                            delta_color = "normal"
                        elif estado.lower() in ['en proceso', 'parcial']:
                            delta_color = "off"
                        else:
                            delta_color = "inverse"
                        
                        st.metric(f"📋 {estado}", cantidad, delta=estado, delta_color=delta_color)
            
            # Selector para elegir estado - similar a la sección de ejecución
            if not estados_conteo_df.empty:
                st.markdown("#### Seleccione un estado para ver órdenes:")
                
                # Crear opciones para el selectbox con información del estado
                estado_options = []
                estado_display = {}
                
                # Añadir "Todos" como primera opción
                estado_options.append("Todos")
                estado_display["Todos"] = "Todos los estados"
                
                for _, row in estados_conteo_df.iterrows():
                    estado = row['estado']
                    cantidad = row['cantidad']
                    
                    # Crear texto para mostrar en el selectbox
                    display_text = f"{estado} ({cantidad} órdenes)"
                    estado_options.append(estado)
                    estado_display[estado] = display_text
                
                # Función para mostrar el texto en el selectbox
                def format_estado(estado):
                    return estado_display[estado]
                
                # Seleccionar estado
                selected_estado = st.selectbox(
                    "Estados disponibles:",
                    options=estado_options,
                    format_func=format_estado,
                    index=0 if estado_seleccionado == "Todos" else estado_options.index(estado_seleccionado) if estado_seleccionado in estado_options else 0
                )
                
                # Construir la consulta para el estado seleccionado o todos los estados
                ordenes_query = f"""
                SELECT
                    oc as "N° OC",
                    codigo as "CÓDIGO",
                    producto as "PRODUCTO",
                    proveedor as "PROVEEDOR",
                    llamado as "LLAMADO",
                    fecha_oc as "FECHA OC",
                    cant_oc as "CANTIDAD",
                    cant_recep as "RECEPCIONADO",
                    saldo as "SALDO",
                    monto_oc as "MONTO TOTAL",
                    p_unit as "PRECIO UNIT.",
                    dias_de_atraso as "DÍAS ATRASO",
                    plazo_entrega as "PLAZO ENTREGA",
                    fec_ult_recep as "ÚLT. RECEPCIÓN",
                    lugar_entrega_oc as "LUGAR ENTREGA",
                    estado as "ESTADO"
                FROM 
                    siciap.ordenes
                WHERE 
                    1=1
                    {filtro_base}
                """
                
                # Aplicar filtro por estado si no es "Todos"
                if selected_estado != "Todos":
                    ordenes_query += f" AND estado = :estado"
                    params["estado"] = selected_estado
                
                # Para fechas almacenadas como texto en formato DD/MM/YYYY
                ordenes_query += " ORDER BY TO_DATE(fecha_oc, 'DD/MM/YYYY') DESC"
                
                # Ejecutar consulta con parámetros
                ordenes_df = pd.read_sql_query(
                    sql=text(ordenes_query), 
                    con=conn.engine, 
                    params=params
                )
                
                if not ordenes_df.empty:
                    # Formatear fechas
                    for fecha_col in ['FECHA OC', 'ÚLT. RECEPCIÓN']:
                        if fecha_col in ordenes_df.columns:
                            ordenes_df[fecha_col] = safe_date_conversion(ordenes_df[fecha_col]).dt.strftime('%d/%m/%Y')
                    
                    # Formatear montos
                    for monto_col in ['MONTO TOTAL', 'PRECIO UNIT.']:
                        if monto_col in ordenes_df.columns:
                            ordenes_df[monto_col] = ordenes_df[monto_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "$0")
                    
                    # Calcular estadísticas
                    total_ocs = len(ordenes_df)
                    
                    # Añadir título con información de la consulta
                    if selected_estado == "Todos":
                        st.markdown(f"##### Mostrando todas las órdenes ({total_ocs})")
                    else:
                        st.markdown(f"##### Mostrando órdenes con estado '{selected_estado}' ({total_ocs})")
                    
                    # Mostrar tabla
                    st.dataframe(ordenes_df, use_container_width=True, height=400)
                    
                    # Estadísticas del grupo
                    total_cantidad = ordenes_df['CANTIDAD'].sum() if 'CANTIDAD' in ordenes_df.columns else 0
                    total_recepcionado = ordenes_df['RECEPCIONADO'].sum() if 'RECEPCIONADO' in ordenes_df.columns else 0
                    total_saldo = ordenes_df['SALDO'].sum() if 'SALDO' in ordenes_df.columns else 0
                    
                    st.markdown("#### Estadísticas:")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📋 Total OCs", total_ocs)
                    with col2:
                        st.metric("📦 Cantidad Total", f"{total_cantidad:,}")
                    with col3:
                        st.metric("✅ Recepcionado", f"{total_recepcionado:,}")
                    with col4:
                        st.metric("⏳ Saldo Pendiente", f"{total_saldo:,}")
                else:
                    st.info(f"No se encontraron órdenes que coincidan con los criterios de búsqueda")
            
            # Mostrar totales generales
            total_query = """
            SELECT COUNT(*) as total, 
                   COALESCE(SUM(monto_oc), 0) as total_monto 
            FROM siciap.ordenes
            """
            total_df = pd.read_sql_query(text(total_query), conn.engine)
            total_oc = total_df['total'].iloc[0] if not total_df.empty else 0
            total_monto = total_df['total_monto'].iloc[0] if not total_df.empty else 0
            
            st.info(f"📊 **Resumen General:** {total_oc:,} órdenes totales | Monto total: ${total_monto:,.2f}")
            
        except Exception as e:
            st.error(f"Error al consultar órdenes: {str(e)}")
            st.error(traceback.format_exc())

        # 2. SECCIÓN: ESTADO DE STOCK - CONVERTIDO A EXPANDER CERRADO
        stock_expander = st.expander("### 📦 Estado de Stock - Nivel de Criticidad", expanded=False)
        
        with stock_expander:
            try:
                # Base para los filtros SQL de stock
                filtro_stock_base = ""
                params_stock = {}
                
                if search_query:
                    search_value = f"%{search_query.replace('%', '%%').replace('\'', '\'\'')}%"
                    filtro_stock_base = " AND (CAST(codigo AS TEXT) ILIKE :search_value OR CAST(producto AS TEXT) ILIKE :search_value)"
                    params_stock["search_value"] = search_value
                
                # Consulta para obtener conteos por nivel de criticidad
                stock_conteo_query = f"""
                SELECT
                    CASE 
                        WHEN dmp IS NULL OR dmp = 0 THEN 'Sin DMP'
                        WHEN stock_disponible IS NULL THEN 'Sin Stock'
                        WHEN stock_disponible < dmp * 0.3 THEN 'Atención'
                        WHEN stock_disponible < dmp * 0.7 THEN 'Precaución'
                        ELSE 'Óptimo'
                    END AS nivel_stock,
                    COUNT(*) as cantidad
                FROM 
                    siciap.stock_critico
                WHERE 1=1
                    {filtro_stock_base}
                GROUP BY 
                    nivel_stock
                ORDER BY 
                    nivel_stock
                """
                
                stock_conteo_df = pd.read_sql_query(
                    sql=text(stock_conteo_query),
                    con=conn.engine,
                    params=params_stock
                )
                
                # Crear tarjetas para niveles de stock usando métricas de Streamlit
                if not stock_conteo_df.empty:
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    # Función para obtener conteo de un nivel específico
                    def get_conteo_nivel(nivel):
                        for _, row in stock_conteo_df.iterrows():
                            if row['nivel_stock'] == nivel:
                                return row['cantidad']
                        return 0
                    
                    # Mostrar conteos por nivel usando st.metric
                    with col1:
                        critico_count = get_conteo_nivel("Atención")
                        st.metric("🔴 Atención", critico_count, delta="CRÍTICO", delta_color="inverse")
                    
                    with col2:
                        bajo_count = get_conteo_nivel("Precaución")
                        st.metric("🟡 Precaución", bajo_count, delta="BAJO", delta_color="off")
                    
                    with col3:
                        normal_count = get_conteo_nivel("Óptimo")
                        st.metric("🟢 Óptimo", normal_count, delta="NORMAL", delta_color="normal")
                    
                    with col4:
                        sin_dmp_count = get_conteo_nivel("Sin DMP")
                        st.metric("⚫ Sin DMP", sin_dmp_count, delta="SIN DMP", delta_color="off")
                    
                    with col5:
                        sin_stock_count = get_conteo_nivel("Sin Stock")
                        st.metric("⚪ Sin Stock", sin_stock_count, delta="SIN STOCK", delta_color="off")

                # Consulta para stock con cálculo de cobertura - CORREGIDA para PostgreSQL
                stock_query = f"""
                SELECT
                    codigo as "CÓDIGO",
                    producto as "PRODUCTO",
                    concentracion as "CONCENTRACIÓN",
                    forma_farmaceutica as "FORMA FARMACÉUTICA",
                    TRUNC(CAST(stock_actual AS NUMERIC)) as "STOCK ACTUAL",
                    TRUNC(CAST(stock_reservado AS NUMERIC)) as "STOCK RESERVADO",
                    TRUNC(CAST(stock_disponible AS NUMERIC)) as "STOCK DISPONIBLE",
                    TRUNC(CAST(dmp AS NUMERIC)) as "DMP",
                    CASE 
                        WHEN dmp IS NULL OR dmp = 0 THEN NULL
                        WHEN stock_disponible IS NULL THEN NULL
                        ELSE ROUND(CAST((stock_disponible::numeric / dmp::numeric) AS NUMERIC), 1)
                    END AS "COBERTURA (MESES)",
                    CASE 
                        WHEN dmp IS NULL OR dmp = 0 THEN 'Sin DMP'
                        WHEN stock_disponible IS NULL THEN 'Sin Stock'
                        WHEN stock_disponible < dmp * 0.3 THEN 'Atención'
                        WHEN stock_disponible < dmp * 0.7 THEN 'Precaución'
                        ELSE 'Óptimo'
                    END AS "NIVEL STOCK"
                FROM 
                    siciap.stock_critico
                WHERE 1=1
                    {filtro_stock_base}
                """
                
                # 3. SECCIÓN: EJECUCIÓN DE CONTRATOS - SIN EXPANSORES ANIDADOS
                st.markdown("### 📋 Ejecución de Contratos - Avance por Llamado")

                try:
                    # Base para los filtros SQL de ejecución
                    filtro_ejecucion_base = ""
                    params_ejecucion = {}
                    
                    if search_query:
                        search_value = f"%{search_query.replace('%', '%%').replace('\'', '\'\'')}%"
                        filtro_ejecucion_base = " AND (CAST(e.codigo AS TEXT) ILIKE :search_value OR CAST(e.medicamento AS TEXT) ILIKE :search_value)"
                        params_ejecucion["search_value"] = search_value

                    # Consulta para obtener métricas generales de ejecución
                    ejecucion_metricas_query = f"""
                    SELECT
                        COUNT(DISTINCT id_llamado) as total_llamados,
                        COUNT(*) as total_items,
                        COALESCE(SUM(COALESCE(cantidad_maxima, 0)), 0) as total_cantidad_maxima,
                        COALESCE(SUM(COALESCE(cantidad_emitida, 0)), 0) as total_cantidad_emitida,
                        COALESCE(SUM(COALESCE(cantidad_maxima, 0) - COALESCE(cantidad_emitida, 0)), 0) as total_saldo_pendiente,
                        COALESCE(AVG(COALESCE(porcentaje_emitido, 0)), 0) as promedio_ejecucion
                    FROM 
                        siciap.ejecucion e
                    WHERE 1=1
                        {filtro_ejecucion_base}
                    """
                    
                    ejecucion_metricas_df = pd.read_sql_query(
                        sql=text(ejecucion_metricas_query),
                        con=conn.engine,
                        params=params_ejecucion
                    )
                    
                    # Métricas principales de ejecución
                    if not ejecucion_metricas_df.empty:
                        metricas = ejecucion_metricas_df.iloc[0]
                        
                        col1, col2, col3, col4, col5 = st.columns(5)
                        
                        with col1:
                            st.metric("🔖 Llamados", int(metricas['total_llamados']), delta="Con Contratos")
                        
                        with col2:
                            st.metric("📋 Items", int(metricas['total_items']), delta="Total Ejecución")
                        
                        with col3:
                            avance_general = metricas['promedio_ejecucion']
                            avance_general = avance_general if avance_general is not None else 0
                            # Formatear avance sin decimales innecesarios
                            avance_str = f"{int(avance_general)}%" if avance_general == int(avance_general) else f"{avance_general:.1f}%"
                            delta_color = "normal" if avance_general >= 70 else "off" if avance_general >= 40 else "inverse"
                            st.metric("📈 Avance", avance_str, delta="Promedio", delta_color=delta_color)
                        
                        with col4:
                            total_emitido = int(metricas['total_cantidad_emitida'] if metricas['total_cantidad_emitida'] is not None else 0)
                            # Formatear con punto como separador de miles
                            total_emitido_str = f"{total_emitido:,}".replace(',', '.')
                            st.metric("✅ Emitido", total_emitido_str, delta="Unidades", delta_color="normal")
                        
                        with col5:
                            total_saldo = int(metricas['total_saldo_pendiente']) if metricas['total_saldo_pendiente'] is not None else 0
                            # Formatear con punto como separador de miles
                            total_saldo_str = f"{total_saldo:,}".replace(',', '.')
                            st.metric("⏳ Saldo", total_saldo_str, delta="Pendiente", delta_color="inverse")

                    # Obtener llamados para agrupar - AHORA INCLUYE dirigido_a, lugares y datos de vigencia
                    llamados_ejecucion_query = f"""
                    SELECT DISTINCT
                        e.id_llamado,
                        e.licitacion,
                        COUNT(*) as cantidad_items,
                        ROUND(AVG(COALESCE(e.porcentaje_emitido, 0)), 1) as porcentaje_promedio,
                        TRUNC(SUM(COALESCE(e.cantidad_maxima, 0) - COALESCE(e.cantidad_emitida, 0))) as saldo_total_llamado,
                        COALESCE(d.descripcion_llamado, 'Sin descripción') as descripcion_llamado,
                        d.dirigido_a,
                        d.lugares,
                        d.numero_contrato,
                        d.fecha_inicio,
                        d.fecha_fin,
                        CASE 
                            WHEN d.fecha_fin IS NULL THEN 'Indeterminado'
                            WHEN UPPER(CAST(d.fecha_fin AS TEXT)) LIKE '%CUMPLIMIENTO TOTAL%' THEN 'Sí'
                            WHEN d.fecha_fin = 'CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES' THEN 'Sí'
                            WHEN d.fecha_fin = 'cumplimiento total de las obligaciones' THEN 'Sí'
                            ELSE 'Sí'
                        END as vigente
                    FROM 
                        siciap.ejecucion e
                    LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
                    WHERE 1=1
                        {filtro_ejecucion_base}
                    GROUP BY
                        e.id_llamado, e.licitacion, d.descripcion_llamado, d.dirigido_a, d.lugares, d.numero_contrato,
                        d.fecha_inicio, d.fecha_fin
                    ORDER BY
                        porcentaje_promedio ASC, saldo_total_llamado DESC
                    """
                    
                    llamados_ejecucion_df = pd.read_sql_query(
                        sql=text(llamados_ejecucion_query),
                        con=conn.engine,
                        params=params_ejecucion
                    )
                    
                    # En lugar de usar expansores anidados, usar selectbox para elegir el llamado
                    if not llamados_ejecucion_df.empty:
                        st.markdown("#### Seleccione un llamado para ver detalles:")
                        
                        # Crear opciones para el selectbox con información del llamado
                        llamado_options = []
                        llamado_display = {}

                        for _, row in llamados_ejecucion_df.iterrows():
                            id_llamado = row['id_llamado']
                            licitacion = row['licitacion']
                            porcentaje_promedio = row['porcentaje_promedio']
                            cantidad_items = row['cantidad_items']
                            
                            # Formatear porcentaje sin decimales innecesarios
                            porcentaje_str = f"{int(porcentaje_promedio)}%" if porcentaje_promedio == int(porcentaje_promedio) else f"{porcentaje_promedio:.1f}%"
                            
                            # Crear texto para mostrar en el selectbox
                            display_text = f"ID: {id_llamado} - {licitacion} | Avance: {porcentaje_str} | Items: {cantidad_items}"
                            llamado_options.append(id_llamado)
                            llamado_display[id_llamado] = display_text
                        
                        # Función para mostrar el texto en el selectbox
                        def format_llamado(id_llamado):
                            return llamado_display[id_llamado]
                        
                        # Seleccionar llamado
                        selected_llamado = st.selectbox(
                            "Llamados disponibles:",
                            options=llamado_options,
                            format_func=format_llamado
                        )
                        
                        if selected_llamado:
                            # Obtener datos del llamado seleccionado
                            llamado_seleccionado = llamados_ejecucion_df[llamados_ejecucion_df['id_llamado'] == selected_llamado].iloc[0]

                            # Mostrar información del llamado
                            st.markdown("---")
                            st.markdown(f"### Detalles del llamado ID: {selected_llamado}")
                            st.markdown(f"**Licitación:** {llamado_seleccionado['licitacion']}")

                            # Mostrar descripción con estilo diferente si no hay datos
                            if pd.notna(llamado_seleccionado.get('descripcion_llamado')) and llamado_seleccionado['descripcion_llamado'] != 'Sin descripción':
                                st.markdown(f"**Descripción:** {llamado_seleccionado['descripcion_llamado']}")
                            else:
                                st.markdown("**Descripción:** <span style='color:#888888;font-style:italic;'>Sin descripción</span>", unsafe_allow_html=True)

                            # Mostrar número de contrato si existe
                            if pd.notna(llamado_seleccionado.get('numero_contrato')):
                                st.markdown(f"**Número de Contrato:** {llamado_seleccionado['numero_contrato']}")

                            # Mostrar fechas de inicio y fin si existen
                            col_fechas1, col_fechas2, col_fechas3 = st.columns([1, 1, 1])
                            with col_fechas1:
                                if pd.notna(llamado_seleccionado.get('fecha_inicio')):
                                    inicio = llamado_seleccionado['fecha_inicio']
                                    if isinstance(inicio, pd.Timestamp):
                                        inicio = inicio.strftime('%d/%m/%Y')
                                    elif isinstance(inicio, str):
                                        # Intentar convertir a formato día/mes/año si está en otro formato
                                        try:
                                            fecha_dt = pd.to_datetime(inicio, errors='coerce', dayfirst=True)
                                            if not pd.isna(fecha_dt):
                                                inicio = fecha_dt.strftime('%d/%m/%Y')
                                        except:
                                            pass
                                    st.markdown(f"**Fecha inicio:** {inicio}")
                            with col_fechas2:
                                if pd.notna(llamado_seleccionado.get('fecha_fin')):
                                    fin = llamado_seleccionado['fecha_fin']
                                    if isinstance(fin, pd.Timestamp):
                                        fin = fin.strftime('%d/%m/%Y')
                                    elif isinstance(fin, str):
                                        # Comprobar si contiene "CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES"
                                        if "CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES" in fin.upper() or "cumplimiento total de las obligaciones" in fin.lower():
                                            fin = "CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES"
                                        else:
                                            # Intentar convertir a formato día/mes/año si está en otro formato
                                            try:
                                                fecha_dt = pd.to_datetime(fin, errors='coerce', dayfirst=True)
                                                if not pd.isna(fecha_dt):
                                                    fin = fecha_dt.strftime('%d/%m/%Y')
                                            except:
                                                pass
                                    st.markdown(f"**Fecha fin:** {fin}")
                            with col_fechas3:
                                if pd.notna(llamado_seleccionado.get('vigente')):
                                    vigencia = llamado_seleccionado['vigente']
                                    # Verificar también si fecha_fin contiene "cumplimiento total"
                                    fin_cumplimiento = False
                                    if pd.notna(llamado_seleccionado.get('fecha_fin')):
                                        fin_str = str(llamado_seleccionado['fecha_fin']).lower()
                                        if 'cumplimiento total' in fin_str:
                                            fin_cumplimiento = True
                                            
                                    if vigencia == "Sí" or fin_cumplimiento:
                                        color = "green"
                                        st.markdown(f"**Vigente:** <span style='color:{color};font-weight:bold'>Sí</span>", unsafe_allow_html=True)
                                    elif vigencia == "No":
                                        color = "red"
                                        st.markdown(f"**Vigente:** <span style='color:{color};font-weight:bold'>{vigencia}</span>", unsafe_allow_html=True)
                                    else:
                                        # Para "Indeterminado" u otros valores, mostrar en gris
                                        st.markdown(f"**Vigente:** <span style='color:#888888;font-style:italic;'>{vigencia}</span>", unsafe_allow_html=True)

                            # Avance y estadísticas
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.markdown(f"**Avance:** {llamado_seleccionado['porcentaje_promedio']:.1f}%")
                            with col2:
                                st.markdown(f"**Items:** {llamado_seleccionado['cantidad_items']}")
                            with col3:
                                st.markdown(f"**Saldo pendiente:** {llamado_seleccionado['saldo_total_llamado']:,.0f}")
                            
                            # Consulta para obtener items de este llamado específico con más detalles
                            items_query = f"""
                            SELECT
                                e.codigo as "CÓDIGO",
                                e.medicamento as "PRODUCTO",
                                e.proveedor as "PROVEEDOR",
                                TRUNC(CAST(e.cantidad_maxima AS NUMERIC)) as "CANT. MÁXIMA",
                                TRUNC(CAST(e.cantidad_emitida AS NUMERIC)) as "CANT. EMITIDA",
                                TRUNC(CAST(e.cantidad_recepcionada AS NUMERIC)) as "CANT. RECEPCIONADA",
                                TRUNC(CAST(e.cantidad_distribuida AS NUMERIC)) as "CANT. DISTRIBUIDA",
                                TRUNC(CAST(COALESCE(e.cantidad_maxima, 0) - COALESCE(e.cantidad_emitida, 0) AS NUMERIC)) as "SALDO PENDIENTE",
                                ROUND(CAST(COALESCE(e.porcentaje_emitido, 0) AS NUMERIC), 1) as "% EMITIDO",
                                TRUNC(CAST(e.monto_adjudicado AS NUMERIC)) as "MONTO ADJUDICADO",
                                TRUNC(CAST(e.monto_emitido AS NUMERIC)) as "MONTO EMITIDO",
                                e.estado_contrato as "ESTADO CONTRATO",
                                TRUNC(CAST(s.stock_disponible AS NUMERIC)) as "STOCK ACTUAL",
                                TRUNC(CAST(s.dmp AS NUMERIC)) as "DMP",
                                CASE 
                                    WHEN s.dmp IS NULL OR s.dmp = 0 THEN 'Sin DMP'
                                    WHEN s.stock_disponible IS NULL THEN 'Sin Stock'
                                    WHEN s.stock_disponible < s.dmp * 0.3 THEN 'Atención'
                                    WHEN s.stock_disponible < s.dmp * 0.7 THEN 'Precaución'
                                    ELSE 'Óptimo'
                                END AS "NIVEL STOCK",
                                CASE 
                                    WHEN d.fecha_fin IS NULL THEN 'Indeterminado'
                                    WHEN UPPER(CAST(d.fecha_fin AS TEXT)) LIKE '%CUMPLIMIENTO TOTAL%' THEN 'Sí'
                                    WHEN d.fecha_fin = 'CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES' THEN 'Sí'
                                    WHEN d.fecha_fin = 'cumplimiento total de las obligaciones' THEN 'Sí'
                                    ELSE 'Sí'
                                END as "VIGENTE"
                            FROM 
                                siciap.ejecucion e
                            LEFT JOIN
                                siciap.stock_critico s ON CAST(e.codigo AS VARCHAR) = CAST(s.codigo AS VARCHAR)
                            LEFT JOIN
                                siciap.datosejecucion d ON e.id_llamado = d.id_llamado
                            WHERE 
                                e.id_llamado = :id_llamado
                                {filtro_ejecucion_base}
                            ORDER BY 
                                e.porcentaje_emitido ASC, (COALESCE(e.cantidad_maxima, 0) - COALESCE(e.cantidad_emitida, 0)) DESC, e.codigo
                            """
                            
                            # Parámetros para la consulta
                            query_params = {"id_llamado": selected_llamado}
                            if search_query:
                                query_params["search_value"] = search_value
                            
                            # Ejecutar consulta
                            items_df = pd.read_sql_query(
                                sql=text(items_query),
                                con=conn.engine,
                                params=query_params
                            )
                            
                            if not items_df.empty:
                                # Formatear porcentaje_emitido
                                items_df['% EMITIDO'] = items_df['% EMITIDO'].apply(
                                    lambda x: f"{x:.1f}%" if pd.notnull(x) else "0.0%"
                                )
                                
                                # Formatear columnas numéricas para eliminar decimales innecesarios
                                numeric_columns = [
                                    'CANT. MÁXIMA', 'CANT. EMITIDA', 'CANT. RECEPCIONADA', 
                                    'CANT. DISTRIBUIDA', 'SALDO PENDIENTE', 'STOCK ACTUAL', 'DMP'
                                ]

                                for col in numeric_columns:
                                    if col in items_df.columns:
                                        # SOLO aplicar format_numeric_value UNA VEZ
                                        items_df[col] = items_df[col].apply(
                                            lambda x: format_numeric_value(x)
                                        )

                                # Formatear montos con punto como separador de miles y sin decimales innecesarios
                                monto_columns = ['MONTO ADJUDICADO', 'MONTO EMITIDO']
                                for col in monto_columns:
                                    if col in items_df.columns:
                                        items_df[col] = items_df[col].apply(
                                            lambda x: format_numeric_value(x, use_currency=True) if pd.notnull(x) else "-"
                                        )

                                # Mostrar tabla principal con todos los datos de ejecución
                                st.markdown("#### Tabla de items:")

                                # Opciones para mostrar columnas
                                show_extended = st.checkbox("Mostrar datos extendidos (montos, distribución)", value=False)

                                if show_extended:
                                    # Mostrar todas las columnas
                                    cols_to_show = [
                                        'CÓDIGO', 'PRODUCTO', 'PROVEEDOR', 
                                        'CANT. MÁXIMA', 'CANT. EMITIDA', 'CANT. RECEPCIONADA', 'CANT. DISTRIBUIDA',
                                        'SALDO PENDIENTE', '% EMITIDO', 'MONTO ADJUDICADO', 'MONTO EMITIDO',
                                        'ESTADO CONTRATO'
                                    ]
                                else:
                                    # Mostrar columnas básicas
                                    cols_to_show = [
                                        'CÓDIGO', 'PRODUCTO', 'PROVEEDOR', 
                                        'CANT. MÁXIMA', 'CANT. EMITIDA', 'SALDO PENDIENTE', 
                                        '% EMITIDO', 'ESTADO CONTRATO'
                                    ]

                                # Añadir VIGENTE solo si existe
                                if 'VIGENTE' in items_df.columns:
                                    cols_to_show.append('VIGENTE')

                                # Formatear montos si están presentes
                                if 'MONTO ADJUDICADO' in items_df.columns:
                                    items_df['MONTO ADJUDICADO'] = items_df['MONTO ADJUDICADO'].apply(
                                        lambda x: format_numeric_value(x, use_currency=True)
                                    )

                                if 'MONTO EMITIDO' in items_df.columns:
                                    items_df['MONTO EMITIDO'] = items_df['MONTO EMITIDO'].apply(
                                        lambda x: format_numeric_value(x, use_currency=True) if pd.notnull(x) else "-"
                                    )

                                # Filtrar solo las columnas que existen en el DataFrame
                                cols_existentes = [col for col in cols_to_show if col in items_df.columns]

                                # Aplicar colores a la columna VIGENTE si existe
                                if 'VIGENTE' in items_df.columns:
                                    def highlight_vigencia(val):
                                        if val == 'Sí':
                                            return 'background-color: #d4edda; color: #155724'  # Verde claro
                                        elif val == 'No':
                                            return 'background-color: #f8d7da; color: #721c24'  # Rojo claro
                                        else:
                                            return 'background-color: #fff3cd; color: #856404'  # Amarillo claro
                                    
                                    # Aplicar estilo condicional
                                    styled_df = items_df[cols_existentes].style.applymap(
                                        highlight_vigencia, 
                                        subset=['VIGENTE'] if 'VIGENTE' in cols_existentes else []
                                    )
                                    
                                    st.dataframe(
                                        styled_df,
                                        use_container_width=True, 
                                        height=400
                                    )
                                else:
                                    st.dataframe(
                                        items_df[cols_existentes], 
                                        use_container_width=True, 
                                        height=400
                                    )
                                
                                # Mostrar información de stock en tabla separada si hay datos
                                mostrar_stock = st.checkbox(f"Mostrar información de stock del llamado {selected_llamado}", key=f"stock_{selected_llamado}")

                                if mostrar_stock:
                                    # Crear columnas más específicas para la información de stock
                                    stock_cols = ['CÓDIGO', 'PRODUCTO', 'STOCK ACTUAL', 'DMP', 'NIVEL STOCK']
                                    
                                    # Añadir información calculada si tenemos DMP
                                    if 'DMP' in items_df.columns and 'STOCK ACTUAL' in items_df.columns:
                                        items_df['COBERTURA (MESES)'] = items_df.apply(
                                            lambda row: round(row['STOCK ACTUAL'] / row['DMP'], 1) 
                                            if pd.notnull(row['DMP']) and pd.notnull(row['STOCK ACTUAL']) and row['DMP'] > 0 
                                            else None, 
                                            axis=1
                                        )
                                        stock_cols.append('COBERTURA (MESES)')
                                    
                                    stock_df = items_df[stock_cols]
                                    
                                    # Aplicar estilos condicionales según nivel de stock
                                    def highlight_nivel_stock(val):
                                        if val == 'Atención':
                                            return 'background-color: #f8d7da; color: #721c24'  # Rojo claro
                                        elif val == 'Precaución':
                                            return 'background-color: #fff3cd; color: #856404'  # Amarillo claro
                                        elif val == 'Óptimo':
                                            return 'background-color: #d4edda; color: #155724'  # Verde claro
                                        else:
                                            return ''
                                    
                                    if not stock_df.empty and not stock_df['STOCK ACTUAL'].isna().all():
                                        st.markdown("**📦 Información de Stock Actual:**")
                                        styled_stock_df = stock_df.style.applymap(
                                            highlight_nivel_stock, 
                                            subset=['NIVEL STOCK']
                                        )
                                        st.dataframe(
                                            styled_stock_df,
                                            use_container_width=True,
                                            height=200
                                        )
                                    else:
                                        st.info("No hay información de stock disponible para estos productos.")
                                
                                # Formatear columnas numéricas para eliminar decimales innecesarios
                                if 'STOCK ACTUAL' in stock_df.columns:
                                    stock_df['STOCK ACTUAL'] = stock_df['STOCK ACTUAL'].apply(
                                        lambda x: format_numeric_value(x)
                                    )

                                if 'DMP' in stock_df.columns:
                                    stock_df['DMP'] = stock_df['DMP'].apply(
                                        lambda x: format_numeric_value(x)
                                    )

                                if 'COBERTURA (MESES)' in stock_df.columns:
                                    stock_df['COBERTURA (MESES)'] = stock_df['COBERTURA (MESES)'].apply(
                                        lambda x: format_numeric_value(x)
                                    )

                                # Estadísticas del llamado
                                total_items_llamado = len(items_df)
                                total_max_llamado = items_df['CANT. MÁXIMA'].sum()
                                total_emitido_llamado = items_df['CANT. EMITIDA'].sum()
                                total_saldo_llamado = items_df['SALDO PENDIENTE'].sum()
                                
                                st.markdown("#### Estadísticas del llamado:")
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("📋 Items", total_items_llamado)
                                with col2:
                                    # Formatear para eliminar decimales innecesarios
                                    total_max_str = f"{int(total_max_llamado):,}".replace(',', '.') if total_max_llamado == int(total_max_llamado) else f"{total_max_llamado:,.2f}".replace(',', '.').rstrip('0').rstrip('.')
                                    st.metric("📦 Total Máximo", total_max_str)
                                with col3:
                                    # Formatear para eliminar decimales innecesarios
                                    total_emitido_str = f"{int(total_emitido_llamado):,}".replace(',', '.') if total_emitido_llamado == int(total_emitido_llamado) else f"{total_emitido_llamado:,.2f}".replace(',', '.').rstrip('0').rstrip('.')
                                    st.metric("✅ Total Emitido", total_emitido_str)
                                with col4:
                                    # Formatear para eliminar decimales innecesarios
                                    total_saldo_str = f"{int(total_saldo_llamado):,}".replace(',', '.') if total_saldo_llamado == int(total_saldo_llamado) else f"{total_saldo_llamado:,.2f}".replace(',', '.').rstrip('0').rstrip('.')
                                    st.metric("⏳ Saldo Total", total_saldo_str)
                            else:
                                st.info(f"No se encontraron items para el llamado {selected_llamado} que coincidan con los criterios")
                    else:
                        st.info("No hay llamados disponibles que coincidan con los criterios de búsqueda")
                    
                    # Mostrar mensaje de totales mejorado
                    if not ejecucion_metricas_df.empty:
                        metricas = ejecucion_metricas_df.iloc[0]
                        total_llamados = int(metricas['total_llamados'] if metricas['total_llamados'] is not None else 0)
                        total_items = int(metricas['total_items'] if metricas['total_items'] is not None else 0)
                        total_cantidad_maxima = int(metricas['total_cantidad_maxima'] if metricas['total_cantidad_maxima'] is not None else 0)
                        total_cantidad_emitida = int(metricas['total_cantidad_emitida'] if metricas['total_cantidad_emitida'] is not None else 0)
                        total_saldo_emision = int(metricas['total_saldo_pendiente'] if metricas['total_saldo_pendiente'] is not None else 0)
                        porcentaje_avance = metricas['promedio_ejecucion'] if metricas['promedio_ejecucion'] is not None else 0
                        
                        # Formatear números con punto como separador de miles
                        total_cantidad_emitida_str = f"{total_cantidad_emitida:,}".replace(',', '.')
                        total_saldo_emision_str = f"{total_saldo_emision:,}".replace(',', '.')
                        
                        # Formatear porcentaje sin decimales innecesarios
                        porcentaje_avance_str = f"{int(porcentaje_avance)}%" if porcentaje_avance == int(porcentaje_avance) else f"{porcentaje_avance:.1f}%"
                        
                        # Mostrar mensaje según si hay filtro o no
                        if search_query:
                            st.info(
                                f"🔍 Filtrado: {total_llamados} llamados y {total_items} ítems que coinciden con '{search_query}' | " +
                                f"📈 Avance: {porcentaje_avance_str} | " +
                                f"📦 Emitido: {total_cantidad_emitida_str} | " +
                                f"⏳ Pendiente: {total_saldo_emision_str} unidades"
                            )
                        else:
                            st.info(
                                f"📋 Total: {total_llamados} llamados, {total_items} ítems | " +
                                f"📈 Avance general: {porcentaje_avance_str} | " +
                                f"📦 Emitido: {total_cantidad_emitida_str} | " +
                                f"⏳ Pendiente: {total_saldo_emision_str} unidades"
                            )
                    else:
                        st.info("No se encontraron datos de ejecución que coincidan con los criterios de búsqueda")
        
                except Exception as e:
                    st.error(f"Error al consultar ejecución: {str(e)}")
                    st.error(traceback.format_exc())

# Cerrar la función con except y finally
            except Exception as e:
                st.error(f"Error general: {str(e)}")
                st.error(traceback.format_exc())
    finally:
        # Cerrar conexión a la base de datos
        conn.close()

def verify_dashboard_data():
    """Verifica que la conexión a la base de datos funcione y que haya datos disponibles"""
    try:
        # Configuración de BD
        db_config = get_db_config()
        conn = PostgresConnection(**db_config)
        
        if not conn.connect():
            st.error("❌ No se pudo conectar a PostgreSQL. Verifica la configuración.")
            st.info("Asegúrate de que el servidor PostgreSQL esté funcionando y los datos de conexión sean correctos.")
            return False
            
        # Verificar que las tablas existan y tengan datos
        tables_to_check = {
            'siciap.stock_critico': 'stock crítico',
            'siciap.ejecucion': 'ejecución',
            'siciap.ordenes': 'órdenes de compra'
        }
        
        empty_tables = []
        
        for table, description in tables_to_check.items():
            try:
                with conn.conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    
                    if count == 0:
                        empty_tables.append(f"Tabla {description} ({table})")
            except Exception as e:
                st.error(f"Error al verificar tabla {table}: {str(e)}")
                return False
        
        if empty_tables:
            st.warning("⚠️ Las siguientes tablas están vacías:")
            for table in empty_tables:
                st.markdown(f"- {table}")
            st.info("Para que el dashboard funcione correctamente, debes importar datos primero.")
            return False
            
        # Intentar hacer una consulta de ejemplo para verificar integración
        sample_query = """
        SELECT s.codigo, s.producto, s.stock_disponible, s.dmp
        FROM siciap.stock_critico s
        WHERE s.dmp > 0
        LIMIT 1
        """
        
        try:
            result = pd.read_sql_query(text(sample_query), conn.engine)
            if result.empty:
                st.warning("⚠️ No se encontraron productos con DMP mayor a cero.")
                st.info("Para mostrar métricas de cobertura, los productos deben tener un valor de DMP.")
                return False
        except Exception as e:
            st.error(f"Error al ejecutar consulta de ejemplo: {str(e)}")
            return False
            
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error general de verificación: {str(e)}")
        return False

def diagnostico_dashboard():
    """Página para diagnosticar problemas en el dashboard"""
    st.title("Diagnóstico de Dashboard SICIAP")
    
    # Obtener configuración de la BD
    db_config = get_db_config()
    
    # Mostrar la configuración actual (sin la contraseña)
    st.subheader("1. Configuración de conexión")
    safe_config = db_config.copy()
    safe_config['password'] = '******'  # Ocultar contraseña
    st.json(safe_config)
    
    # Probar la conexión a la base de datos
    st.subheader("2. Prueba de conexión a PostgreSQL")
    
    with st.spinner("Probando conexión..."):
        try:
            conn = PostgresConnection(**db_config)
            if conn.connect():
                st.success("✅ Conexión exitosa a PostgreSQL")
                connection_ok = True
            else:
                st.error("❌ No se pudo conectar a PostgreSQL")
                connection_ok = False
        except Exception as e:
            st.error(f"❌ Error al conectar: {str(e)}")
            st.code(traceback.format_exc())
            connection_ok = False
    
    # Si la conexión es exitosa, verificar esquemas y tablas
    if connection_ok:
        st.subheader("3. Verificación de esquemas y tablas")
        
        with st.spinner("Verificando esquemas..."):
            try:
                with conn.conn.cursor() as cursor:
                    # Verificar esquema siciap
                    cursor.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name = 'siciap'
                    """)
                    
                    if cursor.fetchone():
                        st.success("✅ Esquema 'siciap' existe")
                        schema_ok = True
                    else:
                        st.error("❌ El esquema 'siciap' no existe")
                        schema_ok = False
            except Exception as e:
                st.error(f"❌ Error al verificar esquema: {str(e)}")
                st.code(traceback.format_exc())
                schema_ok = False
        
        # Si el esquema existe, verificar tablas
        if schema_ok:
            with st.spinner("Verificando tablas..."):
                try:
                    # Tablas a verificar
                    tablas = {
                        'siciap.stock_critico': 'Stock Crítico',
                        'siciap.ordenes': 'Órdenes',
                        'siciap.ejecucion': 'Ejecución',
                        'siciap.datosejecucion': 'Datos de Ejecución'
                    }
                    
                    tabla_col1, tabla_col2 = st.columns(2)
                    
                    # Verificar cada tabla
                    for i, (tabla, nombre) in enumerate(tablas.items()):
                        col = tabla_col1 if i % 2 == 0 else tabla_col2
                        
                        with col:
                            with st.expander(f"Tabla: {nombre}"):
                                # 1. Verificar si la tabla existe
                                try:
                                    schema, table = tabla.split('.')
                                    with conn.conn.cursor() as cursor:
                                        cursor.execute(f"""
                                            SELECT EXISTS (
                                                SELECT FROM information_schema.tables 
                                                WHERE table_schema = '{schema}' AND table_name = '{table}'
                                            )
                                        """)
                                        
                                        if cursor.fetchone()[0]:
                                            st.success(f"✅ Tabla '{tabla}' existe")
                                            
                                            # 2. Verificar conteo de registros
                                            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                                            count = cursor.fetchone()[0]
                                            
                                            if count > 0:
                                                st.success(f"✅ {count} registros encontrados")
                                                
                                                # 3. Obtener y mostrar columnas
                                                cursor.execute(f"""
                                                    SELECT column_name, data_type 
                                                    FROM information_schema.columns 
                                                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                                                    ORDER BY ordinal_position
                                                """)
                                                
                                                columns = cursor.fetchall()
                                                
                                                if columns:
                                                    st.write("Columnas disponibles:")
                                                    column_df = pd.DataFrame(columns, columns=['Columna', 'Tipo'])
                                                    st.dataframe(column_df)
                                                    
                                                    # 4. Mostrar muestra de datos
                                                    cursor.execute(f"SELECT * FROM {tabla} LIMIT 3")
                                                    sample_data = cursor.fetchall()
                                                    
                                                    if sample_data:
                                                        # Obtener nombres de columnas
                                                        col_names = [desc[0] for desc in cursor.description]
                                                        
                                                        # Crear DataFrame
                                                        sample_df = pd.DataFrame(sample_data, columns=col_names)
                                                        
                                                        st.write("Muestra de datos:")
                                                        st.dataframe(sample_df)
                                                    else:
                                                        st.warning("⚠️ No se pudo obtener muestra de datos")
                                                else:
                                                    st.warning("⚠️ No se pudo obtener información de columnas")
                                            else:
                                                st.warning(f"⚠️ La tabla '{tabla}' está vacía")
                                        else:
                                            st.error(f"❌ La tabla '{tabla}' no existe")
                                except Exception as e:
                                    st.error(f"❌ Error al verificar tabla '{tabla}': {str(e)}")
                                    st.code(traceback.format_exc())
                except Exception as e:
                    st.error(f"❌ Error general al verificar tablas: {str(e)}")
                    st.code(traceback.format_exc())
        
        # Verificar la consulta principal del dashboard
        st.subheader("4. Prueba de consulta principal")
        
        with st.spinner("Ejecutando consulta principal..."):
            try:
                # Versión simplificada de la consulta principal
                consulta_test = """
                SELECT 
                    s.codigo,
                    s.producto,
                    s.stock_disponible,
                    s.dmp,
                    CASE 
                        WHEN s.dmp IS NULL OR s.dmp = 0 THEN 'Sin DMP'
                        WHEN s.stock_disponible IS NULL THEN 'Sin Stock'
                        WHEN s.stock_disponible < s.dmp * 0.3 THEN 'Atención'
                        WHEN s.stock_disponible < s.dmp * 0.7 THEN 'Precaución'
                        ELSE 'Óptimo'
                    END AS nivel_stock
                FROM 
                    siciap.stock_critico s
                LIMIT 5
                """
                
                st.code(consulta_test, language="sql")
                
                # Ejecutar la consulta
                resultados = pd.read_sql_query(text(consulta_test), conn.engine)
                
                if not resultados.empty:
                    st.success(f"✅ Consulta ejecutada correctamente. {len(resultados)} resultados obtenidos.")
                    st.dataframe(resultados)
                else:
                    st.warning("⚠️ La consulta no devolvió resultados.")
            except Exception as e:
                st.error(f"❌ Error al ejecutar consulta: {str(e)}")
                st.code(traceback.format_exc())
        
        # Probar la consulta completa con joins
        st.subheader("5. Prueba de consulta con joins")
        
        with st.spinner("Ejecutando consulta con joins..."):
            try:
                consulta_joins = """
                SELECT 
                    s.codigo,
                    s.producto,
                    COUNT(DISTINCT o.oc) AS total_ordenes,
                    COUNT(DISTINCT e.id_llamado) AS total_contratos
                FROM 
                    siciap.stock_critico s
                LEFT JOIN
                    siciap.ordenes o ON s.codigo = o.codigo
                LEFT JOIN
                    siciap.ejecucion e ON s.codigo = e.codigo
                GROUP BY
                    s.codigo, s.producto
                LIMIT 5
                """
                
                st.code(consulta_joins, language="sql")
                
                # Ejecutar la consulta
                resultados_joins = pd.read_sql_query(text(consulta_joins), conn.engine)
                
                if not resultados_joins.empty:
                    st.success(f"✅ Consulta con joins ejecutada correctamente. {len(resultados_joins)} resultados obtenidos.")
                    st.dataframe(resultados_joins)
                else:
                    st.warning("⚠️ La consulta con joins no devolvió resultados.")
            except Exception as e:
                st.error(f"❌ Error al ejecutar consulta con joins: {str(e)}")
                st.code(traceback.format_exc())
        
        # Cerrar la conexión
        conn.close()
    
    # Mostrar posibles soluciones
    st.subheader("Posibles soluciones")
    
    st.markdown("""
    Basado en los resultados del diagnóstico, aquí hay algunas posibles soluciones:
    
    1. **Si hay problemas de conexión:**
       - Verifica que el servidor PostgreSQL esté en ejecución
       - Comprueba los datos de conexión (host, puerto, usuario, contraseña)
       - Asegúrate de que el usuario tenga permisos suficientes
    
    2. **Si faltan esquemas o tablas:**
       - Ejecuta el código para crear el esquema y las tablas
       - Verifica que los nombres sean correctos y coincidan con los utilizados en la aplicación
    
    3. **Si las tablas están vacías:**
       - Importa los datos usando las funciones de importación
       - Verifica que los archivos Excel tengan el formato correcto
    
    4. **Si hay errores en las consultas:**
       - Revisa los mensajes de error específicos
       - Verifica que los nombres de columnas existan y sean correctos
       - Prueba versiones más simples de las consultas para aislar el problema
    """)

# Clases específicas para cada funcionalidad
class OrdenesProcessor:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def create_table_with_schema(self, table_name):
        """Crea una tabla para órdenes si no existe"""
        try:
            # Verificar si la tabla tiene esquema
            if '.' in table_name:
                schema, table = table_name.split('.')
                
                # Crear esquema si no existe
                with self.db_connection.conn.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    logger.info(f"Esquema verificado/creado: {schema}")

                # Construir SQL para crear la tabla usando los nombres correctos
                columns_sql = []
                for col in ORDENES_REQUIRED_COLUMNS:
                    col_name = clean_column_name(col)
                    col_type = ORDENES_COLUMN_TYPES.get(col_name, "VARCHAR(1000)")
                    columns_sql.append(f"{col_name} {col_type}")

                # Añadir ID como clave primaria
                columns_sql.append("id SERIAL PRIMARY KEY")

                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    {', '.join(columns_sql)}
                )
                """

                # Crear la tabla
                with self.db_connection.conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    logger.info(f"Tabla verificada/creada: {schema}.{table}")

                return True
            else:
                logger.error("El nombre de la tabla debe incluir un esquema (esquema.tabla)")
                return False
        except Exception as e:
            logger.error(f"Error al crear tabla de órdenes: {str(e)}")
            return False

    def map_excel_to_required_columns(self, df, file_name):
        """
        Mapea las columnas del Excel a las columnas requeridas para Órdenes
        con mayor tolerancia a variaciones en nombres
        """
        logger.info(f"Mapeando columnas para {file_name}...")
        
        # Crear DataFrame con las columnas requeridas
        mapped_df = pd.DataFrame()
        
        # Limpiar nombres de columnas del DataFrame original
        df.columns = [str(col).strip() if isinstance(col, str) else str(col) for col in df.columns]
        
        # Log para depuración
        logger.info(f"Columnas originales: {', '.join(df.columns.tolist())}")

        # Mapeo explícito de columnas Excel a PostgreSQL - ACTUALIZADO con mayor flexibilidad
        excel_to_postgres_map = {
            # MAPEOS PRINCIPALES - ACTUALIZADO con columnas identificadas
            'Id.Llamado': 'id_llamado',
            'Llamado': 'llamado',
            'P.Unit.': 'p_unit',
            'Fec.Contrato': 'fec_contrato',
            'OC': 'oc',
            'Item': 'item',
            'Codigo': 'codigo',
            'Producto': 'producto',
            'Cant. OC': 'cant_oc',
            'Monto OC': 'monto_oc',
            'Monto Recepciòn': 'monto_recepcion',
            'Monto Recepcion': 'monto_recepcion',
            'Monto Recepción': 'monto_recepcion',
            'Cant. Recep.': 'cant_recep',
            'Monto Saldo': 'monto_saldo',
            'Dias de Atraso': 'dias_de_atraso',
            'Estado': 'estado',
            'Stock': 'stock',
            'Referencia': 'referencia',
            'Proveedor': 'proveedor',
            'Lugar Entrega OC': 'lugar_entrega_oc',
            'Lugar Entrega': 'lugar_entrega_oc',
            'Fec. Ult. Recep.': 'fec_ult_recep',
            'Fecha Recibido Poveedor': 'fecha_recibido_proveedor',
            'Fecha Recibido Proveedor': 'fecha_recibido_proveedor',
            'Fecha OC': 'fecha_oc',
            'Saldo': 'saldo',
            'Plazo Entrega': 'plazo_entrega',
            'Tipo Vigencia': 'tipo_vigencia',
            'Vigencia': 'vigencia',
            'Det. Recep.': 'det_recep'
        }

        # Crear un mapeo normalizado para comparación insensible a mayúsculas/minúsculas y espacios
        normalized_map = {}
        for excel_name, db_name in excel_to_postgres_map.items():
            normalized_key = excel_name.lower().replace(' ', '').replace('.', '').replace('_', '')
            normalized_map[normalized_key] = db_name

        # Mapear columnas Excel a PostgreSQL con tres niveles de búsqueda
        for excel_col in df.columns:
            # 1. Búsqueda directa por nombre exacto
            if excel_col in excel_to_postgres_map:
                postgres_col = excel_to_postgres_map[excel_col]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (exacta)")
                continue
            
            # 2. Búsqueda por normalización
            excel_norm = excel_col.lower().replace(' ', '').replace('.', '').replace('_', '')
            if excel_norm in normalized_map:
                postgres_col = normalized_map[excel_norm]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (normalizada)")
                continue
            
            # 3. Búsqueda por similitud parcial
            matched = False
            for excel_key_norm, postgres_name in normalized_map.items():
                # Comprobar si una es substring de la otra
                if excel_norm in excel_key_norm or excel_key_norm in excel_norm:
                    mapped_df[postgres_name] = df[excel_col].copy()
                    logger.info(f"Columna '{excel_col}' mapeada a '{postgres_name}' (similitud)")
                    matched = True
                    break
            
            if not matched:
                logger.warning(f"No se encontró mapeo para la columna '{excel_col}'")

        # Verificar todas las columnas requeridas y llenar con NULL si no existen
        for postgres_col in set(excel_to_postgres_map.values()):
            if postgres_col not in mapped_df.columns:
                mapped_df[postgres_col] = None
                logger.warning(f"Columna requerida '{postgres_col}' no encontrada, se usará NULL")

        # Convertir tipos de datos con manejo mejorado de errores
        for col in mapped_df.columns:
            col_type = ORDENES_COLUMN_TYPES.get(col, "VARCHAR(1000)")
            
            try:
                if "NUMERIC" in col_type:
                    # Limpiar primero si son strings
                    if mapped_df[col].dtype == object:
                        # Reemplazar comas por puntos en valores numéricos
                        mapped_df[col] = mapped_df[col].astype(str).str.replace(',', '.', regex=False)
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce')
                elif "INTEGER" in col_type or "BIGINT" in col_type:
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce')
                elif "DATE" in col_type:
                    # Usar función segura para conversión de fechas
                    mapped_df[col] = safe_date_conversion(mapped_df[col])
            except Exception as e:
                logger.warning(f"Error al convertir columna {col} a {col_type}: {str(e)}")

        return mapped_df

    def process_excel_file(self, file_content, file_name, table_name):
        """Procesa un archivo Excel de órdenes y lo importa a PostgreSQL"""
        try:
            # Leer Excel
            logger.info(f"Leyendo archivo {file_name}...")
            df = read_excel_robust(file_content, file_name)
            
            if df is None:
                st.error(f"No se pudo leer el archivo {file_name}")
                return False

            # Eliminar filas completamente vacías
            df = df.dropna(how='all')
            
            # Crear tabla con el esquema requerido
            if not self.create_table_with_schema(table_name):
                return False

            # Mapear columnas del Excel a las columnas requeridas
            mapped_df = self.map_excel_to_required_columns(df, file_name)
            
            schema, table = table_name.split('.')
            
            # Verificar si hay datos para importar
            if mapped_df.empty:
                logger.error("No hay datos para importar después del mapeo de columnas")
                return False

            # En lugar de truncar, usamos DELETE + INSERT en una transacción
            with self.db_connection.conn.cursor() as cursor:
                # Comenzar transacción
                self.db_connection.conn.autocommit = False
                try:
                    # Modificación: Usar DELETE FROM en lugar de TRUNCATE
                    cursor.execute(f"DELETE FROM {schema}.{table}")
                    logger.info(f"Tabla {table_name} vaciada correctamente")
                    
                    # Importar los nuevos datos
                    for i, row in mapped_df.iterrows():
                        placeholders = ", ".join(["%s"] * len(row))
                        columns = ", ".join([f'"{col}"' for col in mapped_df.columns])
                        insert_sql = f"""
                        INSERT INTO {schema}.{table} ({columns})
                        VALUES ({placeholders})
                        """
                        # Convertir NaN a None
                        values = [None if pd.isna(val) else val for val in row]
                        cursor.execute(insert_sql, values)

                    # Confirmar transacción
                    self.db_connection.conn.commit()
                    logger.info(f"Datos importados correctamente. {len(mapped_df)} filas.")
                    return True
                    
                except Exception as e:
                    # Revertir cambios en caso de error
                    self.db_connection.conn.rollback()
                    logger.error(f"Error al importar datos: {str(e)}")
                    logger.error(traceback.format_exc())
                    return False
                finally:
                    # Restaurar autocommit
                    self.db_connection.conn.autocommit = True

        except Exception as e:
            logger.error(f"Error al procesar {file_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

class EjecucionProcessor:
    def __init__(self, db_conn=None):
        self.db_conn = db_conn

    def process_excel_file(self, file_content, file_name, table_name):
        """Procesa un archivo Excel - Este es el método que tu código actual está llamando"""
        # Simplemente delegamos al método process_file
        return self.process_file(file_path=file_name, file_content=file_content, conn=self.db_conn)

    def process_file(self, file_path=None, file_content=None, conn=None):
        """Procesa un archivo de ejecución"""
        try:
            # Verificar si tenemos conexión a la base de datos
            if conn is None:
                conn = self.db_conn if self.db_conn else get_db_connection()

            # Si el archivo se llama específicamente "ejecucion.xlsx", usar método especial
            if file_path and "ejecucion.xlsx" in file_path.lower():
                return self.procesar_ejecucion_xlsx(file_content, conn)

            # Método estándar para otros archivos
            logger.info(f"Leyendo archivo {file_path}...")
            logger.info(f"Intentando leer el archivo {file_path} con métodos robustos...")
            
            if file_content is None:
                logger.error("No se proporcionó contenido de archivo")
                return False
            
            logger.info("Recibido contenido de archivo en memoria")

            # Intentar leer el archivo Excel
            try:
                logger.info("Intentando método 1 para leer Excel...")
                buffer = BytesIO(file_content)
                df = pd.read_excel(buffer)
                # Limpiar el DataFrame
                df = self.limpiar_dataframe(df)
                logger.info(f"Limpieza completada: {len(df)} filas, {len(df.columns)} columnas")
                logger.info(f"EXITO con método 1. Encontradas {len(df.columns)} columnas y {len(df)} filas.")
                
                # Crear o verificar el esquema y la tabla
                self.crear_esquema_y_tabla(conn, 'siciap', 'ejecucion')
                
                # Mapear columnas
                df_mapped = self.map_excel_to_required_columns(df, file_path)
                
                # VERIFICAR QUE SOLO INSERTEMOS COLUMNAS QUE EXISTEN EN LA TABLA
                tabla_columnas = self.get_table_columns(conn, 'siciap', 'ejecucion')
                df_final = self.filter_valid_columns(df_mapped, tabla_columnas)
                
                # En lugar de truncar, usamos DELETE + INSERT en una transacción
                if isinstance(conn, PostgresConnection):
                    with conn.conn.cursor() as cursor:
                        # Comenzar transacción
                        conn.conn.autocommit = False
                        try:
                            # Modificación: Usar DELETE FROM en lugar de TRUNCATE
                            cursor.execute("DELETE FROM siciap.ejecucion")
                            logger.info("Tabla siciap.ejecucion vaciada correctamente")
                            
                            # Importar los nuevos datos
                            for i, row in df_final.iterrows():
                                # CREAR LA CONSULTA DINÁMICAMENTE CON SOLO LAS COLUMNAS QUE TENEMOS
                                columns_list = list(df_final.columns)
                                placeholders = ", ".join(["%s"] * len(columns_list))
                                columns_str = ", ".join([f'"{col}"' for col in columns_list])
                                
                                insert_sql = f"""
                                INSERT INTO siciap.ejecucion ({columns_str})
                                VALUES ({placeholders})
                                """
                                
                                # Convertir NaN a None
                                values = [None if pd.isna(val) else val for val in row]
                                
                                # Log para depuración (solo primera fila)
                                if i == 0:
                                    logger.info(f"SQL de inserción: {insert_sql}")
                                    logger.info(f"Columnas a insertar: {columns_list}")
                                    logger.info(f"Valores de ejemplo: {values[:5]}...")
                                
                                cursor.execute(insert_sql, values)

                            # Confirmar transacción
                            conn.conn.commit()
                            logger.info(f"Datos importados correctamente. {len(df_final)} filas.")

                            # SINCRONIZACIÓN AUTOMÁTICA - Preservar datos de datosejecucion
                            if sincronizar_datosejecucion(conn):
                                logger.info("✅ Sincronización automática completada")
                            else:
                                logger.warning("⚠️ Sincronización tuvo problemas, revisar logs")

                            return True
                            
                        except Exception as e:
                            # Revertir cambios en caso de error
                            conn.conn.rollback()
                            logger.error(f"Error al importar datos: {str(e)}")
                            logger.error(traceback.format_exc())
                            return False
                        finally:
                            # Restaurar autocommit
                            conn.conn.autocommit = True
                else:
                    # Si no es un objeto PostgresConnection, intentar con el método anterior
                    # Pero esto es menos seguro con las restricciones de clave foránea
                    return self.importar_por_lotes(conn, 'siciap.ejecucion', df_final)
                    
            except Exception as e:
                logger.error(f"Error al leer archivo Excel: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"Error al procesar archivo de ejecución: {str(e)}")
            return False
        finally:
            # Cerrar la conexión solo si no se proporcionó externamente
            if conn and conn != self.db_conn:
                conn.close()
                logger.info("Conexión a PostgreSQL cerrada")

                

    def get_table_columns(self, conn, schema, table):
        """Obtiene las columnas que realmente existen en la tabla"""
        try:
            if isinstance(conn, PostgresConnection):
                with conn.conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s
                    """, (schema, table))
                    
                    columns = [row[0] for row in cursor.fetchall()]
                    logger.info(f"Columnas existentes en {schema}.{table}: {columns}")
                    return columns
            else:
                # Para otros tipos de conexión
                query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                """
                result = pd.read_sql_query(query, conn)
                columns = result['column_name'].tolist()
                logger.info(f"Columnas existentes en {schema}.{table}: {columns}")
                return columns
        except Exception as e:
            logger.error(f"Error al obtener columnas de la tabla: {str(e)}")
            # Retornar las columnas básicas como fallback
            return ['id_llamado', 'licitacion', 'proveedor', 'codigo', 'medicamento', 
                   'item', 'cantidad_maxima', 'cantidad_emitida', 'cantidad_recepcionada',
                   'cantidad_distribuida', 'monto_adjudicado', 'monto_emitido', 'saldo',
                   'porcentaje_emitido', 'ejecucion_mayor_al_50', 'estado_stock',
                   'estado_contrato', 'cantidad_ampliacion', 'porcentaje_ampliado',
                   'porcentaje_ampliacion_emitido', 'obs']

    def filter_valid_columns(self, df, valid_columns):
        """Filtra el DataFrame para incluir solo columnas válidas"""
        try:
            # Crear DataFrame con solo las columnas que existen en la tabla
            valid_df_columns = []
            for col in df.columns:
                if col in valid_columns:
                    valid_df_columns.append(col)
                    logger.info(f"Columna '{col}' será incluida")
                else:
                    logger.warning(f"Columna '{col}' NO existe en la tabla, se omitirá")
            
            if not valid_df_columns:
                logger.error("No hay columnas válidas para insertar")
                return pd.DataFrame()
            
            # Crear DataFrame filtrado
            filtered_df = df[valid_df_columns].copy()
            logger.info(f"DataFrame filtrado creado con {len(filtered_df.columns)} columnas válidas")
            
            return filtered_df
        except Exception as e:
            logger.error(f"Error al filtrar columnas válidas: {str(e)}")
            return pd.DataFrame()

    def procesar_ejecucion_xlsx(self, file_content, conn):
        """Procesa específicamente el archivo ejecucion.xlsx con formato especial"""
        logger.info("Procesando archivo ejecucion.xlsx con método especializado...")
        try:
            # Crear buffer de memoria con el contenido del archivo
            if isinstance(file_content, BytesIO):
                # Si ya es un objeto BytesIO, usar directamente
                buffer = file_content
                # Asegurar que el puntero está al inicio del archivo
                buffer.seek(0)
            else:
                # Si es bytes crudos, crear un nuevo BytesIO
                buffer = BytesIO(file_content)

            # Leer el Excel SIN ESPECIFICAR HEADER para examinar manualmente
            df_raw = pd.read_excel(buffer, header=None)
            
            # Buscar la fila que contiene los encabezados
            header_row = None
            for i in range(min(10, len(df_raw))):
                row = df_raw.iloc[i]
                # Convertir fila a string para buscar patrones
                row_str = ' '.join([str(x) for x in row if pd.notna(x)])
                logger.info(f"Fila {i}: {row_str[:100]}...")  # Mostrar primeros 100 caracteres
                
                # Verificar si esta fila contiene encabezados comunes
                if ('Id.Llamado' in row_str and 'Licitación' in row_str and 'Proveedor' in row_str):
                    header_row = i
                    logger.info(f"¡Fila de encabezados encontrada en posición {i}!")
                    break
            
            if header_row is None:
                logger.error("No se pudo encontrar fila de encabezados")
                return False

            # Crear nombres de columnas manualmente desde la fila de encabezados
            headers = []
            for val in df_raw.iloc[header_row]:
                if pd.isna(val):
                    headers.append('Unknown')
                else:
                    headers.append(str(val).strip())

            # Crear un nuevo DataFrame usando estos encabezados y los datos desde la fila siguiente
            df = pd.DataFrame(df_raw.iloc[header_row+1:].values, columns=headers)
            
            # Mapeo de nombres de columnas a nombres de la base de datos
            column_map = {
                'Id.Llamado': 'id_llamado',
                'Licitación': 'licitacion',
                'Proveedor': 'proveedor',
                'Codigo': 'codigo',
                'Medicamento': 'medicamento',
                'Item': 'item',
                'Cantidad Maxima': 'cantidad_maxima',
                'Cantidad Emitida': 'cantidad_emitida',
                'Cantidad Recepcionada': 'cantidad_recepcionada',
                'Cantidad Distribuida': 'cantidad_distribuida',
                'Monto Adjudicado': 'monto_adjudicado',
                'Monto Emitido': 'monto_emitido',
                'Saldo': 'saldo',
                'Porcentaje Emitido': 'porcentaje_emitido',
                'Ejecución Mayor al 50%': 'ejecucion_mayor_al_50',
                'Estado Stock': 'estado_stock',
                'Estado Contrato': 'estado_contrato',
                'Cantidad Ampliacion': 'cantidad_ampliacion',
                'Porcentaje Ampliado': 'porcentaje_ampliado',
                'Porcentaje Ampliacion Emitido': 'porcentaje_ampliacion_emitido',
                'Obs.': 'obs'
            }
            
            # Renombrar columnas (solo las que existen)
            rename_map = {}
            for excel_col, db_col in column_map.items():
                if excel_col in df.columns:
                    rename_map[excel_col] = db_col
                    logger.info(f"Mapeo encontrado: {excel_col} → {db_col}")

            df = df.rename(columns=rename_map)

            # Verificar qué columnas faltan
            db_columns = set(column_map.values())
            existing_columns = set(df.columns) & db_columns
            missing_columns = db_columns - existing_columns
            
            logger.info(f"Columnas mapeadas correctamente: {len(existing_columns)}")
            logger.info(f"Columnas faltantes: {len(missing_columns)} - {missing_columns}")
            
            # Agregar columnas faltantes
            for col in missing_columns:
                df[col] = None
                logger.info(f"Agregada columna faltante: {col}")

            # OBTENER LAS COLUMNAS QUE REALMENTE EXISTEN EN LA TABLA
            tabla_columnas = self.get_table_columns(conn, 'siciap', 'ejecucion')
            df_final = self.filter_valid_columns(df, tabla_columnas)

            # Eliminar filas que parecen encabezados o valores no válidos
            try:
                # Convertir id_llamado a numérico para filtrar
                df_final['id_llamado_numeric'] = pd.to_numeric(df_final['id_llamado'], errors='coerce')
                # Eliminar filas donde id_llamado no es un número
                rows_before = len(df_final)
                df_final = df_final[~df_final['id_llamado_numeric'].isna()]
                rows_after = len(df_final)
                logger.info(f"Filas eliminadas (encabezados/no válidas): {rows_before - rows_after}")
                # Eliminar columna temporal
                df_final = df_final.drop(columns=['id_llamado_numeric'])
            except Exception as e:
                logger.warning(f"Error al filtrar filas no válidas: {str(e)}")
            
            # Convertir tipos de datos
            numeric_columns = [
                ('id_llamado', 'int'), 
                ('item', 'int'),
                ('cantidad_maxima', 'float'),
                ('cantidad_emitida', 'float'),
                ('cantidad_recepcionada', 'float'),
                ('cantidad_distribuida', 'float'),
                ('monto_adjudicado', 'float'),
                ('monto_emitido', 'float'),
                ('saldo', 'float'),
                ('porcentaje_emitido', 'float'),
                ('cantidad_ampliacion', 'float'),
                ('porcentaje_ampliado', 'float'),
                ('porcentaje_ampliacion_emitido', 'float')
            ]
            
            for col, dtype in numeric_columns:
                if col in df_final.columns:
                    try:
                        if dtype == 'int':
                            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)
                        else:  # float
                            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
                        logger.info(f"Convertida columna {col} a {dtype}")
                    except Exception as e:
                        logger.warning(f"Error al convertir {col} a {dtype}: {str(e)}")
            
            # Verificar datos finales
            logger.info(f"Total de filas procesadas: {len(df_final)}")
            
            if len(df_final) == 0:
                logger.error("No hay datos para importar después del procesamiento")
                return False

            # Truncar tabla existente
            try:
                # Verificar qué tipo de conexión tenemos
                if isinstance(conn, PostgresConnection):
                    # Es un objeto PostgresConnection
                    with conn.conn.cursor() as cursor:
                        cursor.execute("DELETE FROM siciap.ejecucion")
                        conn.conn.commit()
                        logger.info("Tabla siciap.ejecucion truncada correctamente usando conexión PostgreSQL")
                elif hasattr(conn, 'execute'):
                    # SQLAlchemy connection
                    from sqlalchemy import text
                    conn.execute(text("DELETE FROM siciap.ejecucion"))
                    logger.info("Tabla siciap.ejecucion truncada correctamente usando SQLAlchemy")
                elif hasattr(conn, 'cursor'):
                    # Psycopg2 connection
                    with conn.cursor() as cursor:
                        cursor.execute("DELETE FROM siciap.ejecucion")
                        conn.commit()
                        logger.info("Tabla siciap.ejecucion truncada correctamente usando cursor")
                else:
                    logger.warning("Tipo de conexión no reconocido, no se pudo truncar la tabla")
            except Exception as e:
                logger.error(f"Error al truncar tabla: {str(e)}")
                # Continuar con la importación incluso si no se pudo truncar

            # Importar datos
            try:
                # Verificar qué tipo de conexión tenemos y usar la apropiada
                if isinstance(conn, PostgresConnection):
                    engine = conn.engine
                else:
                    engine = conn
                
                # Usar método to_sql de pandas
                df_final.to_sql('ejecucion', con=engine, schema='siciap', if_exists='append', index=False)
                logger.info(f"Datos importados correctamente: {len(df_final)} filas")
                return True
            except Exception as e:
                logger.error(f"Error al importar con to_sql: {str(e)}")
                try:
                    # Intentar importar por lotes
                    return self.importar_por_lotes_flexible(conn, 'siciap.ejecucion', df_final)
                except Exception as batch_e:
                    logger.error(f"Error al importar por lotes: {str(batch_e)}")
                    return False
        except Exception as e:
            logger.error(f"Error al procesar archivo de ejecución: {str(e)}")
            return False

    def map_excel_to_required_columns(self, df, file_name):
        """
        Mapea las columnas del Excel a las columnas requeridas para Ejecución
        con mayor tolerancia a variaciones en nombres
        """
        logger.info(f"Mapeando columnas para {file_name}...")
        
        # Crear DataFrame con las columnas requeridas
        mapped_df = pd.DataFrame()
        
        # Limpiar nombres de columnas del DataFrame original
        df.columns = [str(col).strip() if isinstance(col, str) else str(col) for col in df.columns]
        
        # Log para depuración
        logger.info(f"Columnas originales: {', '.join(df.columns.tolist())}")

        # Mapeo explícito de columnas Excel a PostgreSQL
        excel_to_postgres_map = {
            'Id.Llamado': 'id_llamado',
            'Licitación': 'licitacion',
            'Proveedor': 'proveedor',
            'Codigo': 'codigo',
            'Medicamento': 'medicamento',
            'Item': 'item',
            'Cantidad Maxima': 'cantidad_maxima',
            'Cantidad Emitida': 'cantidad_emitida',
            'Cantidad Recepcionada': 'cantidad_recepcionada',
            'Cantidad Distribuida': 'cantidad_distribuida',
            'Monto Adjudicado': 'monto_adjudicado',
            'Monto Emitido': 'monto_emitido',
            'Saldo': 'saldo',
            'Porcentaje Emitido': 'porcentaje_emitido',
            'Ejecución Mayor al 50%': 'ejecucion_mayor_al_50',
            'Estado Stock': 'estado_stock',
            'Estado Contrato': 'estado_contrato',
            'Cantidad Ampliacion': 'cantidad_ampliacion',
            'Porcentaje Ampliado': 'porcentaje_ampliado',
            'Porcentaje Ampliacion Emitido': 'porcentaje_ampliacion_emitido',
            'Obs.': 'obs'
        }
        
        # Crear un mapeo normalizado para comparación insensible a mayúsculas/minúsculas y espacios
        normalized_map = {}
        for excel_name, db_name in excel_to_postgres_map.items():
            normalized_key = excel_name.lower().replace(' ', '').replace('.', '').replace('_', '')
            normalized_map[normalized_key] = db_name
        
        # Mapear columnas Excel a PostgreSQL con tres niveles de búsqueda
        for excel_col in df.columns:
            # 1. Búsqueda directa por nombre exacto
            if excel_col in excel_to_postgres_map:
                postgres_col = excel_to_postgres_map[excel_col]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (exacta)")
                continue
            
            # 2. Búsqueda por normalización
            excel_norm = excel_col.lower().replace(' ', '').replace('.', '').replace('_', '')
            if excel_norm in normalized_map:
                postgres_col = normalized_map[excel_norm]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (normalizada)")
                continue
            
            # 3. Búsqueda por similitud parcial
            matched = False
            for excel_key_norm, postgres_name in normalized_map.items():
                # Comprobar si una es substring de la otra
                if excel_norm in excel_key_norm or excel_key_norm in excel_norm:
                    mapped_df[postgres_name] = df[excel_col].copy()
                    logger.info(f"Columna '{excel_col}' mapeada a '{postgres_name}' (similitud)")
                    matched = True
                    break
            
            if not matched:
                logger.warning(f"No se encontró mapeo para la columna '{excel_col}'")
        
        # Verificar todas las columnas requeridas y llenar con NULL si no existen
        required_columns = [
            'id_llamado', 'licitacion', 'proveedor', 'codigo', 'medicamento', 
            'item', 'cantidad_maxima', 'cantidad_emitida', 'cantidad_recepcionada',
            'cantidad_distribuida', 'monto_adjudicado', 'monto_emitido', 'saldo',
            'porcentaje_emitido', 'ejecucion_mayor_al_50', 'estado_stock',
            'estado_contrato', 'cantidad_ampliacion', 'porcentaje_ampliado',
            'porcentaje_ampliacion_emitido', 'obs'
        ]
        
        for postgres_col in required_columns:
            if postgres_col not in mapped_df.columns:
                mapped_df[postgres_col] = None
                logger.warning(f"Columna requerida '{postgres_col}' no encontrada, se usará NULL")
        
        # Convertir tipos de datos con manejo mejorado de errores
        for col in mapped_df.columns:
            col_type = EJECUCION_COLUMN_TYPES.get(col, "VARCHAR(255)")
            
            try:
                if "NUMERIC" in col_type:
                    # Limpiar primero si son strings
                    if mapped_df[col].dtype == object:
                        # Reemplazar comas por puntos en valores numéricos
                        mapped_df[col] = mapped_df[col].astype(str).str.replace(',', '.', regex=False)
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce')
                elif "INTEGER" in col_type:
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Error al convertir columna {col} a {col_type}: {str(e)}")

        return mapped_df

    def limpiar_dataframe(self, df):
        """
        Limpia el DataFrame: elimina filas completamente vacías, etc.
        """
        # Eliminar filas completamente vacías
        df = df.dropna(how='all')
        # Eliminar columnas completamente vacías
        df = df.dropna(axis=1, how='all')
        return df

    def crear_esquema_y_tabla(self, conn, schema, tabla):
        """
        Crea el esquema y la tabla si no existen
        """
        try:
            cursor = conn.cursor()
            
            # Crear esquema si no existe
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.commit()
            logger.info(f"Esquema verificado/creado: {schema}")
            
            # Crear tabla si no existe
            if tabla == 'ejecucion':
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{tabla} (
                        id SERIAL PRIMARY KEY,
                        id_llamado INTEGER,
                        licitacion VARCHAR(255),
                        proveedor VARCHAR(255),
                        codigo VARCHAR(50),
                        medicamento TEXT,
                        item INTEGER,
                        cantidad_maxima NUMERIC,
                        cantidad_emitida NUMERIC,
                        cantidad_recepcionada NUMERIC,
                        cantidad_distribuida NUMERIC,
                        monto_adjudicado NUMERIC,
                        monto_emitido NUMERIC,
                        saldo NUMERIC,
                        porcentaje_emitido NUMERIC,
                        ejecucion_mayor_al_50 VARCHAR(10),
                        estado_stock VARCHAR(50),
                        estado_contrato VARCHAR(50),
                        cantidad_ampliacion NUMERIC,
                        porcentaje_ampliado NUMERIC,
                        porcentaje_ampliacion_emitido NUMERIC,
                        obs TEXT
                    )
                    """)
                conn.commit()
                logger.info(f"Tabla verificada/creada: {schema}.{tabla}")
            
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Error al crear esquema/tabla: {str(e)}")
            conn.rollback()
            return False

    def importar_por_lotes_flexible(self, conn, tabla, df, batch_size=100):
        """
        Importa datos por lotes de manera flexible, compatibilidad con diferentes tipos de conexiones
        """
        total_rows = len(df)
        total_batches = (total_rows + batch_size - 1) // batch_size
        rows_imported = 0
        
        try:
            for i in range(0, total_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                try:
                    # Intentar importar el lote con to_sql que es más flexible
                    batch_df.to_sql(tabla.split('.')[-1], con=conn.engine if hasattr(conn, 'engine') else conn, 
                                   schema=tabla.split('.')[0], 
                                   if_exists='append', index=False)
                    
                    rows_imported += len(batch_df)
                    logger.info(f"Insertado lote {i+1}/{total_batches} ({len(batch_df)} filas)")
                    
                except Exception as e:
                    logger.error(f"Error al insertar lote: {str(e)}")
                    
                    # Intentar insertar fila por fila en caso de error en el lote
                    logger.info("Intentando insertar fila por fila para el lote con error...")
                    batch_success = 0
                    
                    for _, row in batch_df.iterrows():
                        try:
                            # Crear un DataFrame de una sola fila
                            row_df = pd.DataFrame([row])
                            
                            # Usar to_sql para insertar una sola fila
                            row_df.to_sql(tabla.split('.')[-1], con=conn.engine if hasattr(conn, 'engine') else conn, 
                                         schema=tabla.split('.')[0], 
                                         if_exists='append', index=False)
                            
                            batch_success += 1
                            
                        except Exception as row_error:
                            logger.error(f"Error en fila individual: {str(row_error)}")
                    
                    rows_imported += batch_success
                    logger.info(f"Procesamiento fila por fila: {batch_success} de {len(batch_df)} insertadas")
            
            logger.info(f"Importación por lotes completada. {rows_imported} de {total_rows} filas importadas")
            
            if rows_imported < total_rows:
                logger.warning(f"{total_rows - rows_imported} de {total_rows} filas con errores")

            return rows_imported > 0

        except Exception as e:
            logger.error(f"Error general en importación por lotes: {str(e)}")
            return False

    def importar_por_lotes(self, conn, tabla, df, batch_size=100):
        """
        Importa datos por lotes en caso de error con to_sql
        """
        total_rows = len(df)
        total_batches = (total_rows + batch_size - 1) // batch_size
        rows_imported = 0
        
        try:
            for i in range(0, total_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                try:
                    # Intentar importar el lote
                    cursor = conn.cursor()
                    
                    # Generar la consulta SQL INSERT DINÁMICAMENTE con las columnas que tenemos
                    columns_list = list(batch_df.columns)
                    columns_str = ', '.join([f'"{col}"' for col in columns_list])
                    placeholders = ', '.join(['%s'] * len(columns_list))
                    
                    query = f'INSERT INTO {tabla} ({columns_str}) VALUES ({placeholders})'
                    
                    # Preparar los valores para la consulta
                    batch_data = []
                    for _, row in batch_df.iterrows():
                        row_values = []
                        for col in columns_list:
                            value = row[col]
                            # Convertir NaN a None
                            if pd.isna(value):
                                row_values.append(None)
                            else:
                                row_values.append(value)
                        batch_data.append(tuple(row_values))

                    # Ejecutar la consulta con los datos del lote
                    cursor.executemany(query, batch_data)
                    conn.commit()
                    cursor.close()
                    
                    rows_imported += len(batch_df)
                    logger.info(f"Insertado lote {i+1}/{total_batches} ({len(batch_df)} filas)")
                    
                except Exception as e:
                    logger.error(f"Error al insertar lote: {str(e)}")
                    conn.rollback()
                    
                    # Intentar insertar fila por fila en caso de error en el lote
                    logger.info("Intentando insertar fila por fila para el lote con error...")
                    batch_success = 0
                    
                    for _, row in batch_df.iterrows():
                        try:
                            cursor = conn.cursor()

                            # Generar la consulta SQL INSERT para una sola fila DINÁMICAMENTE
                            columns_list = list(batch_df.columns)
                            columns_str = ', '.join([f'"{col}"' for col in columns_list])
                            placeholders = ', '.join(['%s'] * len(columns_list))
                            
                            query = f'INSERT INTO {tabla} ({columns_str}) VALUES ({placeholders})'
                            
                            # Preparar los valores para la consulta
                            row_values = []
                            for col in columns_list:
                                value = row[col]
                                # Convertir NaN a None
                                if pd.isna(value):
                                    row_values.append(None)
                                else:
                                    row_values.append(value)
                            
                            # Ejecutar la consulta con los datos de la fila
                            cursor.execute(query, tuple(row_values))
                            conn.commit()
                            cursor.close()
                            
                            batch_success += 1
                            
                        except Exception as row_error:
                            logger.error(f"Error en fila individual: {str(row_error)}")
                            logger.error(f"SQL problemático: {query}")
                            logger.error(f"Valores problemáticos: {row_values}")
                            conn.rollback()
                    
                    rows_imported += batch_success
                    logger.info(f"Procesamiento fila por fila: {batch_success} de {len(batch_df)} insertadas")
            
            logger.info(f"Importación por lotes completada. {rows_imported} de {total_rows} filas importadas")
            
            if rows_imported < total_rows:
                logger.warning(f"{total_rows - rows_imported} de {total_rows} filas con errores")

            return rows_imported > 0

        except Exception as e:
            logger.error(f"Error general en importación por lotes: {str(e)}")
            return False

    def truncar_tabla(self, conn, schema, tabla):
        """
        Trunca una tabla de la base de datos
        Args:
            conn: Conexión a la base de datos (PostgresConnection o psycopg2 connection)
            schema (str): Nombre del esquema
            tabla (str): Nombre de la tabla
            
        Returns:
            bool: True si se truncó correctamente, False en caso contrario
        """
        try:
            if isinstance(conn, PostgresConnection):
                # Es un objeto PostgresConnection
                with conn.conn.cursor() as cursor:
                    cursor.execute(f"DELETE FROM {schema}.{tabla}")
                    conn.conn.commit()
            elif hasattr(conn, 'execute'):
                # SQLAlchemy connection
                from sqlalchemy import text
                conn.execute(text(f"DELETE FROM {schema}.{tabla}"))
            elif hasattr(conn, 'cursor'):
                # Psycopg2 connection
                with conn.cursor() as cursor:
                    cursor.execute(f"DELETE FROM {schema}.{tabla}")
                    conn.commit()
            else:
                logger.error(f"Tipo de conexión no reconocido para truncar tabla {schema}.{tabla}")
                return False
            
            logger.info(f"Tabla {schema}.{tabla} truncada correctamente")
            return True
        except Exception as e:
            logger.error(f"Error al truncar tabla {schema}.{tabla}: {str(e)}")
            if isinstance(conn, PostgresConnection) and conn.conn:
                conn.conn.rollback()
            elif hasattr(conn, 'rollback'):
                conn.rollback()
            return False

class StockProcessor:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def create_table_with_schema(self, table_name):
        """
        Crea una tabla para stock si no existe
        """
        try:
            # Verificar si la tabla tiene esquema
            if '.' in table_name:
                schema, table = table_name.split('.')
                
                # Crear esquema si no existe
                with self.db_connection.conn.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    logger.info(f"Esquema verificado/creado: {schema}")

                # Determinar qué configuración usar basado en el nombre de la tabla
                if table == 'ejecucion':
                    required_columns = EJECUCION_REQUIRED_COLUMNS
                    column_types = EJECUCION_COLUMN_TYPES
                elif table == 'stock_critico':
                    required_columns = STOCK_REQUIRED_COLUMNS
                    column_types = STOCK_COLUMN_TYPES
                else:  # Asumimos 'ordenes' como default
                    required_columns = ORDENES_REQUIRED_COLUMNS
                    column_types = ORDENES_COLUMN_TYPES

                # Construir SQL para crear la tabla
                columns_sql = []
                for col in required_columns:
                    col_name = clean_column_name(col)
                    col_type = column_types.get(col_name, "VARCHAR(1000)")
                    columns_sql.append(f"{col_name} {col_type}")

                # Añadir ID como clave primaria
                columns_sql.append("id SERIAL PRIMARY KEY")

                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    {', '.join(columns_sql)}
                )
                """

                # Crear la tabla
                with self.db_connection.conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    logger.info(f"Tabla verificada/creada: {schema}.{table}")

                return True
            else:
                logger.error("El nombre de la tabla debe incluir un esquema (esquema.tabla)")
                return False
        except Exception as e:
            logger.error(f"Error al crear tabla de stock: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def map_excel_to_required_columns(self, df, file_name):
        """
        Mapea las columnas del Excel a las columnas requeridas para Stock
        con mayor tolerancia a variaciones en nombres
        """
        logger.info(f"Mapeando columnas para {file_name}...")
        
        # Crear DataFrame con las columnas requeridas
        mapped_df = pd.DataFrame()
        
        # Limpiar nombres de columnas del DataFrame original
        df.columns = [str(col).strip() if isinstance(col, str) else str(col) for col in df.columns]
        
        # Log para depuración
        logger.info(f"Columnas originales: {', '.join(df.columns.tolist())}")

        # Mapeo explícito de columnas Excel a PostgreSQL para Stock con más variaciones
        excel_to_postgres_map = {
            'Codigo': 'codigo',
            'Código': 'codigo',
            'Producto': 'producto',
            'Concentración': 'concentracion',
            'Concentracion': 'concentracion',
            'Forma Farmaceutica': 'forma_farmaceutica',
            'Forma Farmacéutica': 'forma_farmaceutica',
            'Presentación': 'presentacion',
            'Presentacion': 'presentacion',
            'Clasificacion': 'clasificacion',
            'Clasificación': 'clasificacion',
            'Meses en Movimiento': 'meses_en_movimiento',
            'Cantidad Distribuida': 'cantidad_distribuida',
            'Stock Actual': 'stock_actual',
            'Stock Reservado': 'stock_reservado',
            'Stock Disponible': 'stock_disponible',
            'DMP': 'dmp',
            'Estado Stock': 'estado_stock',
            'Stock Hosp.': 'stock_hosp',
            'OC.': 'oc'
        }

        # Crear un mapeo normalizado para comparación insensible a mayúsculas/minúsculas y espacios
        normalized_map = {}
        for excel_name, db_name in excel_to_postgres_map.items():
            normalized_key = excel_name.lower().replace(' ', '').replace('.', '').replace('_', '')
            normalized_map[normalized_key] = db_name

        # Mapear columnas Excel a PostgreSQL con tres niveles de búsqueda
        for excel_col in df.columns:
            # 1. Búsqueda directa por nombre exacto
            if excel_col in excel_to_postgres_map:
                postgres_col = excel_to_postgres_map[excel_col]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (exacta)")
                continue
            
            # 2. Búsqueda por normalización
            excel_norm = excel_col.lower().replace(' ', '').replace('.', '').replace('_', '')
            if excel_norm in normalized_map:
                postgres_col = normalized_map[excel_norm]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (normalizada)")
                continue
            
            # 3. Búsqueda por similitud parcial
            matched = False
            for excel_key_norm, postgres_name in normalized_map.items():
                # Comprobar si una es substring de la otra
                if excel_norm in excel_key_norm or excel_key_norm in excel_norm:
                    mapped_df[postgres_name] = df[excel_col].copy()
                    logger.info(f"Columna '{excel_col}' mapeada a '{postgres_name}' (similitud)")
                    matched = True
                    break
            
            if not matched:
                logger.warning(f"No se encontró mapeo para la columna '{excel_col}'")

        # Verificar todas las columnas requeridas y llenar con NULL si no existen
        for postgres_col in set(excel_to_postgres_map.values()):
            if postgres_col not in mapped_df.columns:
                mapped_df[postgres_col] = None
                logger.warning(f"Columna requerida '{postgres_col}' no encontrada, se usará NULL")

        # Convertir tipos de datos según la definición
        for col in mapped_df.columns:
            col_type = STOCK_COLUMN_TYPES.get(col, "VARCHAR(255)")
            
            try:
                if "NUMERIC" in col_type:
                    # Limpiar primero si son strings
                    if mapped_df[col].dtype == object:
                        # Reemplazar comas por puntos en valores numéricos
                        mapped_df[col] = mapped_df[col].astype(str).str.replace(',', '.', regex=False)
                    # Convertir a numérico
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce')
                elif "INTEGER" in col_type:
                    # Convertir a entero
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce').astype('Int64')
                # Para VARCHAR y TEXT, dejar como strings
            except Exception as e:
                logger.warning(f"Error al convertir columna {col} a {col_type}: {str(e)}")

        return mapped_df

    def process_excel_file(self, file_content, file_name, table_name):
        """Procesa un archivo STOCK_CRITICO y lo importa a PostgreSQL"""
        try:
            # Leer Excel con métodos robustos
            logger.info(f"Leyendo archivo {file_name}...")
            df = read_excel_robust(file_content, file_name)
            
            if df is None:
                st.error(f"No se pudo leer el archivo {file_name}")
                return False

            # Eliminar filas completamente vacías
            df = df.dropna(how='all')
            
            # Log para depuración
            logger.info(f"Columnas en el archivo original: {', '.join(df.columns.tolist())}")
            logger.info(f"Primeras filas del archivo original: \n{df.head().to_string()}")
            
            # Crear tabla con el esquema requerido
            if not self.create_table_with_schema(table_name):
                return False

            # Mapear columnas del Excel a las columnas requeridas
            mapped_df = self.map_excel_to_required_columns(df, file_name)
            
            # Log para depuración
            logger.info(f"Columnas mapeadas: {', '.join(mapped_df.columns.tolist())}")
            logger.info(f"Muestra de datos mapeados: \n{mapped_df.head().to_string()}")
            
            schema, table = table_name.split('.')
            
            # Verificar si hay datos para importar
            if mapped_df.empty:
                logger.error("No hay datos para importar después del mapeo de columnas")
                return False

            # En lugar de truncar, usamos DELETE + INSERT en una transacción
            with self.db_connection.conn.cursor() as cursor:
                # Comenzar transacción
                self.db_connection.conn.autocommit = False
                try:
                    # Modificación: Usar DELETE FROM en lugar de TRUNCATE
                    cursor.execute(f"DELETE FROM {schema}.{table}")
                    logger.info(f"Tabla {table_name} vaciada correctamente")
                    
                    # Importar los nuevos datos usando INSERT
                    for i, row in mapped_df.iterrows():
                        placeholders = ", ".join(["%s"] * len(row))
                        columns = ", ".join([f'"{col}"' for col in mapped_df.columns])
                        insert_sql = f"""
                        INSERT INTO {schema}.{table} ({columns})
                        VALUES ({placeholders})
                        """
                        # Convertir NaN a None
                        values = [None if pd.isna(val) else val for val in row]
                        cursor.execute(insert_sql, values)

                    # Confirmar transacción
                    self.db_connection.conn.commit()
                    logger.info(f"Datos importados correctamente. {len(mapped_df)} filas.")
                    return True
                    
                except Exception as e:
                    # Revertir cambios en caso de error
                    self.db_connection.conn.rollback()
                    logger.error(f"Error al importar datos: {str(e)}")
                    logger.error(traceback.format_exc())
                    return False
                finally:
                    # Restaurar autocommit
                    self.db_connection.conn.autocommit = True

        except Exception as e:
            logger.error(f"Error al procesar {file_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

# Agregar esta clase después de la clase StockProcessor (alrededor de la línea 1800)
class PedidosProcessor:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def create_table_with_schema(self, table_name):
        """
        Crea una tabla para pedidos con TODAS las columnas necesarias
        """
        try:
            # Verificar si la tabla tiene esquema
            if '.' in table_name:
                schema, table = table_name.split('.')
                
                # Crear esquema si no existe
                with self.db_connection.conn.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    logger.info(f"Esquema verificado/creado: {schema}")

                # Construir SQL para crear la tabla con TODAS las columnas
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    nro_pedido VARCHAR(255),
                    simese VARCHAR(255),
                    fecha_pedido DATE,
                    codigo VARCHAR(50),
                    medicamento TEXT,
                    stock NUMERIC,
                    dmp NUMERIC,
                    cantidad NUMERIC,
                    meses_cantidad NUMERIC,
                    dias_transcurridos INTEGER,
                    estado VARCHAR(255),
                    prioridad VARCHAR(255),
                    nro_oc VARCHAR(255),
                    fecha_oc DATE,
                    descripcion TEXT,
                    proveedor VARCHAR(255),
                    entrega DATE,
                    recepcion DATE,
                    cantidad_recibida NUMERIC,
                    observaciones TEXT,
                    id SERIAL PRIMARY KEY
                )
                """

                # Crear la tabla
                with self.db_connection.conn.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    logger.info(f"Tabla verificada/creada: {schema}.{table}")

                return True
            else:
                logger.error("El nombre de la tabla debe incluir un esquema (esquema.tabla)")
                return False
        except Exception as e:
            logger.error(f"Error al crear tabla de pedidos: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def map_excel_to_required_columns(self, df, file_name):
        """
        Mapea las columnas del Excel a las columnas requeridas para Pedidos
        con mayor tolerancia a variaciones en nombres
        """
        logger.info(f"Mapeando columnas para {file_name}...")
        
        # Crear DataFrame con las columnas requeridas
        mapped_df = pd.DataFrame()
        
        # Limpiar nombres de columnas del DataFrame original
        df.columns = [str(col).strip() if isinstance(col, str) else str(col) for col in df.columns]
        
        # Log para depuración
        logger.info(f"Columnas originales: {', '.join(df.columns.tolist())}")

        # Mapeo explícito de columnas Excel a PostgreSQL con más variaciones
        excel_to_postgres_map = {
            # Mapeos principales - TODOS LOS CAMPOS DEL EXCEL
            'Numero Pedido': 'nro_pedido',
            'SIMESE': 'simese',
            'Fecha Pedido': 'fecha_pedido',
            'Codigo Medicamento': 'codigo',
            'Medicamento': 'medicamento',
            'Stock': 'stock',
            'DMP': 'dmp',
            'Cantidad': 'cantidad',
            'Meses Cantidad': 'meses_cantidad',
            'Dias Transcurridos': 'dias_transcurridos',
            'Estado': 'estado',
            'Prioridad': 'prioridad',
            'Nro.OC.': 'nro_oc',
            'Fecha OC.': 'fecha_oc',
            'Opciones': 'opciones',
            
            # Variaciones comunes
            'Nro. Pedido': 'nro_pedido',
            'Nro Pedido': 'nro_pedido',
            'N° Pedido': 'nro_pedido',
            'Fecha de Pedido': 'fecha_pedido',
            'Código': 'codigo',
            'Código Medicamento': 'codigo',
            'Codigo': 'codigo',
            'Nro OC': 'nro_oc',
            'Numero OC': 'nro_oc',
            'N° OC': 'nro_oc',
            'Fecha OC': 'fecha_oc'
        }

        # Crear un mapeo normalizado para comparación insensible a mayúsculas/minúsculas y espacios
        normalized_map = {}
        for excel_name, db_name in excel_to_postgres_map.items():
            normalized_key = excel_name.lower().replace(' ', '').replace('.', '').replace('_', '')
            normalized_map[normalized_key] = db_name

        # Mapear columnas Excel a PostgreSQL con tres niveles de búsqueda
        for excel_col in df.columns:
            # 1. Búsqueda directa por nombre exacto
            if excel_col in excel_to_postgres_map:
                postgres_col = excel_to_postgres_map[excel_col]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (exacta)")
                continue
            
            # 2. Búsqueda por normalización
            excel_norm = excel_col.lower().replace(' ', '').replace('.', '').replace('_', '')
            if excel_norm in normalized_map:
                postgres_col = normalized_map[excel_norm]
                mapped_df[postgres_col] = df[excel_col].copy()
                logger.info(f"Columna '{excel_col}' mapeada a '{postgres_col}' (normalizada)")
                continue
            
            # 3. Búsqueda por similitud parcial
            matched = False
            for excel_key_norm, postgres_name in normalized_map.items():
                # Comprobar si una es substring de la otra
                if excel_norm in excel_key_norm or excel_key_norm in excel_norm:
                    mapped_df[postgres_name] = df[excel_col].copy()
                    logger.info(f"Columna '{excel_col}' mapeada a '{postgres_name}' (similitud)")
                    matched = True
                    break
            
            if not matched:
                logger.warning(f"No se encontró mapeo para la columna '{excel_col}'")

        # Verificar todas las columnas requeridas según la estructura correcta
        required_db_columns = [
            'nro_pedido', 'simese', 'fecha_pedido', 'codigo', 'medicamento', 
            'stock', 'dmp', 'cantidad', 'meses_cantidad', 'dias_transcurridos',
            'estado', 'prioridad', 'nro_oc', 'fecha_oc', 'opciones'
        ]
        
        for postgres_col in required_db_columns:
            if postgres_col not in mapped_df.columns:
                mapped_df[postgres_col] = None
                logger.warning(f"Columna requerida '{postgres_col}' no encontrada, se usará NULL")

        # Convertir tipos de datos según la definición
        for col in mapped_df.columns:
            col_type = PEDIDOS_COLUMN_TYPES.get(col, "VARCHAR(255)")
            
            try:
                if "NUMERIC" in col_type:
                    # Limpiar primero si son strings
                    if mapped_df[col].dtype == object:
                        # Reemplazar comas por puntos en valores numéricos
                        mapped_df[col] = mapped_df[col].astype(str).str.replace(',', '.', regex=False)
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce')
                elif "INTEGER" in col_type:
                    mapped_df[col] = pd.to_numeric(mapped_df[col], errors='coerce').astype('Int64')
                elif "DATE" in col_type:
                    # Usar función segura para conversión de fechas
                    mapped_df[col] = safe_date_conversion(mapped_df[col])
                # Para VARCHAR y TEXT, dejar como strings
            except Exception as e:
                logger.warning(f"Error al convertir columna {col} a {col_type}: {str(e)}")

        return mapped_df

    def process_excel_file(self, file_content, file_name, table_name):
        """Procesa un archivo Excel de pedidos y lo importa a PostgreSQL"""
        try:
            # Leer Excel con métodos robustos
            logger.info(f"Leyendo archivo {file_name}...")
            df = read_excel_robust(file_content, file_name)
            
            if df is None:
                st.error(f"No se pudo leer el archivo {file_name}")
                return False

            # Eliminar filas completamente vacías
            df = df.dropna(how='all')
            
            # Log para depuración
            logger.info(f"Columnas en el archivo original: {', '.join(df.columns.tolist())}")
            logger.info(f"Primeras filas del archivo original: \n{df.head().to_string()}")
            
            # Crear tabla con el esquema requerido
            if not self.create_table_with_schema(table_name):
                return False

            # Mapear columnas del Excel a las columnas requeridas
            mapped_df = self.map_excel_to_required_columns(df, file_name)
            
            # Log para depuración
            logger.info(f"Columnas mapeadas: {', '.join(mapped_df.columns.tolist())}")
            logger.info(f"Muestra de datos mapeados: \n{mapped_df.head().to_string()}")
            
            schema, table = table_name.split('.')
            
            # Verificar si hay datos para importar
            if mapped_df.empty:
                logger.error("No hay datos para importar después del mapeo de columnas")
                return False

            # En lugar de truncar, usamos DELETE + INSERT en una transacción
            with self.db_connection.conn.cursor() as cursor:
                # Comenzar transacción
                self.db_connection.conn.autocommit = False
                try:
                    # Modificación: Usar DELETE FROM en lugar de TRUNCATE
                    cursor.execute(f"DELETE FROM {schema}.{table}")
                    logger.info(f"Tabla {table_name} vaciada correctamente")
                    
                    # Importar los nuevos datos
                    for i, row in mapped_df.iterrows():
                        placeholders = ", ".join(["%s"] * len(row))
                        columns = ", ".join([f'"{col}"' for col in mapped_df.columns])
                        insert_sql = f"""
                        INSERT INTO {schema}.{table} ({columns})
                        VALUES ({placeholders})
                        """
                        # Convertir NaN a None
                        values = [None if pd.isna(val) else val for val in row]
                        cursor.execute(insert_sql, values)

                    # Confirmar transacción
                    self.db_connection.conn.commit()
                    logger.info(f"Datos importados correctamente. {len(mapped_df)} filas.")
                    return True
                    
                except Exception as e:
                    # Revertir cambios en caso de error
                    self.db_connection.conn.rollback()
                    logger.error(f"Error al importar datos: {str(e)}")
                    logger.error(traceback.format_exc())
                    return False
                finally:
                    # Restaurar autocommit
                    self.db_connection.conn.autocommit = True
                    
        except Exception as e:
            logger.error(f"Error al procesar {file_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

# Inicializar el estado de sesión para la navegación si no existe
if 'nav_option' not in st.session_state:
    st.session_state.nav_option = "Órdenes"

# Diseño de la página con la navegación en el sidebar
# Navegación en el sidebar
st.sidebar.title("SICIAP")
st.sidebar.markdown("---")

# Navegación directa sin usar session_state
selected_option = st.sidebar.selectbox(
    "Selecciona una opción:",
    ["📋 Órdenes", "📊 Ejecución", "📦 Stock", "📝 Pedidos", "📈 Dashboard", 
     "📑 Contratos", "🔍 Diagnóstico", "📤 Exportar Datos", "📥 Importar Datos"],
    key="navigation"
)

# Configuración de PostgreSQL en el sidebar
st.sidebar.markdown("---")
st.sidebar.header("Configuración de PostgreSQL")
host = st.sidebar.text_input("Host", "localhost", key="global_host")
port = st.sidebar.number_input("Puerto", value=5432, min_value=1, max_value=65535, key="global_port")
dbname = st.sidebar.text_input("Base de datos", "postgres", key="global_dbname")
user = st.sidebar.text_input("Usuario", "postgres", key="global_user")
password = st.sidebar.text_input("Contraseña", "Dggies12345", type="password", key="global_password")

# Botón para probar la conexión
if st.sidebar.button("Probar Conexión", key="global_test_conn"):
    conn = PostgresConnection(host, port, dbname, user, password)
    if conn.connect():
        st.sidebar.success("✅ Conexión exitosa a PostgreSQL!")
        conn.close()
    else:
        st.sidebar.error("❌ No se pudo conectar a PostgreSQL")

# Esta función está duplicada, usar la función get_db_config() definida arriba
# Se mantiene por compatibilidad pero ahora usa la configuración unificada

def test_db_connection():
    """
    Prueba la conexión a la base de datos.
    """
    try:
        db_config = get_db_config()
        engine_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        logger.info(f"Intentando conectar a la base de datos: {db_config['dbname']} en {db_config['host']}:{db_config['port']}")
        
        engine = create_engine(engine_string)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test")).fetchone()
            if result and result[0] == 1:
                logger.info("Conexión exitosa a la base de datos!")
                print("Conexión exitosa a la base de datos!")
                return True
            else:
                logger.error("Error de conexión a la base de datos.")
                print("Error de conexión a la base de datos.")
                return False
    except Exception as e:
        error_msg = f"Error al conectar a la base de datos: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        print(error_msg)
        traceback.print_exc()
        return False

def ordenes_page():
    """Página para importar órdenes de compra"""
    st.header("Importación de Órdenes de Compra")
    st.markdown("### Carga tus archivos Excel de órdenes para importarlos a PostgreSQL")

    # Obtener configuración de la BD
    db_config = {
        'host': st.session_state.get('global_host', 'localhost'),
        'port': st.session_state.get('global_port', 5432),
        'dbname': st.session_state.get('global_dbname', 'postgres'),
        'user': st.session_state.get('global_user', 'postgres'),
        'password': st.session_state.get('global_password', 'Dggies12345')
    }
    
    # Subir archivos
    uploaded_files = st.file_uploader(
        "Selecciona uno o más archivos Excel de órdenes", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True,
        key="ordenes_uploader"
    )
    
    if uploaded_files:
        st.success(f"Se han seleccionado {len(uploaded_files)} archivos")
        
        # Mostrar archivos seleccionados
        for i, file in enumerate(uploaded_files):
            st.write(f"{i+1}. {file.name} ({file.size/1024:.2f} KB)")

        # Configuración adicional
        st.subheader("Configuración de Importación")
        table_name = st.text_input("Nombre de la tabla (predeterminado: siciap.ordenes)", "siciap.ordenes", key="ordenes_table")

        # Botón para importar datos
        if st.button("Importar Datos de Órdenes", key="ordenes_import"):
            # Crear conexión a PostgreSQL
            db_conn = PostgresConnection(
                db_config['host'], 
                int(db_config['port']), 
                db_config['dbname'], 
                db_config['user'], 
                db_config['password']
            )
            
            if not db_conn.connect():
                st.error("❌ Error al conectar a PostgreSQL. Verifica la configuración.")
                return

            # Crear procesador de órdenes
            processor = OrdenesProcessor(db_conn)
            
            # Procesar cada archivo
            with st.spinner("Importando archivos..."):
                overall_success = True
                for uploaded_file in uploaded_files:
                    file_name = uploaded_file.name
                    progress_text = st.empty()
                    progress_text.write(f"Procesando {file_name}...")

                    try:
                        # Leer el archivo en memoria
                        file_content = BytesIO(uploaded_file.getvalue())
                        
                        # Procesar e importar
                        success = processor.process_excel_file(file_content, file_name, table_name)
                        
                        if success:
                            progress_text.success(f"✅ {file_name} importado correctamente")
                        else:
                            progress_text.error(f"❌ Error al importar {file_name}")
                            overall_success = False
                    except Exception as e:
                        progress_text.error(f"❌ Error al procesar {file_name}: {str(e)}")
                        st.exception(e)
                        overall_success = False
            
            # Cerrar conexión
            db_conn.close()
            
            if overall_success:
                st.success("✅ Proceso completado exitosamente. Conexión a PostgreSQL cerrada.")
            else:
                st.warning("⚠️ Proceso completado con errores. Conexión a PostgreSQL cerrada.")
    else:
        st.info("Por favor, selecciona uno o más archivos Excel para importar.")

# En la función de la página de ejecución (ejecucion_page())
def ejecucion_page():
    """Página para importar datos de ejecución"""
    st.header("Importación de Datos de Ejecución")
    st.markdown("### Carga tus archivos Excel de ejecución para importarlos a PostgreSQL")
    
    # Obtener configuración de la BD
    db_config = {
        'host': st.session_state.get('global_host', 'localhost'),
        'port': st.session_state.get('global_port', 5432),
        'dbname': st.session_state.get('global_dbname', 'postgres'),
        'user': st.session_state.get('global_user', 'postgres'),
        'password': st.session_state.get('global_password', 'Dggies12345')
    }
    
    # Subir archivos
    uploaded_files = st.file_uploader(
        "Selecciona uno o más archivos Excel de ejecución", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True,
        key="ejecucion_uploader"
    )
    
    if uploaded_files:
        st.success(f"Se han seleccionado {len(uploaded_files)} archivos")
        
        # Mostrar archivos seleccionados
        for i, file in enumerate(uploaded_files):
            st.write(f"{i+1}. {file.name} ({file.size/1024:.2f} KB)")

        # Configuración adicional
        st.subheader("Configuración de Importación")
        table_name = st.text_input("Nombre de la tabla (predeterminado: siciap.ejecucion)", "siciap.ejecucion", key="ejecucion_table")

        # Botón para importar datos
        if st.button("Importar Datos de Ejecución", key="ejecucion_import"):
            # Crear conexión a PostgreSQL
            db_conn = PostgresConnection(
                host=db_config['host'], 
                port=int(db_config['port']), 
                dbname=db_config['dbname'], 
                user=db_config['user'], 
                password=db_config['password']
            )
            
            if not db_conn.connect():
                st.error("❌ Error al conectar a PostgreSQL. Verifica la configuración.")
                return

            # Crear procesador de ejecución - CORREGIDO
            processor = EjecucionProcessor(db_conn)
            
            # Procesar cada archivo
            with st.spinner("Importando archivos..."):
                overall_success = True
                for uploaded_file in uploaded_files:
                    file_name = uploaded_file.name
                    progress_text = st.empty()
                    progress_text.write(f"Procesando {file_name}...")

                    try:
                        # Leer el archivo en memoria
                        file_content = BytesIO(uploaded_file.getvalue())
                        
                        # Procesar e importar
                        success = processor.process_excel_file(file_content, file_name, table_name)
                        
                        if success:
                            progress_text.success(f"✅ {file_name} importado correctamente")
                        else:
                            progress_text.error(f"❌ Error al importar {file_name}")
                            overall_success = False
                    except Exception as e:
                        progress_text.error(f"❌ Error al procesar {file_name}: {str(e)}")
                        st.exception(e)
                        overall_success = False
            
            # Cerrar conexión
            db_conn.close()
            
            if overall_success:
                st.success("✅ Proceso completado exitosamente. Conexión a PostgreSQL cerrada.")
            else:
                st.warning("⚠️ Proceso completado con errores. Conexión a PostgreSQL cerrada.")
    else:
        st.info("Por favor, selecciona uno o más archivos Excel para importar.")

# En la función de la página de stock (stock_page())
def stock_page():
    """Página para importar datos de stock"""
    st.header("Importación de Datos de Stock")
    st.markdown("### Carga tus archivos Excel de stock para importarlos a PostgreSQL")
    
    # Obtener configuración de la BD
    db_config = {
        'host': st.session_state.get('global_host', 'localhost'),
        'port': st.session_state.get('global_port', 5432),
        'dbname': st.session_state.get('global_dbname', 'postgres'),
        'user': st.session_state.get('global_user', 'postgres'),
        'password': st.session_state.get('global_password', 'Dggies12345')
    }
    
    # Subir archivos
    uploaded_files = st.file_uploader(
        "Selecciona uno o más archivos Excel de stock", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True,
        key="stock_uploader"
    )
    
    if uploaded_files:
        st.success(f"Se han seleccionado {len(uploaded_files)} archivos")
        
        # Mostrar archivos seleccionados
        for i, file in enumerate(uploaded_files):
            st.write(f"{i+1}. {file.name} ({file.size/1024:.2f} KB)")

        # Configuración adicional
        st.subheader("Configuración de Importación")
        table_name = st.text_input("Nombre de la tabla (predeterminado: siciap.stock_critico)", "siciap.stock_critico", key="stock_table")

        # Botón para importar datos
        if st.button("Importar Datos de Stock", key="stock_import"):
            # Crear conexión a PostgreSQL
            db_conn = PostgresConnection(
                host=db_config['host'], 
                port=int(db_config['port']), 
                dbname=db_config['dbname'], 
                user=db_config['user'], 
                password=db_config['password']
            )
            
            if not db_conn.connect():
                st.error("❌ Error al conectar a PostgreSQL. Verifica la configuración.")
                return

            # Crear procesador de stock - CORREGIDO
            processor = StockProcessor(db_conn)
            
            # Procesar cada archivo
            with st.spinner("Importando archivos..."):
                overall_success = True
                for uploaded_file in uploaded_files:
                    file_name = uploaded_file.name
                    progress_text = st.empty()
                    progress_text.write(f"Procesando {file_name}...")

                    try:
                        # Leer el archivo en memoria
                        file_content = BytesIO(uploaded_file.getvalue())
                        
                        # Procesar e importar
                        success = processor.process_excel_file(file_content, file_name, table_name)
                        
                        if success:
                            progress_text.success(f"✅ {file_name} importado correctamente")
                        else:
                            progress_text.error(f"❌ Error al importar {file_name}")
                            overall_success = False
                    except Exception as e:
                        progress_text.error(f"❌ Error al procesar {file_name}: {str(e)}")
                        st.exception(e)
                        overall_success = False
            
            # Cerrar conexión
            db_conn.close()
            
            if overall_success:
                st.success("✅ Proceso completado exitosamente. Conexión a PostgreSQL cerrada.")
            else:
                st.warning("⚠️ Proceso completado con errores. Conexión a PostgreSQL cerrada.")
    else:
        st.info("Por favor, selecciona uno o más archivos Excel para importar.")

def pedidos_page():
    """Página para importar pedidos"""
    st.header("Importación de Pedidos")
    st.markdown("### Carga tus archivos Excel de pedidos para importarlos a PostgreSQL")

    # Obtener configuración de la BD
    db_config = {
        'host': st.session_state.get('global_host', 'localhost'),
        'port': st.session_state.get('global_port', 5432),
        'dbname': st.session_state.get('global_dbname', 'postgres'),
        'user': st.session_state.get('global_user', 'postgres'),
        'password': st.session_state.get('global_password', 'Dggies12345')
    }
    
    # Subir archivos
    uploaded_files = st.file_uploader(
        "Selecciona uno o más archivos Excel de pedidos", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True,
        key="pedidos_uploader"
    )
    
    if uploaded_files:
        st.success(f"Se han seleccionado {len(uploaded_files)} archivos")
        
        # Mostrar archivos seleccionados
        for i, file in enumerate(uploaded_files):
            st.write(f"{i+1}. {file.name} ({file.size/1024:.2f} KB)")

        # Configuración adicional
        st.subheader("Configuración de Importación")
        table_name = st.text_input("Nombre de la tabla (predeterminado: siciap.pedidos)", "siciap.pedidos", key="pedidos_table")

        # Botón para importar datos
        if st.button("Importar Datos de Pedidos", key="pedidos_import"):
            # Crear conexión a PostgreSQL
            db_conn = PostgresConnection(
                host=db_config['host'], 
                port=int(db_config['port']), 
                dbname=db_config['dbname'], 
                user=db_config['user'], 
                password=db_config['password']
            )
            
            if not db_conn.connect():
                st.error("❌ Error al conectar a PostgreSQL. Verifica la configuración.")
                return

            # Crear procesador de pedidos
            processor = PedidosProcessor(db_conn)
            
            # Procesar cada archivo
            with st.spinner("Importando archivos..."):
                overall_success = True
                for uploaded_file in uploaded_files:
                    file_name = uploaded_file.name
                    progress_text = st.empty()
                    progress_text.write(f"Procesando {file_name}...")

                    try:
                        # Leer el archivo en memoria
                        file_content = BytesIO(uploaded_file.getvalue())
                        
                        # Procesar e importar
                        success = processor.process_excel_file(file_content, file_name, table_name)
                        
                        if success:
                            progress_text.success(f"✅ {file_name} importado correctamente")
                        else:
                            progress_text.error(f"❌ Error al importar {file_name}")
                            overall_success = False
                    except Exception as e:
                        progress_text.error(f"❌ Error al procesar {file_name}: {str(e)}")
                        st.exception(e)
                        overall_success = False
            
            # Cerrar conexión
            db_conn.close()
            
            if overall_success:
                st.success("✅ Proceso completado exitosamente. Conexión a PostgreSQL cerrada.")
            else:
                st.warning("⚠️ Proceso completado con errores. Conexión a PostgreSQL cerrada.")
    else:
        st.info("Por favor, selecciona uno o más archivos Excel para importar.")

def verificar_tablas():
    # Configuración de base de datos
    db_config = get_db_config()
    conn = PostgresConnection(**db_config)
    
    if not conn.connect():
        logger.error("No se pudo conectar a PostgreSQL para verificar tablas")
        return False

    try:
        with conn.conn.cursor() as cursor:
            # Crear esquema si no existe
            cursor.execute("CREATE SCHEMA IF NOT EXISTS siciap")
            
            # Verificar tabla pedidos
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'siciap' AND table_name = 'pedidos'
                )
            """)
            
            if not cursor.fetchone()[0]:
                # Crear tabla pedidos
                columns = []
                for col, type_def in PEDIDOS_COLUMN_TYPES.items():
                    columns.append(f"{col} {type_def}")
                columns.append("id SERIAL PRIMARY KEY")
                
                create_sql = f"""
                CREATE TABLE siciap.pedidos (
                    {", ".join(columns)}
                )
                """
                cursor.execute(create_sql)
                logger.info("Tabla siciap.pedidos creada")
        
        # Resto de verificaciones de tablas...
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error al verificar tablas: {str(e)}")
        if conn:
            conn.close()
        return False

def contracts_management_page():
    """Página para gestionar contratos, llamados y sus relaciones"""
    st.markdown("# Gestión de Contratos y Llamados")
    st.info("Esta página permite gestionar llamados, contratos y sus relaciones.")
    
    # Conexión a la base de datos
    db_config = get_db_config()
    conn = PostgresConnection(**db_config)
    
    if not conn.connect():
        st.error("❌ No se pudo conectar a PostgreSQL.")
        return

    try:
        # Primero, aseguramos que la estructura de la base de datos es correcta
        ensure_datosejecucion_table(conn)
        
        # Crear pestañas para las diferentes secciones
        tab1, tab2, tab3, tab4 = st.tabs(["Llamados", "Contratos", "Items", "Cargar Datos"])
        
        # TAB 1: GESTIÓN DE LLAMADOS
        with tab1:
            st.subheader("Gestión de Llamados")
            st.info("Aquí puedes gestionar los llamados y sus descripciones.")
            
            # Mostrar tabla de llamados existentes
            try:
                llamados_query = """
                SELECT 
                    COALESCE(d.id_llamado, e.id_llamado) as id_llamado,
                    COALESCE(d.licitacion, e.licitacion) as licitacion,
                    d.descripcion_llamado,
                    COUNT(DISTINCT e.codigo) as cantidad_items,
                    d.fecha_inicio,
                    d.fecha_fin,
                    CASE 
                        WHEN d.fecha_fin IS NULL THEN 'Indeterminado'
                        WHEN d.fecha_fin = 'CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES' THEN 'Sí'
                        WHEN d.fecha_fin = 'cumplimiento total de las obligaciones' THEN 'Sí'
                        ELSE 'Sí'
                    END as vigente
                FROM siciap.ejecucion e
                FULL OUTER JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
                WHERE COALESCE(d.id_llamado, e.id_llamado) IS NOT NULL
                GROUP BY d.id_llamado, e.id_llamado, d.licitacion, e.licitacion, 
                         d.descripcion_llamado, d.fecha_inicio, d.fecha_fin
                ORDER BY COALESCE(d.id_llamado, e.id_llamado) DESC
                """
                llamados_df = pd.read_sql_query(text(llamados_query), conn.engine)
                
                # Formatear fechas
                for fecha_col in ['fecha_inicio', 'fecha_fin']:
                    if fecha_col in llamados_df.columns:
                        llamados_df[fecha_col] = safe_date_conversion(llamados_df[fecha_col]).dt.strftime('%d/%m/%Y')
                
                if not llamados_df.empty:
                    st.dataframe(llamados_df, use_container_width=True)
                    
                    # Opciones para editar llamados
                    st.subheader("Editar Llamado")
                    llamado_seleccionado = st.selectbox(
                        "Seleccionar llamado para editar:",
                        options=llamados_df['id_llamado'].tolist(),
                        format_func=lambda x: f"ID: {x} - {llamados_df[llamados_df['id_llamado']==x]['descripcion_llamado'].values[0] if not pd.isna(llamados_df[llamados_df['id_llamado']==x]['descripcion_llamado'].values[0]) else 'Sin descripción'}"
                    )
                    
                    if llamado_seleccionado:
                        # Obtener datos actuales del llamado
                        llamado_actual = llamados_df[llamados_df['id_llamado'] == llamado_seleccionado].iloc[0]
                        
                        # Formulario para editar
                        with st.form(key=f"editar_llamado_{llamado_seleccionado}"):
                            st.markdown("### Editar información del llamado")
                            
                            # Campos del formulario
                            desc_llamado = st.text_input(
                                "Descripción del Llamado", 
                                value=llamado_actual['descripcion_llamado'] if pd.notna(llamado_actual['descripcion_llamado']) else ""
                            )
                            
                            fecha_inicio_str = llamado_actual['fecha_inicio'] if pd.notna(llamado_actual['fecha_inicio']) else ""
                            fecha_fin_str = llamado_actual['fecha_fin'] if pd.notna(llamado_actual['fecha_fin']) else ""
                            
                            # Convertir las fechas de string a datetime para el date_input
                            from datetime import datetime
                            
                            try:
                                fecha_inicio_date = datetime.strptime(fecha_inicio_str, '%d/%m/%Y') if fecha_inicio_str else None
                            except:
                                fecha_inicio_date = None
                                
                            try:
                                fecha_fin_date = datetime.strptime(fecha_fin_str, '%d/%m/%Y') if fecha_fin_str else None
                            except:
                                fecha_fin_date = None
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                fecha_inicio = st.date_input(
                                    "Fecha de Inicio del Contrato",
                                    value=fecha_inicio_date if fecha_inicio_date else None
                                )
                            with col2:
                                fecha_fin = st.date_input(
                                    "Fecha de Fin del Contrato",
                                    value=fecha_fin_date if fecha_fin_date else None
                                )
                            
                            # Botón para guardar cambios
                            submitted = st.form_submit_button("Guardar Cambios")
                            
                            if submitted:
                                try:
                                    # Verificar si ya existe en la tabla datosejecucion
                                    check_query = text("""
                                        SELECT id FROM siciap.datosejecucion 
                                        WHERE id_llamado = :id_llamado
                                    """)
                                    with conn.engine.connect() as connection:
                                        result = connection.execute(check_query, {"id_llamado": id_llamado}).fetchone()
                                    
                                    if result:
                                        # Actualizar registro existente
                                        update_query = text("""
                                            UPDATE siciap.datosejecucion
                                            SET descripcion_llamado = :descripcion,
                                                fecha_inicio = :fecha_inicio,
                                                fecha_fin = :fecha_fin
                                            WHERE id_llamado = :id_llamado
                                        """)
                                        with conn.engine.connect() as connection:
                                            connection.execute(update_query, {
                                                "descripcion": desc_llamado,
                                                "fecha_inicio": fecha_inicio,
                                                "fecha_fin": fecha_fin,
                                                "id_llamado": llamado_seleccionado
                                            })
                                            connection.commit()
                                    else:
                                        # Insertar nuevo registro
                                        insert_query = text("""
                                            INSERT INTO siciap.datosejecucion 
                                            (id_llamado, descripcion_llamado, fecha_inicio, fecha_fin, licitacion)
                                            VALUES (:id_llamado, :descripcion, :fecha_inicio, :fecha_fin, 
                                                   (SELECT licitacion FROM siciap.ejecucion 
                                                    WHERE id_llamado = :id_llamado LIMIT 1))
                                        """)
                                        with conn.engine.connect() as connection:
                                            connection.execute(insert_query, {
                                                "id_llamado": id_llamado,
                                                "descripcion": desc_llamado,
                                                "fecha_inicio": fecha_inicio,
                                                "fecha_fin": fecha_fin
                                            })
                                            connection.commit()
                                    
                                    st.success(f"Información del llamado ID {llamado_seleccionado} actualizada correctamente.")
                                    st.experimental_rerun()
                                except Exception as e:
                                    st.error(f"Error al actualizar información: {str(e)}")
                else:
                    st.info("No hay llamados registrados.")
            except Exception as e:
                st.error(f"Error al mostrar llamados: {str(e)}")
                st.error(traceback.format_exc())

        # TAB 2: GESTIÓN DE CONTRATOS
        with tab2:
            st.subheader("Gestión de Contratos")
            st.info("Aquí puedes gestionar contratos y proveedores.")
            
            # Mostrar contratos existentes
            try:
                contratos_query = """
                SELECT DISTINCT 
                    e.id_llamado,
                    e.licitacion,
                    e.proveedor,
                    d.numero_contrato,
                    COUNT(e.codigo) as cantidad_items,
                    d.descripcion_llamado,
                    d.fecha_inicio,
                    d.fecha_fin,
                    d.dirigido_a,
                    d.lugares,
                    CASE 
                        WHEN d.fecha_fin IS NULL THEN 'Indeterminado'
                        WHEN d.fecha_fin = 'CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES' THEN 'Sí'
                        WHEN d.fecha_fin = 'cumplimiento total de las obligaciones' THEN 'Sí'
                        ELSE 'Sí'
                    END as vigente
                FROM siciap.ejecucion e
                LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
                WHERE e.proveedor IS NOT NULL
                GROUP BY e.id_llamado, e.licitacion, e.proveedor, d.numero_contrato, 
                         d.descripcion_llamado, d.fecha_inicio, d.fecha_fin, d.dirigido_a, d.lugares
                ORDER BY e.id_llamado, e.proveedor
                """
                contratos_df = pd.read_sql_query(text(contratos_query), conn.engine)
                
                # Formatear fechas
                for fecha_col in ['fecha_inicio', 'fecha_fin']:
                    if fecha_col in contratos_df.columns:
                        contratos_df[fecha_col] = safe_date_conversion(contratos_df[fecha_col]).dt.strftime('%d/%m/%Y')
                
                if not contratos_df.empty:
                    st.dataframe(contratos_df, use_container_width=True)
                    
                    # Opciones para editar contratos
                    st.subheader("Editar Contrato")
                    
                    # Primero seleccionar el ID de llamado
                    llamado_ids = contratos_df['id_llamado'].unique().tolist()
                    llamado_seleccionado = st.selectbox(
                        "Seleccionar ID de Llamado:",
                        options=llamado_ids
                    )
                    
                    if llamado_seleccionado:
                        # Filtrar proveedores por el llamado seleccionado
                        proveedores = contratos_df[contratos_df['id_llamado'] == llamado_seleccionado]['proveedor'].unique().tolist()
                        proveedor_seleccionado = st.selectbox(
                            "Seleccionar Proveedor:",
                            options=proveedores
                        )
                        
                        if proveedor_seleccionado:
                            # Obtener datos actuales del contrato
                            contrato_actual = contratos_df[
                                (contratos_df['id_llamado'] == llamado_seleccionado) & 
                                (contratos_df['proveedor'] == proveedor_seleccionado)
                            ].iloc[0]
                            
                            # Formulario para editar
                            with st.form(key=f"editar_contrato_{llamado_seleccionado}_{proveedor_seleccionado}"):
                                st.markdown("### Editar información del contrato")
                                
                                # Campos del formulario
                                numero_contrato = st.text_input(
                                    "Número de Contrato", 
                                    value=contrato_actual['numero_contrato'] if pd.notna(contrato_actual['numero_contrato']) else ""
                                )
                                
                                dirigido_a = st.text_input(
                                    "Dirigido a", 
                                    value=contrato_actual['dirigido_a'] if pd.notna(contrato_actual['dirigido_a']) else ""
                                )
                                
                                lugares = st.text_area(
                                    "Lugares (uno por línea)", 
                                    value=contrato_actual['lugares'] if pd.notna(contrato_actual['lugares']) else ""
                                )
                                
                                # Botón para guardar cambios
                                submitted = st.form_submit_button("Guardar Cambios")
                                
                                if submitted:
                                    try:
                                        # Actualizar o insertar en datosejecucion
                                        check_query = text("""
                                            SELECT id FROM siciap.datosejecucion 
                                            WHERE id_llamado = :id_llamado
                                        """)

                                        with conn.engine.connect() as connection:
                                            result = connection.execute(check_query, {"id_llamado": llamado_seleccionado}).fetchone()
                                            
                                            if result:
                                                # Actualizar registro existente
                                                update_query = text("""
                                                    UPDATE siciap.datosejecucion
                                                    SET numero_contrato = :numero_contrato,
                                                        dirigido_a = :dirigido_a,
                                                        lugares = :lugares,
                                                        proveedor = :proveedor
                                                    WHERE id_llamado = :id_llamado
                                                """)
                                                
                                                connection.execute(update_query, {
                                                    "id_llamado": llamado_seleccionado,
                                                    "numero_contrato": numero_contrato,
                                                    "dirigido_a": dirigido_a,
                                                    "lugares": lugares,
                                                    "proveedor": proveedor_seleccionado
                                                })
                                            else:
                                                # Insertar nuevo registro
                                                insert_query = text("""
                                                    INSERT INTO siciap.datosejecucion
                                                        (id_llamado, licitacion, proveedor, numero_contrato, dirigido_a, lugares)
                                                    VALUES (
                                                        :id_llamado, 
                                                        :licitacion, 
                                                        :proveedor, 
                                                        :numero_contrato, 
                                                        :dirigido_a, 
                                                        :lugares
                                                    )
                                                """)
                                                
                                                connection.execute(insert_query, {
                                                    "id_llamado": llamado_seleccionado,
                                                    "licitacion": contrato_actual['licitacion'],
                                                    "proveedor": proveedor_seleccionado,
                                                    "numero_contrato": numero_contrato,
                                                    "dirigido_a": dirigido_a,
                                                    "lugares": lugares
                                                })
                                            
                                            # Commit los cambios
                                            connection.commit()

                                        
                                        st.success(f"Información del contrato actualizada correctamente.")
                                        st.experimental_rerun()
                                    except Exception as e:
                                        st.error(f"Error al actualizar contrato: {str(e)}")
                else:
                    st.info("No hay contratos registrados.")
            except Exception as e:
                st.error(f"Error al mostrar contratos: {str(e)}")
                st.error(traceback.format_exc())

        # TAB 3: GESTIÓN DE ITEMS
        with tab3:
            st.subheader("Gestión de Items")
            st.info("Aquí puedes ver los items de cada contrato.")
            
            # Mostrar items con información de ejecución
            try:
                items_query = """
                SELECT
                    e.id_llamado,
                    e.licitacion, 
                    e.proveedor,
                    e.codigo,
                    e.medicamento as producto,
                    e.cantidad_maxima,
                    e.cantidad_emitida,
                    e.porcentaje_emitido,
                    CASE 
                        WHEN d.fecha_fin IS NULL THEN 'Indeterminado'
                        WHEN d.fecha_fin = 'CUMPLIMIENTO TOTAL DE LAS OBLIGACIONES' THEN 'Sí'
                        WHEN d.fecha_fin = 'cumplimiento total de las obligaciones' THEN 'Sí'
                        ELSE 'Sí'
                    END as vigente
                FROM siciap.ejecucion e
                LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
                WHERE e.codigo IS NOT NULL
                ORDER BY e.id_llamado, e.proveedor, e.codigo
                """
                items_df = pd.read_sql_query(text(items_query), conn.engine)
                
                if not items_df.empty:
                    # Formatear porcentaje
                    if 'porcentaje_emitido' in items_df.columns:
                        items_df['porcentaje_emitido'] = items_df['porcentaje_emitido'].apply(
                            lambda x: f"{x:.1f}%" if pd.notna(x) else "0.0%"
                        )
                    
                    # Filtros para items
                    col1, col2 = st.columns(2)
                    with col1:
                        # Filtro por ID de llamado
                        llamado_ids = items_df['id_llamado'].unique().tolist()
                        llamado_ids.insert(0, "Todos")  # Añadir opción "Todos"
                        filtro_llamado = st.selectbox(
                            "Filtrar por ID Llamado:",
                            options=llamado_ids
                        )
                    
                    with col2:
                        # Filtro por vigencia
                        filtro_vigencia = st.selectbox(
                            "Filtrar por Vigencia:",
                            options=["Todos", "Sí", "No", "Indeterminado"]
                        )
                    
                    # Aplicar filtros
                    filtered_df = items_df.copy()
                    if filtro_llamado != "Todos":
                        filtered_df = filtered_df[filtered_df['id_llamado'] == filtro_llamado]
                    
                    if filtro_vigencia != "Todos":
                        filtered_df = filtered_df[filtered_df['vigente'] == filtro_vigencia]
                    
                    # Mostrar dataframe filtrado
                    st.dataframe(filtered_df, use_container_width=True)
                else:
                    st.info("No hay items registrados.")
            except Exception as e:
                st.error(f"Error al mostrar items: {str(e)}")
                st.error(traceback.format_exc())

        # TAB 4: CARGAR DATOS
        with tab4:
            st.subheader("Cargar Datos de Contratos y Llamados")
            st.info("Aquí puedes cargar información adicional sobre contratos y llamados.")
            
            # Formulario para cargar datos nuevos
            with st.form(key="cargar_datos_ejecucion"):
                st.markdown("### Datos del Llamado")
                
                # Selección del ID de llamado (opciones de los ya existentes)
                llamados_query = """
                SELECT DISTINCT id_llamado, licitacion 
                FROM siciap.ejecucion
                WHERE id_llamado IS NOT NULL
                ORDER BY id_llamado DESC
                """
                llamados_opciones = pd.read_sql_query(text(llamados_query), conn.engine)
                
                if not llamados_opciones.empty:
                    id_llamado = st.selectbox(
                        "ID de Llamado",
                        options=llamados_opciones['id_llamado'].tolist(),
                        format_func=lambda x: f"ID: {x} - {llamados_opciones[llamados_opciones['id_llamado']==x]['licitacion'].values[0]}"
                    )
                    
                    # Obtener licitación automáticamente
                    licitacion = llamados_opciones[llamados_opciones['id_llamado']==id_llamado]['licitacion'].values[0]
                    st.text(f"Licitación: {licitacion}")
                    
                    # Otros datos del llamado
                    descripcion = st.text_area("Descripción del Llamado")
                    
                    # Datos de contrato
                    st.markdown("### Datos del Contrato")
                    
                    # Obtener proveedores de este llamado
                    proveedores_query = """
                    SELECT DISTINCT proveedor 
                    FROM siciap.ejecucion 
                    WHERE id_llamado = :id_llamado
                    ORDER BY proveedor
                    """
                    proveedores_opciones = pd.read_sql_query(
                        text(proveedores_query), 
                        conn.engine, 
                        params={"id_llamado": id_llamado}
                    )
                    
                    if not proveedores_opciones.empty:
                        proveedor = st.selectbox(
                            "Proveedor",
                            options=proveedores_opciones['proveedor'].tolist()
                        )
                        
                        numero_contrato = st.text_input("Número de Contrato")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            fecha_inicio = st.date_input("Fecha de Inicio del Contrato")
                        with col2:
                            fecha_fin = st.date_input("Fecha de Fin del Contrato")
                        
                        dirigido_a = st.text_input("Dirigido a", placeholder="Ej: Hospitales, Regional")
                        
                        # Sección para lugares
                        st.markdown("### Lugares de Entrega")
                        st.info("Ingresa un lugar por línea")
                        lugares = st.text_area("Lugares de Entrega", placeholder="Lugar 1\nLugar 2\nLugar 3")
                        
                        # Verificación de datos existentes
                        check_existing = st.checkbox("Verificar datos existentes antes de guardar")
                        
                        # Botón de envío
                        submitted = st.form_submit_button("Guardar Datos")
                        
                        if submitted:
                            try:
                                # Si se solicita verificación
                                if check_existing:
                                    existing_query = """
                                    SELECT * FROM siciap.datosejecucion
                                    WHERE id_llamado = :id_llamado
                                    """
                                    existing_data = pd.read_sql_query(
                                        text(existing_query), 
                                        conn.engine, 
                                        params={"id_llamado": id_llamado}
                                    )
                                    
                                    if not existing_data.empty:
                                        st.warning("⚠️ Ya existen datos para este llamado en la base de datos.")
                                        st.dataframe(existing_data)
                                        
                                        # Confirmar sobrescritura
                                        if st.button("Confirmar sobrescritura"):
                                            save_data = True
                                        else:
                                            save_data = False
                                    else:
                                        save_data = True
                                else:
                                    save_data = True
                                
                                # Guardar datos si corresponde
                                if save_data:
                                    # Verificar si ya existe el registro
                                    check_query = text("""
                                        SELECT id FROM siciap.datosejecucion 
                                        WHERE id_llamado = :id_llamado
                                    """)
                                    
                                    with conn.engine.connect() as connection:
                                        result = connection.execute(check_query, {"id_llamado": id_llamado}).fetchone()
                                    
                                    if result:
                                        # Actualizar registro existente
                                        update_query = text("""
                                            UPDATE siciap.datosejecucion
                                            SET licitacion = :licitacion,
                                                proveedor = :proveedor,
                                                descripcion_llamado = :descripcion,
                                                numero_contrato = :numero_contrato,
                                                fecha_inicio = :fecha_inicio,
                                                fecha_fin = :fecha_fin,
                                                dirigido_a = :dirigido_a,
                                                lugares = :lugares
                                            WHERE id_llamado = :id_llamado
                                        """)
                                        
                                        conn.engine.execute(update_query, {
                                            "licitacion": licitacion,
                                            "proveedor": proveedor,
                                            "descripcion": descripcion,
                                            "numero_contrato": numero_contrato,
                                            "fecha_inicio": fecha_inicio,
                                            "fecha_fin": fecha_fin,
                                            "dirigido_a": dirigido_a,
                                            "lugares": lugares,
                                            "id_llamado": id_llamado
                                        })
                                        
                                        st.success(f"Datos actualizados correctamente para el llamado ID {id_llamado}.")
                                    else:
                                        # Insertar nuevo registro
                                        insert_query = text("""
                                            INSERT INTO siciap.datosejecucion 
                                            (id_llamado, licitacion, proveedor, descripcion_llamado, 
                                            numero_contrato, fecha_inicio, fecha_fin, dirigido_a, lugares)
                                            VALUES 
                                            (:id_llamado, :licitacion, :proveedor, :descripcion, 
                                            :numero_contrato, :fecha_inicio, :fecha_fin, :dirigido_a, :lugares)
                                        """)
                                        
                                        conn.engine.execute(insert_query, {
                                            "id_llamado": id_llamado,
                                            "licitacion": licitacion,
                                            "proveedor": proveedor,
                                            "descripcion": descripcion,
                                            "numero_contrato": numero_contrato,
                                            "fecha_inicio": fecha_inicio,
                                            "fecha_fin": fecha_fin,
                                            "dirigido_a": dirigido_a,
                                            "lugares": lugares
                                        })
                                        
                                        st.success(f"Datos guardados correctamente para el llamado ID {id_llamado}.")
                            except Exception as e:
                                st.error(f"Error al guardar datos: {str(e)}")
                                st.error(traceback.format_exc())
                    else:
                        st.error("No se encontraron proveedores para este llamado.")
                else:
                    st.error("No hay llamados disponibles en la base de datos.")
                
            # Opción para cargar datos por CSV/Excel
            st.markdown("---")
            st.subheader("Cargar Datos desde Excel")
            
            uploaded_file = st.file_uploader(
                "Selecciona un archivo Excel con datos de contratos", 
                type=["xlsx", "xls"]
            )
            
            if uploaded_file:
                try:
                    # Leer el archivo Excel
                    df = read_excel_robust(BytesIO(uploaded_file.getvalue()), uploaded_file.name)
                    
                    if df is not None:
                        st.write("Vista previa de los datos:")
                        st.dataframe(df.head(), use_container_width=True)
                        
                        # Verificar si las columnas mínimas están presentes
                        required_cols = ['id_llamado', 'licitacion', 'descripcion_llamado', 'numero_contrato']
                        missing_cols = [col for col in required_cols if col not in df.columns]
                        
                        if missing_cols:
                            st.warning(f"Faltan columnas requeridas: {', '.join(missing_cols)}")
                            
                            # Opción para mapear columnas
                            st.subheader("Mapear Columnas")
                            
                            # Crear un mapeo de columnas
                            col_mapping = {}
                            for req_col in required_cols:
                                if req_col in missing_cols:
                                    col_mapping[req_col] = st.selectbox(
                                        f"Selecciona la columna para '{req_col}':",
                                        options=[''] + df.columns.tolist()
                                    )
                            
                            # Botón para aplicar mapeo
                            if st.button("Aplicar Mapeo y Cargar"):
                                # Crear nuevo DataFrame con columnas mapeadas
                                df_mapped = pd.DataFrame()
                                
                                for req_col in required_cols:
                                    if req_col in df.columns:
                                        df_mapped[req_col] = df[req_col]
                                    elif col_mapping.get(req_col):
                                        df_mapped[req_col] = df[col_mapping[req_col]]
                                    else:
                                        df_mapped[req_col] = None
                                
                                # Cargar datos mapeados a la base de datos
                                cargar_datos_desde_df(conn, df_mapped)
                        else:
                            # Botón para cargar directamente
                            if st.button("Cargar Datos a la Base de Datos"):
                                cargar_datos_desde_df(conn, df)
                    else:
                        st.error("No se pudo leer el archivo Excel.")
                except Exception as e:
                    st.error(f"Error al procesar el archivo: {str(e)}")
                    st.error(traceback.format_exc())

    except Exception as e:
        st.error(f"Error general: {str(e)}")
        st.error(traceback.format_exc())
    finally:
        # Cerrar conexión a la base de datos
        conn.close()

def ensure_datosejecucion_table(conn):
    """Asegura que la tabla datosejecucion existe con la estructura correcta"""
    try:
        with conn.conn.cursor() as cursor:
            # Crear esquema si no existe
            cursor.execute("CREATE SCHEMA IF NOT EXISTS siciap")
            
            # Crear tabla datosejecucion si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS siciap.datosejecucion (
                    id SERIAL PRIMARY KEY,
                    id_llamado INTEGER,
                    licitacion VARCHAR(255),
                    proveedor VARCHAR(255),
                    descripcion_llamado TEXT,
                    numero_contrato VARCHAR(255),
                    fecha_inicio DATE,
                    fecha_fin DATE,
                    dirigido_a VARCHAR(100),
                    lugares TEXT
                )
            """)
            
            # Crear índice para búsqueda rápida
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_datosejecucion_id_llamado 
                ON siciap.datosejecucion(id_llamado)
            """)
            
            conn.conn.commit()
            logger.info("Tabla datosejecucion verificada/creada correctamente")
            return True
    except Exception as e:
        logger.error(f"Error al verificar tabla datosejecucion: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def cargar_datos_desde_df(conn, df):
    """Carga datos a la tabla datosejecucion desde un DataFrame - CORREGIDO para SQLAlchemy 2.0"""
    try:
        # Verificar si hay datos para importar
        if df.empty:
            st.error("No hay datos para importar en el DataFrame")
            return False
            
        # Limpiar y convertir datos
        for col in df.columns:
            if col in ['fecha_inicio', 'fecha_fin']:
                # Convertir fechas usando la función segura
                df[col] = safe_date_conversion(df[col])
        
        # Contador de éxitos y errores
        exitos = 0
        errores = 0
        
        # Procesar cada fila
        for _, row in df.iterrows():
            try:
                # Verificar si ya existe el registro para este id_llamado
                check_query = text("""
                    SELECT id FROM siciap.datosejecucion 
                    WHERE id_llamado = :id_llamado
                """)
                
                # Usar with para gestionar la conexión
                with conn.engine.connect() as connection:
                    result = connection.execute(check_query, {"id_llamado": row['id_llamado']}).fetchone()
                
                if result:
                    # Preparar datos para actualizar
                    update_data = {
                        "id_llamado": row['id_llamado']
                    }
                    
                    # Añadir solo columnas que existen en el DataFrame
                    for col in ['licitacion', 'proveedor', 'descripcion_llamado', 
                               'numero_contrato', 'fecha_inicio', 'fecha_fin', 
                               'dirigido_a', 'lugares']:
                        if col in df.columns:
                            update_data[col] = row[col]
                    
                    # Construir consulta dinámica para actualizar solo las columnas disponibles
                    cols_to_update = [f"{col} = :{col}" for col in update_data.keys() if col != 'id_llamado']
                    
                    if cols_to_update:
                        update_query = text(f"""
                            UPDATE siciap.datosejecucion
                            SET {', '.join(cols_to_update)}
                            WHERE id_llamado = :id_llamado
                        """)
                        
                        # Ejecutar con la conexión y hacer commit
                        with conn.engine.connect() as connection:
                            connection.execute(update_query, update_data)
                            connection.commit()  # Importante: commit para guardar cambios
                else:
                    # Preparar datos para insertar
                    insert_data = {
                        "id_llamado": row['id_llamado']
                    }
                    
                    # Añadir solo columnas que existen en el DataFrame
                    for col in ['licitacion', 'proveedor', 'descripcion_llamado', 
                               'numero_contrato', 'fecha_inicio', 'fecha_fin', 
                               'dirigido_a', 'lugares']:
                        if col in df.columns:
                            insert_data[col] = row[col]
                    
                    # Construir consulta dinámica para insertar solo las columnas disponibles
                    cols = list(insert_data.keys())
                    placeholders = [f":{col}" for col in cols]
                    
                    insert_query = text(f"""
                        INSERT INTO siciap.datosejecucion 
                        ({', '.join(cols)})
                        VALUES 
                        ({', '.join(placeholders)})
                    """)
                    
                    # Ejecutar con la conexión y hacer commit
                    with conn.engine.connect() as connection:
                        connection.execute(insert_query, insert_data)
                        connection.commit()  # Importante: commit para guardar cambios
                
                exitos += 1
            except Exception as e:
                logger.error(f"Error en fila {_}: {str(e)}")
                errores += 1
        
        # Mostrar resultados
        if exitos > 0:
            st.success(f"✅ {exitos} registros importados correctamente")
        
        if errores > 0:
            st.warning(f"⚠️ {errores} registros con errores")
            
        return exitos > 0
    except Exception as e:
        st.error(f"Error al cargar datos: {str(e)}")
        st.error(traceback.format_exc())
        return False

def ensure_database_structure(conn):
    """Asegura que la estructura de la base de datos sea correcta"""
    try:
        with conn.conn.cursor() as cursor:
            # Verificar y crear la tabla datosejecucion si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS siciap.datosejecucion (
                    id_llamado INTEGER PRIMARY KEY,
                    licitacion VARCHAR(255),
                    proveedor VARCHAR(255),
                    descripcion_llamado TEXT,
                    numero_contrato VARCHAR(100),
                    fecha_inicio DATE,
                    fecha_fin DATE,
                    dirigido_a VARCHAR(100),
                    lugares TEXT
                );
            """)
            
            conn.conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error al verificar estructura de BD: {str(e)}")
        return False

def crear_indices_siciap():
    try:
        # Configuración de base de datos
        db_config = get_db_config()
        engine_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        engine = create_engine(engine_string)
        
        with engine.connect() as conn:
            # Crear índices para mejorar rendimiento
            indices = [
                "CREATE INDEX IF NOT EXISTS idx_ordenes_codigo ON siciap.ordenes(codigo)",
                "CREATE INDEX IF NOT EXISTS idx_ordenes_fecha_oc ON siciap.ordenes(fecha_oc)",
                "CREATE INDEX IF NOT EXISTS idx_ordenes_estado ON siciap.ordenes(estado)",
                "CREATE INDEX IF NOT EXISTS idx_ejecucion_codigo ON siciap.ejecucion(codigo)",
                "CREATE INDEX IF NOT EXISTS idx_ejecucion_item ON siciap.ejecucion(item)",
                "CREATE INDEX IF NOT EXISTS idx_stock_critico_codigo ON siciap.stock_critico(codigo)",
                "CREATE INDEX IF NOT EXISTS idx_stock_critico_dmp ON siciap.stock_critico(dmp)"
            ]
            
            for idx_sql in indices:
                conn.execute(text(idx_sql))
                print(f"Índice creado: {idx_sql}")

        return True
    except Exception as e:
        print(f"Error al crear índices: {str(e)}")
        traceback.print_exc()
        return False

def buscar_producto_integrado(codigo):
    """
    Función optimizada para buscar información integrada de un producto por su código
    utilizando la relación directa entre las tres tablas a través del campo código.
    """
    try:
        # Configuración de base de datos
        db_config = get_db_config()
        engine_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        engine = create_engine(engine_string)
        
        # Resultados
        resultado = {
            "producto": None,
            "ordenes": None,
            "ejecucion": None
        }
        
        with engine.connect() as conn:
            # 1. Información del producto desde stock_critico
            query = text("""
                SELECT 
                    codigo, 
                    producto, 
                    concentracion, 
                    forma_farmaceutica, 
                    presentacion, 
                    clasificacion, 
                    stock_actual, 
                    stock_reservado, 
                    stock_disponible, 
                    dmp, 
                    estado_stock,
                    CASE 
                        WHEN stock_disponible IS NULL OR dmp IS NULL OR dmp = 0 THEN 'Sin datos'
                        WHEN stock_disponible < dmp * 0.3 THEN 'Crítico'
                        WHEN stock_disponible < dmp * 0.7 THEN 'Bajo'
                        ELSE 'Normal'
                    END AS nivel_criticidad
                FROM 
                    siciap.stock_critico 
                WHERE 
                    codigo = :codigo
            """)
            
            producto_result = conn.execute(query, {"codigo": codigo}).fetchone()
            
            if producto_result:
                # Convertir a diccionario
                cols = conn.execute(query, {"codigo": codigo}).keys()
                producto_dict = dict(zip(cols, producto_result))
                resultado["producto"] = producto_dict

                # 2. Información de órdenes de compra
                query = text("""
                    SELECT 
                        oc, 
                        fecha_oc, 
                        proveedor, 
                        lugar_entrega_oc, 
                        llamado, 
                        cant_oc, 
                        p_unit, 
                        monto_oc, 
                        cant_recep, 
                        COALESCE(monto_recepcion, "monto_recepci_n"::numeric) AS monto_recepcion,
                        saldo, 
                        monto_saldo, 
                        estado, 
                        fec_ult_recep, 
                        dias_de_atraso,
                        plazo_entrega,
                        COALESCE(fecha_recibido_proveedor, fecha_recibido_poveedor::date) AS fecha_recibido_proveedor
                    FROM 
                        siciap.ordenes 
                    WHERE 
                        codigo = :codigo
                    ORDER BY 
                        fecha_oc DESC
                """)
                
                ordenes_result = conn.execute(query, {"codigo": codigo}).fetchall()
                
                if ordenes_result:
                    # Convertir a lista de diccionarios
                    cols = conn.execute(query, {"codigo": codigo}).keys()
                    ordenes_list = []
                    for row in ordenes_result:
                        ordenes_list.append(dict(zip(cols, row)))
                    resultado["ordenes"] = ordenes_list

                # 3. Información de ejecución
                query = text("""
                    SELECT 
                        codigo,
                        item, 
                        cantidad_maxima, 
                        cantidad_emitida, 
                        cantidad_recepcionada, 
                        cantidad_distribuida, 
                        monto_adjudicado, 
                        monto_emitido, 
                        saldo, 
                        porcentaje_emitido, 
                        estado_stock, 
                        estado_contrato
                    FROM 
                        siciap.ejecucion 
                    WHERE 
                        codigo = :codigo
                    OR 
                        item LIKE :codigo_pattern
                """)
                
                ejecucion_result = conn.execute(query, {
                    "codigo": codigo,
                    "codigo_pattern": f"%{codigo}%"
                }).fetchall()
                
                if ejecucion_result:
                    # Convertir a lista de diccionarios
                    cols = conn.execute(query, {
                        "codigo": codigo,
                        "codigo_pattern": f"%{codigo}%"
                    }).keys()
                    ejecucion_list = []
                    for row in ejecucion_result:
                        ejecucion_list.append(dict(zip(cols, row)))
                    resultado["ejecucion"] = ejecucion_list

        return resultado

    except Exception as e:
        print(f"Error en buscar_producto_integrado: {str(e)}")
        traceback.print_exc()
        return {"error": str(e)}

def exportar_datos_ejecucion():
    """
    Función mejorada para exportar datos con opciones de filtrado y selección de campos
    Maneja campos de fecha que pueden contener texto como 'cumplimiento total de las obligaciones'
    """
    st.header("🔽 Exportación de Datos de Ejecución")
    st.markdown("### Exporta datos personalizados para DBeaver u otras herramientas")
    
    # Conexión a la base de datos
    db_config = get_db_config()
    conn = PostgresConnection(**db_config)
    
    if not conn.connect():
        st.error("❌ No se pudo conectar a PostgreSQL.")
        return
    
    try:
        # Primero, obtener las columnas reales de las tablas
        query_columnas_ejecucion = """
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_schema = 'siciap' 
        AND table_name = 'ejecucion'
        ORDER BY ordinal_position
        """
        
        query_columnas_datosejecucion = """
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_schema = 'siciap' 
        AND table_name = 'datosejecucion'
        ORDER BY ordinal_position
        """
        
        # Obtener columnas disponibles con sus tipos
        df_cols_ejec = pd.read_sql_query(text(query_columnas_ejecucion), conn.engine)
        df_cols_datos = pd.read_sql_query(text(query_columnas_datosejecucion), conn.engine)
        
        columnas_ejecucion = df_cols_ejec['column_name'].tolist()
        columnas_datosejecucion = df_cols_datos['column_name'].tolist()
        
        # Mostrar columnas disponibles para debug
        with st.expander("🔍 Ver columnas disponibles (para debug)"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Tabla ejecucion:**")
                for idx, row in df_cols_ejec.iterrows():
                    st.write(f"- {row['column_name']} ({row['data_type']})")
            with col2:
                st.write("**Tabla datosejecucion:**")
                for idx, row in df_cols_datos.iterrows():
                    st.write(f"- {row['column_name']} ({row['data_type']})")
            
            # Mostrar columnas duplicadas
            columnas_duplicadas = set(columnas_ejecucion).intersection(set(columnas_datosejecucion))
            if columnas_duplicadas:
                st.warning(f"⚠️ Columnas duplicadas entre tablas: {list(columnas_duplicadas)}")
        
        # PASO 1: Selección del tipo de exportación
        st.subheader("1️⃣ Tipo de Exportación")
        col1, col2 = st.columns(2)
        
        with col1:
            tipo_exportacion = st.radio(
                "¿Qué tipo de datos deseas exportar?",
                ["Solo nombres/licitaciones", "Contratos", "Vigencias", "Datos completos", "Personalizado"],
                help="""
                - **Solo nombres**: ID llamado y descripción
                - **Contratos**: Licitación, proveedor, items, códigos
                - **Vigencias**: Fechas y estados de contratos
                - **Datos completos**: Todos los campos (con prefijos para evitar duplicados)
                - **Personalizado**: Selecciona campos específicos
                """
            )
        
        # PASO 2: Filtros opcionales
        st.subheader("2️⃣ Filtros (Opcional)")
        
        with st.expander("🔍 Aplicar filtros", expanded=False):
            col_filter1, col_filter2, col_filter3 = st.columns(3)
            
            with col_filter1:
                # Obtener lista de ID llamados únicos
                query_ids = "SELECT DISTINCT id_llamado FROM siciap.ejecucion ORDER BY id_llamado"
                df_ids = pd.read_sql_query(text(query_ids), conn.engine)
                
                filtrar_por_id = st.checkbox("Filtrar por ID Llamado")
                if filtrar_por_id:
                    ids_seleccionados = st.multiselect(
                        "Selecciona los ID Llamado:",
                        options=df_ids['id_llamado'].tolist(),
                        default=[]
                    )
            
            with col_filter2:
                # Filtro por proveedor
                query_proveedores = "SELECT DISTINCT proveedor FROM siciap.ejecucion WHERE proveedor IS NOT NULL ORDER BY proveedor"
                df_proveedores = pd.read_sql_query(text(query_proveedores), conn.engine)
                
                filtrar_por_proveedor = st.checkbox("Filtrar por Proveedor")
                if filtrar_por_proveedor:
                    proveedores_seleccionados = st.multiselect(
                        "Selecciona proveedores:",
                        options=df_proveedores['proveedor'].tolist(),
                        default=[]
                    )
            
            with col_filter3:
                # Filtro por rango de fechas (solo si existen campos de fecha)
                if 'fecha_inicio' in columnas_datosejecucion and 'fecha_fin' in columnas_datosejecucion:
                    filtrar_por_fecha = st.checkbox("Filtrar por Rango de Fechas")
                    if filtrar_por_fecha:
                        fecha_inicio = st.date_input("Fecha inicio")
                        fecha_fin = st.date_input("Fecha fin")
                        
                        # Opción para incluir contratos con texto especial
                        incluir_texto_especial = st.checkbox(
                            "Incluir contratos con 'cumplimiento total de las obligaciones'",
                            value=True,
                            help="Marca esta opción para incluir contratos que tengan texto especial en lugar de fechas"
                        )
        
        # PASO 3: Configuración de campos según el tipo
        campos_seleccionados = []
        
        if tipo_exportacion == "Solo nombres/licitaciones":
            campos_seleccionados = [
                "id_llamado",
                "descripcion_llamado"
            ]
            query_campos = """
            SELECT DISTINCT
                e.id_llamado,
                d.descripcion_llamado
            FROM siciap.ejecucion e
            LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
            """
            
        elif tipo_exportacion == "Contratos":
            # Verificar qué campos existen realmente
            campos_contratos = []
            campos_labels = []
            
            # Campos básicos que deberían existir
            campos_contratos.append("e.id_llamado")
            campos_labels.append("id_llamado")
            
            if 'descripcion_llamado' in columnas_datosejecucion:
                campos_contratos.append("d.descripcion_llamado")
                campos_labels.append("descripcion_llamado")
            
            if 'proveedor' in columnas_ejecucion:
                campos_contratos.append("e.proveedor")
                campos_labels.append("proveedor")
            
            if 'item' in columnas_ejecucion:
                campos_contratos.append("e.item")
                campos_labels.append("item")
            
            if 'codigo' in columnas_ejecucion:
                campos_contratos.append("e.codigo")
                campos_labels.append("codigo")
            
            if 'medicamento' in columnas_ejecucion:
                campos_contratos.append("e.medicamento")
                campos_labels.append("medicamento")
            
            # Buscar columnas de precio
            precio_cols = [col for col in columnas_ejecucion if 'precio' in col.lower() or 'unitario' in col.lower()]
            if precio_cols:
                campos_contratos.append(f"e.{precio_cols[0]}")
                campos_labels.append(precio_cols[0])
            
            # Buscar columnas de cantidad
            cantidad_cols = [col for col in columnas_ejecucion if 'cantidad' in col.lower()]
            if cantidad_cols:
                campos_contratos.append(f"e.{cantidad_cols[0]}")
                campos_labels.append(cantidad_cols[0])
            
            campos_seleccionados = campos_labels
            
            query_campos = f"""
            SELECT 
                {', '.join(campos_contratos)}
            FROM siciap.ejecucion e
            LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
            """
            
        elif tipo_exportacion == "Vigencias":
            # Verificar qué campos existen para vigencias
            campos_vigencias = []
            campos_labels = []
            
            campos_vigencias.append("e.id_llamado")
            campos_labels.append("id_llamado")
            
            if 'descripcion_llamado' in columnas_datosejecucion:
                campos_vigencias.append("d.descripcion_llamado")
                campos_labels.append("descripcion_llamado")
            
            if 'proveedor' in columnas_ejecucion:
                campos_vigencias.append("e.proveedor")
                campos_labels.append("proveedor")
            
            if 'numero_contrato' in columnas_datosejecucion:
                campos_vigencias.append("d.numero_contrato")
                campos_labels.append("numero_contrato")
            
            # Campos de fecha con manejo especial
            if 'fecha_inicio' in columnas_datosejecucion:
                campos_vigencias.append("d.fecha_inicio")
                campos_labels.append("fecha_inicio")
            
            if 'fecha_fin' in columnas_datosejecucion:
                # Convertir fecha_fin a texto para manejar valores mixtos
                campos_vigencias.append("d.fecha_fin::TEXT")
                campos_labels.append("fecha_fin")
                
                # Agregar estado calculado considerando texto especial
                campos_vigencias.append("""
                CASE 
                    WHEN UPPER(d.fecha_fin::TEXT) LIKE '%CUMPLIMIENTO TOTAL%' THEN 'Cumplimiento total'
                    WHEN d.fecha_fin::TEXT ~ '^\d{4}-\d{2}-\d{2}' AND d.fecha_fin::DATE < CURRENT_DATE THEN 'Vencido'
                    WHEN d.fecha_fin::TEXT ~ '^\d{4}-\d{2}-\d{2}' AND d.fecha_fin::DATE >= CURRENT_DATE THEN 'Vigente'
                    ELSE 'Sin fecha definida'
                END""")
                campos_labels.append("estado")
            
            # Si existe campo vigencia, incluirlo también
            if 'vigencia' in columnas_datosejecucion:
                campos_vigencias.append("d.vigencia")
                campos_labels.append("vigencia")
            
            campos_seleccionados = campos_labels
            
            query_campos = f"""
            SELECT DISTINCT
                {', '.join(campos_vigencias)}
            FROM siciap.ejecucion e
            LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
            """
            
        elif tipo_exportacion == "Datos completos":
            # Construir query con todos los campos, pero evitando duplicados
            
            # Primero, obtener todos los campos de ejecucion con alias
            campos_ejecucion_alias = [f"e.{col} as e_{col}" for col in columnas_ejecucion]
            
            # Luego, obtener campos de datosejecucion con manejo de duplicados
            campos_datosejecucion_alias = []
            
            # Identificar columnas que están en ambas tablas
            columnas_duplicadas = set(columnas_ejecucion).intersection(set(columnas_datosejecucion))
            
            for col in columnas_datosejecucion:
                if col in columnas_duplicadas:
                    # Si la columna está duplicada, agregarle un alias con prefijo 'd_'
                    campos_datosejecucion_alias.append(f"d.{col} as d_{col}")
                else:
                    # Si no está duplicada, mantener el nombre original
                    campos_datosejecucion_alias.append(f"d.{col}")
            
            # Combinar todos los campos
            todos_los_campos = campos_ejecucion_alias + campos_datosejecucion_alias
            
            query_campos = f"""
            SELECT 
                {', '.join(todos_los_campos)}
            FROM siciap.ejecucion e
            LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
            """
            
        elif tipo_exportacion == "Personalizado":
            st.subheader("3️⃣ Selección de Campos")
            
            st.write("**Campos de Ejecución:**")
            col_campos1, col_campos2 = st.columns(2)
            
            campos_seleccionados_e = []
            for i, campo in enumerate(columnas_ejecucion):
                if i % 2 == 0:
                    with col_campos1:
                        if st.checkbox(campo, key=f"e_{campo}"):
                            campos_seleccionados_e.append(f"e.{campo}")
                else:
                    with col_campos2:
                        if st.checkbox(campo, key=f"e_{campo}"):
                            campos_seleccionados_e.append(f"e.{campo}")
            
            st.write("**Campos de Datos de Ejecución:**")
            col_campos3, col_campos4 = st.columns(2)
            
            campos_seleccionados_d = []
            for i, campo in enumerate(columnas_datosejecucion):
                if campo != 'id_llamado':  # Evitar duplicados
                    if i % 2 == 0:
                        with col_campos3:
                            if st.checkbox(campo, key=f"d_{campo}"):
                                campos_seleccionados_d.append(f"d.{campo}")
                    else:
                        with col_campos4:
                            if st.checkbox(campo, key=f"d_{campo}"):
                                campos_seleccionados_d.append(f"d.{campo}")
            
            campos_query = campos_seleccionados_e + campos_seleccionados_d
            
            if not campos_query:
                st.warning("Por favor selecciona al menos un campo")
                return
            
            query_campos = f"""
            SELECT {', '.join(campos_query)}
            FROM siciap.ejecucion e
            LEFT JOIN siciap.datosejecucion d ON e.id_llamado = d.id_llamado
            """
        
        # Aplicar filtros si están activos
        where_conditions = []
        
        if 'filtrar_por_id' in locals() and filtrar_por_id and 'ids_seleccionados' in locals() and ids_seleccionados:
            ids_str = "', '".join(ids_seleccionados)
            where_conditions.append(f"e.id_llamado IN ('{ids_str}')")
        
        if 'filtrar_por_proveedor' in locals() and filtrar_por_proveedor and 'proveedores_seleccionados' in locals() and proveedores_seleccionados:
            provs_str = "', '".join([p.replace("'", "''") for p in proveedores_seleccionados])
            where_conditions.append(f"e.proveedor IN ('{provs_str}')")
        
        if 'filtrar_por_fecha' in locals() and filtrar_por_fecha:
            if 'incluir_texto_especial' in locals() and incluir_texto_especial:
                # Filtro complejo que incluye fechas válidas y texto especial
                where_conditions.append(f"""
                (
                    (d.fecha_inicio::TEXT ~ '^\d{{4}}-\d{{2}}-\d{{2}}' AND d.fecha_inicio::DATE >= '{fecha_inicio}' 
                     AND d.fecha_fin::TEXT ~ '^\d{{4}}-\d{{2}}-\d{{2}}' AND d.fecha_fin::DATE <= '{fecha_fin}')
                    OR 
                    UPPER(d.fecha_fin::TEXT) LIKE '%CUMPLIMIENTO TOTAL%'
                )
                """)
            else:
                # Filtro solo por fechas válidas
                where_conditions.append(f"""
                (d.fecha_inicio::TEXT ~ '^\d{{4}}-\d{{2}}-\d{{2}}' AND d.fecha_inicio::DATE >= '{fecha_inicio}' 
                 AND d.fecha_fin::TEXT ~ '^\d{{4}}-\d{{2}}-\d{{2}}' AND d.fecha_fin::DATE <= '{fecha_fin}')
                """)
        
        # Construir query final
        if where_conditions:
            query_campos += " WHERE " + " AND ".join(where_conditions)
        
        # Agregar ORDER BY según el tipo de exportación y campos disponibles
        if tipo_exportacion == "Solo nombres/licitaciones":
            query_campos += " ORDER BY e.id_llamado"
        elif tipo_exportacion == "Vigencias":
            order_by_parts = ["e.id_llamado"]
            if 'proveedor' in columnas_ejecucion:
                order_by_parts.append("e.proveedor")
            query_campos += " ORDER BY " + ", ".join(order_by_parts)
        else:
            order_by_parts = ["e.id_llamado"]
            if 'item' in columnas_ejecucion:
                order_by_parts.append("e.item")
            query_campos += " ORDER BY " + ", ".join(order_by_parts)
        
        # Ejecutar consulta
        st.subheader("📊 Vista Previa de Datos")
        
        # Mostrar query para debug
        with st.expander("🔧 Ver Query SQL (para debug)"):
            st.code(query_campos, language='sql')
        
        with st.spinner("Cargando datos..."):
            df = pd.read_sql_query(text(query_campos), conn.engine)
            
            if df.empty:
                st.warning("No hay datos con los filtros seleccionados.")
                return
            
            # Post-procesamiento para campos de fecha con texto
            if tipo_exportacion == "Vigencias" and 'fecha_fin' in df.columns:
                # Formatear fechas que son texto para mejor visualización
                def format_fecha_mixta(fecha_str):
                    if fecha_str and 'cumplimiento total' in str(fecha_str).lower():
                        return "Cumplimiento total de las obligaciones"
                    elif fecha_str and str(fecha_str).strip() != '':
                        try:
                            # Intentar parsear como fecha
                            fecha = pd.to_datetime(fecha_str, errors='coerce')
                            if not pd.isna(fecha):
                                return fecha.strftime('%d/%m/%Y')
                            else:
                                return fecha_str
                        except:
                            return fecha_str
                    else:
                        return "Sin fecha"
                
                df['fecha_fin'] = df['fecha_fin'].apply(format_fecha_mixta)
                
                # También formatear fecha_inicio si existe
                if 'fecha_inicio' in df.columns:
                    df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'], errors='coerce').dt.strftime('%d/%m/%Y')
            
            # Eliminar duplicados si es necesario
            if tipo_exportacion in ["Solo nombres/licitaciones", "Vigencias"]:
                df = df.drop_duplicates()
            
            st.success(f"✅ Se encontraron {len(df)} registros")
            
            # Mostrar vista previa
            st.dataframe(df.head(20), use_container_width=True)
            
            # Estadísticas rápidas
            col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
            
            with col_stat1:
                if 'id_llamado' in df.columns:
                    st.metric("Licitaciones únicas", df['id_llamado'].nunique())
            
            with col_stat2:
                if 'proveedor' in df.columns:
                    st.metric("Proveedores únicos", df['proveedor'].nunique())
            
            with col_stat3:
                st.metric("Total de registros", len(df))
            
            with col_stat4:
                if tipo_exportacion == "Vigencias" and 'estado' in df.columns:
                    cumplimiento_total = len(df[df['estado'] == 'Cumplimiento total'])
                    st.metric("Con cumplimiento total", cumplimiento_total)
        
        # PASO 4: Formato de exportación
        st.subheader("4️⃣ Formato de Exportación")
        
        col_formato1, col_formato2 = st.columns([2, 1])
        
        with col_formato1:
            formato = st.selectbox(
                "Selecciona el formato:",
                ["Excel", "CSV", "SQL Insert", "JSON"],
                help="""
                - **Excel**: Compatible con Excel, LibreOffice
                - **CSV**: Formato universal, compatible con DBeaver
                - **SQL Insert**: Script SQL para insertar datos
                - **JSON**: Formato estructurado para aplicaciones
                """
            )
        
        with col_formato2:
            # Opciones adicionales según formato
            if formato == "CSV":
                separador = st.selectbox("Separador", [",", ";", "\t", "|"])
                encoding = st.selectbox("Codificación", ["utf-8", "latin1", "cp1252"])
        
        # PASO 5: Generar y descargar
        if st.button("🚀 Generar Archivo de Exportación", type="primary"):
            with st.spinner(f"Preparando archivo en formato {formato}..."):
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if formato == "Excel":
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df.to_excel(writer, sheet_name="Datos", index=False)
                        
                        # Agregar hoja de metadatos
                        metadata = pd.DataFrame({
                            'Información': ['Fecha de exportación', 'Tipo de datos', 'Registros exportados', 'Filtros aplicados'],
                            'Valor': [
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                tipo_exportacion,
                                len(df),
                                'Sí' if where_conditions else 'No'
                            ]
                        })
                        metadata.to_excel(writer, sheet_name="Información", index=False)
                    
                    data = output.getvalue()
                    filename = f"siciap_export_{tipo_exportacion.lower().replace(' ', '_')}_{timestamp}.xlsx"
                    mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                
                elif formato == "CSV":
                    csv_string = df.to_csv(
                        index=False, 
                        sep=separador if 'separador' in locals() else ',',
                        encoding=encoding if 'encoding' in locals() else 'utf-8'
                    )
                    data = csv_string.encode(encoding if 'encoding' in locals() else 'utf-8')
                    filename = f"siciap_export_{tipo_exportacion.lower().replace(' ', '_')}_{timestamp}.csv"
                    mime = "text/csv"
                
                elif formato == "SQL Insert":
                    inserts = []
                    table_name = "siciap.datos_exportados"
                    
                    # Crear statement de creación de tabla
                    create_table = f"-- Crear tabla si no existe\nCREATE TABLE IF NOT EXISTS {table_name} (\n"
                    for col in df.columns:
                        dtype = "TEXT"
                        if df[col].dtype in ['int64', 'int32']:
                            dtype = "INTEGER"
                        elif df[col].dtype in ['float64', 'float32']:
                            dtype = "NUMERIC"
                        elif 'fecha' in col.lower() or 'date' in col.lower():
                            # Para campos de fecha mixtos, usar TEXT
                            if col == 'fecha_fin' and tipo_exportacion == "Vigencias":
                                dtype = "TEXT"
                            else:
                                dtype = "DATE"
                        create_table += f"    {col} {dtype},\n"
                    create_table = create_table.rstrip(',\n') + "\n);\n\n"
                    
                    inserts.append(create_table)
                    inserts.append(f"-- Insertar {len(df)} registros\n")
                    
                    for _, row in df.iterrows():
                        cols = ", ".join([f'"{col}"' for col in df.columns])
                        values = []
                        for val in row:
                            if pd.isna(val):
                                values.append("NULL")
                            elif isinstance(val, (int, float)):
                                values.append(str(val))
                            else:
                                values.append(f"'{str(val).replace('\'', '\'\'')}'")
                        vals_str = ", ".join(values)
                        inserts.append(f"INSERT INTO {table_name} ({cols}) VALUES ({vals_str});")
                    
                    sql_content = "\n".join(inserts)
                    data = sql_content.encode()
                    filename = f"siciap_export_{tipo_exportacion.lower().replace(' ', '_')}_{timestamp}.sql"
                    mime = "text/plain"
                
                elif formato == "JSON":
                    json_str = df.to_json(orient="records", date_format="iso", force_ascii=False)
                    data = json_str.encode()
                    filename = f"siciap_export_{tipo_exportacion.lower().replace(' ', '_')}_{timestamp}.json"
                    mime = "application/json"
                
                # Botón de descarga
                st.download_button(
                    label=f"⬇️ Descargar {formato}",
                    data=data,
                    file_name=filename,
                    mime=mime,
                    key="download_export"
                )
                
                st.success(f"✅ Archivo generado: {filename}")
                
                # Instrucciones según formato
                with st.expander("📖 Instrucciones de importación"):
                    if formato == "Excel":
                        st.markdown("""
                        **Para importar en DBeaver:**
                        1. Click derecho en la tabla destino
                        2. Seleccionar 'Import Data'
                        3. Elegir 'Excel' como formato
                        4. Seleccionar este archivo
                        5. Mapear columnas y confirmar
                        """)
                    elif formato == "CSV":
                        st.markdown(f"""
                        **Para importar en DBeaver:**
                        1. Click derecho en la tabla destino
                        2. Seleccionar 'Import Data'
                        3. Configurar:
                           - Separador: `{separador if 'separador' in locals() else ','}`
                           - Encoding: `{encoding if 'encoding' in locals() else 'utf-8'}`
                        4. Mapear columnas y confirmar
                        """)
                    elif formato == "SQL Insert":
                        st.markdown("""
                        **Para ejecutar en DBeaver:**
                        1. Abrir el archivo en el editor SQL
                        2. Seleccionar toda la consulta (Ctrl+A)
                        3. Ejecutar con Ctrl+Enter
                        
                        **Nota sobre campos de fecha con texto:**
                        Si el campo fecha_fin contiene texto como 'Cumplimiento total de las obligaciones',
                        asegúrate de que la tabla destino tenga ese campo como TEXT en lugar de DATE.
                        """)
    
    except Exception as e:
        st.error(f"❌ Error al exportar datos: {str(e)}")
        st.error(traceback.format_exc())

def importar_datos_ejecucion():
    """
    Función para importar datos a la tabla datosejecucion,
    procesando correctamente campos múltiples como "lugares"
    que están separados por //
    """
    st.header("Importación de Datos para Contratos/Ejecución")
    st.markdown("### Importa datos a la tabla datosejecucion con soporte para múltiples lugares")
    
    # Instrucciones
    with st.expander("Instrucciones de uso", expanded=True):
        st.markdown("""
        ### Instrucciones para preparar el archivo de importación:
        
        1. **Formato del archivo**: El archivo debe estar en formato Excel (.xlsx) o CSV (.csv).
        
        2. **Columnas requeridas**:
           - `id_llamado`: ID del llamado (numérico)
           - `licitacion`: Nombre o número de la licitación
           - `proveedor`: Nombre del proveedor
           - `descripcion_llamado`: Descripción detallada del llamado
           - `numero_contrato`: Número del contrato
           - `fecha_inicio`: Fecha de inicio del contrato (formato DD/MM/YYYY)
           - `fecha_fin`: Fecha de fin del contrato (formato DD/MM/YYYY)
           - `dirigido_a`: A quién está dirigido el contrato
           - `lugares`: Lugares de entrega, separados por `//` si hay más de uno
        
        3. **Formato del campo lugares**: Si hay múltiples lugares, sepáralos con `//`. Por ejemplo:
           ```
           Hospital Central // Hospital Regional // Clínica San Juan
           ```
           Durante la importación, estos lugares se convertirán automáticamente al formato correcto.
        """)
    
    # Sección para cargar archivo
    st.subheader("Cargar archivo")
    uploaded_file = st.file_uploader(
        "Selecciona un archivo Excel o CSV con los datos a importar", 
        type=["xlsx", "xls", "csv"]
    )
    
    if uploaded_file:
        try:
            # Determinar tipo de archivo
            file_extension = uploaded_file.name.split('.')[-1].lower()
            
            # Leer archivo según formato
            if file_extension in ['xlsx', 'xls']:
                df = read_excel_robust(BytesIO(uploaded_file.getvalue()), uploaded_file.name)
            else:
                # Intentar con diferentes codificaciones para CSV
                try:
                    df = pd.read_csv(uploaded_file)
                except UnicodeDecodeError:
                    try:
                        df = pd.read_csv(uploaded_file, encoding='latin1')
                    except:
                        df = pd.read_csv(uploaded_file, encoding='cp1252')
            
            if df is None or df.empty:
                st.error("❌ No se pudieron leer datos del archivo.")
                return
            
            # Mostrar vista previa
            st.subheader("Vista previa de los datos")
            st.dataframe(df.head(5), use_container_width=True)
            
            # Mapeo de columnas
            st.subheader("Mapeo de columnas")
            st.markdown("Asegúrate de mapear correctamente las columnas del archivo con las columnas de la base de datos.")
            
            # Columnas requeridas en la base de datos
            db_columns = [
                'id_llamado', 'licitacion', 'proveedor', 'descripcion_llamado', 
                'numero_contrato', 'fecha_inicio', 'fecha_fin', 'dirigido_a', 'lugares'
            ]
            
            # Permitir al usuario mapear columnas
            mapping = {}
            for db_col in db_columns:
                options = [''] + list(df.columns)
                default_index = options.index(db_col) if db_col in options else 0
                
                selected_col = st.selectbox(
                    f"Mapear columna '{db_col}' con:",
                    options=options,
                    index=default_index,
                    key=f"map_{db_col}"
                )
                
                if selected_col:
                    mapping[db_col] = selected_col
            
            # Opciones de importación
            st.subheader("Opciones de importación")
            
            col1, col2 = st.columns(2)
            with col1:
                mode = st.radio(
                    "Modo de importación:",
                    ["Insertar nuevos registros", "Actualizar existentes", "Insertar y actualizar"]
                )
            
            with col2:
                process_lugares = st.checkbox(
                    "Procesar separador '//' en lugares", 
                    value=True,
                    help="Convierte lugares separados por // a formato multilínea"
                )
            
            # Botón para procesar e importar
            if st.button("Procesar e Importar Datos"):
                # Verificar mapeo
                if len(mapping) < 3:  # Al menos id_llamado, licitacion y algún otro campo
                    st.error("❌ Debes mapear al menos 'id_llamado', 'licitacion' y algún otro campo.")
                    return
                
                with st.spinner("Procesando datos para importación..."):
                    # Crear DataFrame con el mapeo correcto
                    mapped_df = pd.DataFrame()
                    
                    for db_col, file_col in mapping.items():
                        if file_col:  # Solo incluir columnas mapeadas
                            mapped_df[db_col] = df[file_col].copy()
                    
                    # Procesar campo lugares si está habilitada la opción
                    if 'lugares' in mapped_df.columns and process_lugares:
                        # Reemplazar // por saltos de línea
                        mapped_df['lugares'] = mapped_df['lugares'].apply(
                            lambda x: x.replace(' // ', '\n').replace('//', '\n') if isinstance(x, str) else x
                        )
                    
                    # Convertir fechas a formato adecuado
                    for date_col in ['fecha_inicio', 'fecha_fin']:
                        if date_col in mapped_df.columns:
                            mapped_df[date_col] = safe_date_conversion(mapped_df[date_col])
                    
                    # Conexión a la base de datos
                    db_config = get_db_config()
                    conn = PostgresConnection(**db_config)
                    
                    if not conn.connect():
                        st.error("❌ No se pudo conectar a PostgreSQL.")
                        return
                    
                    try:
                        # Asegurar que la tabla existe
                        ensure_datosejecucion_table(conn)
                        
                        # Contador de operaciones
                        inserted = 0
                        updated = 0
                        errors = 0
                        
                        # Procesar según el modo seleccionado
                        for _, row in mapped_df.iterrows():
                            try:
                                # Verificar si existe el registro
                                check_query = text("""
                                    SELECT id FROM siciap.datosejecucion 
                                    WHERE id_llamado = :id_llamado
                                """)
                                
                                id_llamado = int(row['id_llamado']) if pd.notna(row['id_llamado']) else None
                                if id_llamado is None:
                                    errors += 1
                                    continue
                                
                                with conn.engine.connect() as connection:
                                    result = connection.execute(check_query, {"id_llamado": id_llamado}).fetchone()
                                    
                                    # Preparar datos para la operación
                                    data = {}
                                    for col in mapped_df.columns:
                                        # Convertir nan a None
                                        data[col] = None if pd.isna(row[col]) else row[col]
                                    
                                    # Si existe y modo apropiado
                                    if result and mode in ["Actualizar existentes", "Insertar y actualizar"]:
                                        # Actualizar
                                        cols_to_update = [f"{col} = :{col}" for col in data.keys() if col != 'id_llamado']
                                        
                                        if cols_to_update:
                                            update_query = text(f"""
                                                UPDATE siciap.datosejecucion
                                                SET {', '.join(cols_to_update)}
                                                WHERE id_llamado = :id_llamado
                                            """)
                                            
                                            connection.execute(update_query, data)
                                            connection.commit()
                                            updated += 1
                                    
                                    # Si no existe y modo apropiado
                                    elif not result and mode in ["Insertar nuevos registros", "Insertar y actualizar"]:
                                        # Insertar
                                        cols = list(data.keys())
                                        placeholders = [f":{col}" for col in cols]
                                        
                                        insert_query = text(f"""
                                            INSERT INTO siciap.datosejecucion 
                                            ({', '.join(cols)})
                                            VALUES 
                                            ({', '.join(placeholders)})
                                        """)
                                        
                                        connection.execute(insert_query, data)
                                        connection.commit()
                                        inserted += 1
                            except Exception as e:
                                logger.error(f"Error en fila {_}: {str(e)}")
                                errors += 1
                        
                        # Mostrar resultados
                        st.success(f"✅ Importación completada: {inserted} registros insertados, {updated} actualizados, {errors} con errores.")
                        
                        # Mostrar detalles si hay errores
                        if errors > 0:
                            st.warning(f"⚠️ Hubo {errors} errores durante la importación. Revisa el registro para más detalles.")
                        
                    except Exception as e:
                        st.error(f"Error durante la importación: {str(e)}")
                        st.error(traceback.format_exc())
                    finally:
                        # Cerrar conexión
                        conn.close()
        
        except Exception as e:
            st.error(f"Error al leer el archivo: {str(e)}")
            st.error(traceback.format_exc())
    else:
        st.info("Por favor, carga un archivo para importar datos.")

# Mostrar la página correspondiente según la selección directamente
if selected_option == "📋 Órdenes":
    ordenes_page()
elif selected_option == "📊 Ejecución":
    ejecucion_page()
elif selected_option == "📦 Stock":
    stock_page()
elif selected_option == "📝 Pedidos":
    pedidos_page()  # Nueva opción
elif selected_option == "📈 Dashboard":
    dashboard_page()
elif selected_option == "📑 Contratos":
    contracts_management_page()
if selected_option == "🔍 Diagnóstico":
    diagnostico_dashboard()
elif selected_option == "📤 Exportar Datos":
    exportar_datos_ejecucion()
elif selected_option == "📥 Importar Datos":
    importar_datos_ejecucion()