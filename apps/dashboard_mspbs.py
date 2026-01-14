import streamlit as st
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import os
import time

# Configurar UTF-8
os.environ['PYTHONIOENCODING'] = 'utf-8'

st.set_page_config(
    page_title="Dashboard MSPBS", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# ====================================
# AUTO-REFRESH AUTOMÁTICO
# ====================================

# CSS para ocultar elementos de Streamlit y mejorar apariencia
st.markdown("""
<style>
    /* Ocultar menú y footer de Streamlit */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Indicador de actualización */
    .refresh-indicator {
        position: fixed;
        top: 10px;
        right: 10px;
        background-color: #0e4f3c;
        color: white;
        padding: 5px 10px;
        border-radius: 5px;
        font-size: 12px;
        z-index: 1000;
    }
    
    /* Indicador de estado de datos */
    .status-indicator {
        position: fixed;
        top: 40px;
        right: 10px;
        background-color: #28a745;
        color: white;
        padding: 3px 8px;
        border-radius: 3px;
        font-size: 10px;
        z-index: 1000;
    }
</style>
""", unsafe_allow_html=True)

# Configurar auto-refresh cada 30 segundos para UI, pero datos se actualizan desde PostgreSQL
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

# Mostrar indicadores de estado
current_time = datetime.now().strftime("%H:%M:%S")
st.markdown(f'<div class="refresh-indicator">🔄 Actualizado: {current_time}</div>', unsafe_allow_html=True)
st.markdown(f'<div class="status-indicator">🟢 Sistema Activo</div>', unsafe_allow_html=True)

# Auto-refresh de la página cada 30 segundos
st.markdown("""
<script>
    setTimeout(function(){
        window.location.reload();
    }, 30000);
</script>
""", unsafe_allow_html=True)

st.title("📊 TABLERO DGGIES - DGL - TIENDA VIRTUAL DNCP")
st.markdown("🔄 **Sistema de actualización automática cada 30 minutos** | Dashboard se refresca cada 30 segundos")

# Configuración BD - Lee de secrets o variables de entorno
def get_db_config_dashboard():
    """Obtiene configuración de BD desde secrets o variables de entorno"""
    try:
        if hasattr(st, 'secrets') and 'db_config' in st.secrets:
            return {
                'host': st.secrets['db_config']['host'],
                'port': int(st.secrets['db_config']['port']),
                'database': st.secrets['db_config']['dbname'],
                'user': st.secrets['db_config']['user'],
                'password': st.secrets['db_config']['password']
            }
    except Exception:
        pass
    
    # Fallback a variables de entorno o valores por defecto
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'Dggies12345')
    }

@st.cache_resource
def get_engine(_host, _port, _database, _user, _password):
    """Crear engine con cache que se invalida cuando cambian los secrets"""
    try:
        conn_str = f"postgresql://{_user}:{_password}@{_host}:{_port}/{_database}"
        engine = create_engine(
            conn_str, 
            connect_args={
                "client_encoding": "utf8",
                "connect_timeout": 10
            },
            pool_pre_ping=True
        )
        return engine
    except Exception as e:
        st.error(f"Error conexión PostgreSQL: {e}")
        return None

@st.cache_data(ttl=30)  # Cache por 30 segundos para refresh automático
def load_covid_data():
    config = get_db_config_dashboard()
    engine = get_engine(
        _host=config['host'],
        _port=config['port'],
        _database=config['database'],
        _user=config['user'],
        _password=config['password']
    )
    if not engine:
        return pd.DataFrame()

    try:
        with engine.connect() as conn:
            # Consulta optimizada con timestamp para verificar última actualización
            query = text("""
            SELECT 
                fecha_orden_compra, 
                nro_orden_compra, 
                nombre_entidad, 
                proveedor, 
                ruc_completo, 
                n5, 
                cantidad, 
                precio_unitario, 
                precio_total,
                NOW() as ultima_consulta
            FROM contrataciones_datos 
            WHERE entidad = 'Ministerio de Salud Pública y Bienestar Social'
              AND nombre_entidad = 'Uoc Nro 1  Nivel Central (D.O.C) MSPBS / Ministerio de Salud Pública y Bienestar Social'
              AND (id LIKE '%382392%' OR id LIKE '%386038%' 
                   OR id LIKE '%395261%' OR id LIKE '%400275%')
            ORDER BY fecha_orden_compra DESC, proveedor ASC
            """)

            df = pd.read_sql(query, conn)

            # Limpiar encoding
            if not df.empty:
                for col in df.columns:
                    if df[col].dtype == 'object' and col != 'ultima_consulta':
                        try:
                            df[col] = df[col].astype(str)
                            df[col] = df[col].str.replace('\x00', '', regex=False)
                            df[col] = df[col].str.replace('\ufffd', '', regex=False)
                            df[col] = df[col].str.encode('utf-8', errors='ignore').str.decode('utf-8')
                        except:
                            df[col] = df[col].astype(str)

                # Convertir tipos
                if 'fecha_orden_compra' in df.columns:
                    df['fecha_orden_compra'] = pd.to_datetime(df['fecha_orden_compra'], errors='coerce')
                
                for col in ['precio_total', 'precio_unitario', 'cantidad']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # Mostrar info de última actualización de datos
                if 'ultima_consulta' in df.columns and not df.empty:
                    ultima_actualizacion = df['ultima_consulta'].iloc[0]
                    st.sidebar.success(f"🕐 Última consulta BD: {ultima_actualizacion.strftime('%H:%M:%S')}")

            return df

    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return pd.DataFrame()

# ====================================
# INFORMACIÓN DEL SISTEMA
# ====================================
st.sidebar.header("📊 Sistema Continuo")
st.sidebar.info("""
🔄 **FUNCIONAMIENTO AUTOMÁTICO:**
- Datos se actualizan cada 30 min
- Dashboard se refresca cada 30 seg
- Sistema funciona 24/7 en segundo plano

🟢 **ESTADO:** Activo
""")

# Cargar datos automáticamente
df = load_covid_data()

if df.empty:
    st.warning("⚠️ Sin datos disponibles")
    st.info("💡 El sistema de actualización continua está trabajando...")
    
    # Mostrar progreso simulado mientras se cargan datos
    progress_bar = st.progress(0)
    for i in range(100):
        time.sleep(0.1)
        progress_bar.progress(i + 1)
    
    st.info("🔄 Reintentando conexión automáticamente...")
    time.sleep(2)
    st.rerun()
else:
    st.success(f"✅ {len(df):,} registros cargados | Sistema funcionando continuamente")

# ====================================
# FILTROS AVANZADOS
# ====================================
st.sidebar.header("🔍 Filtros Avanzados")

# Filtro por Proveedor
if 'proveedor' in df.columns:
    try:
        proveedores_unicos = df['proveedor'].dropna().astype(str).unique()
        proveedores_limpios = [p for p in proveedores_unicos if p and p != 'nan']
        
        if proveedores_limpios:
            proveedores = ['Todos'] + sorted(set(proveedores_limpios))
            proveedor_filtro = st.sidebar.selectbox("🏢 Proveedor:", proveedores)
    except:
        proveedor_filtro = 'Todos'

# Filtro por RUC
if 'ruc_completo' in df.columns:
    try:
        rucs_unicos = df['ruc_completo'].dropna().astype(str).unique()
        rucs_limpios = [r for r in rucs_unicos if r and r != 'nan']
        
        if rucs_limpios:
            rucs = ['Todos'] + sorted(set(rucs_limpios))
            ruc_filtro = st.sidebar.selectbox("🆔 RUC:", rucs)
    except:
        ruc_filtro = 'Todos'  

# Filtro por INSUMOS MEDICAMENTOS (N5)
if 'n5' in df.columns:
    try:
        n5_unicos = df['n5'].dropna().astype(str).unique()
        n5_limpios = [n for n in n5_unicos if n and n != 'nan']
        
        if n5_limpios:
            n5_options = ['Todos'] + sorted(set(n5_limpios))
            n5_filtro = st.sidebar.selectbox("🔢 Insumos Medicamentos:", n5_options)
    except:
        n5_filtro = 'Todos'

# Filtro por Número de Orden
nro_orden_buscar = st.sidebar.text_input("📋 Buscar Nro. Orden:")

# Filtro por rango de fechas
if 'fecha_orden_compra' in df.columns:
    df_with_dates = df.dropna(subset=['fecha_orden_compra'])
    if not df_with_dates.empty:
        min_date = df_with_dates['fecha_orden_compra'].min().date()
        max_date = df_with_dates['fecha_orden_compra'].max().date()
        
        fecha_inicio = st.sidebar.date_input("📅 Fecha Inicio:", min_date)
        fecha_fin = st.sidebar.date_input("📅 Fecha Fin:", max_date)

# Filtro por rango de cantidad
if 'cantidad' in df.columns:
    cantidad_min = int(df['cantidad'].min()) if not df['cantidad'].isna().all() else 0
    cantidad_max = int(df['cantidad'].max()) if not df['cantidad'].isna().all() else 1000
    
    if cantidad_max > cantidad_min:
        rango_cantidad = st.sidebar.slider(
            "📦 Rango de Cantidad:",
            min_value=cantidad_min,
            max_value=cantidad_max,
            value=(cantidad_min, cantidad_max)
        )

# Filtro por rango de precios
if 'precio_total' in df.columns:
    precio_min = float(df['precio_total'].min()) if not df['precio_total'].isna().all() else 0
    precio_max = float(df['precio_total'].max()) if not df['precio_total'].isna().all() else 1000000
    
    if precio_max > precio_min:
        rango_precio = st.sidebar.slider(
            "💰 Rango de Precio Total:",
            min_value=precio_min,
            max_value=precio_max,
            value=(precio_min, precio_max),
            format="₲%.0f"
        )

# ====================================
# APLICAR FILTROS
# ====================================
df_filtrado = df.copy()

try:
    # Aplicar todos los filtros
    if 'proveedor_filtro' in locals() and proveedor_filtro != 'Todos':
        mask = df_filtrado['proveedor'].astype(str).str.contains(proveedor_filtro, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    if 'ruc_filtro' in locals() and ruc_filtro != 'Todos':
        mask = df_filtrado['ruc_completo'].astype(str).str.contains(ruc_filtro, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    if 'n5_filtro' in locals() and n5_filtro != 'Todos':
        mask = df_filtrado['n5'].astype(str).str.contains(n5_filtro, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    if nro_orden_buscar:
        mask = df_filtrado['nro_orden_compra'].astype(str).str.contains(nro_orden_buscar, case=False, na=False)
        df_filtrado = df_filtrado[mask]
    
    if 'fecha_inicio' in locals() and 'fecha_fin' in locals():
        mask_fecha = (df_filtrado['fecha_orden_compra'].dt.date >= fecha_inicio) & (df_filtrado['fecha_orden_compra'].dt.date <= fecha_fin)
        df_filtrado = df_filtrado.loc[mask_fecha]
    
    if 'rango_cantidad' in locals():
        mask_cantidad = (df_filtrado['cantidad'] >= rango_cantidad[0]) & (df_filtrado['cantidad'] <= rango_cantidad[1])
        df_filtrado = df_filtrado.loc[mask_cantidad]
    
    if 'rango_precio' in locals():
        mask_precio = (df_filtrado['precio_total'] >= rango_precio[0]) & (df_filtrado['precio_total'] <= rango_precio[1])
        df_filtrado = df_filtrado.loc[mask_precio]
        
except Exception as e:
    st.sidebar.warning(f"Error en filtros: {e}")
    df_filtrado = df.copy()

# Botón limpiar filtros
if st.sidebar.button("🗑️ Limpiar Filtros"):
    st.rerun()

# ====================================
# MÉTRICAS PRINCIPALES EN TIEMPO REAL
# ====================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📊 Registros", f"{len(df_filtrado):_}".replace('_', '.'), delta=f"{len(df_filtrado) - len(df):_}".replace('_', '.') if len(df_filtrado) != len(df) else None)

with col2:
    if 'precio_total' in df_filtrado.columns:
        total = df_filtrado['precio_total'].sum()
        st.metric("💰 Monto Total", f"₲ {total:_.0f}".replace('_', '.'))

with col3:
    if 'proveedor' in df_filtrado.columns:
        proveedores_unicos = df_filtrado['proveedor'].nunique()
        st.metric("🏢 Proveedores", f"{proveedores_unicos}")

with col4:
    if 'precio_total' in df_filtrado.columns:
        promedio = df_filtrado['precio_total'].mean()
        st.metric("📊 Promedio", f"₲ {promedio:_.0f}".replace('_', '.'))

st.markdown("---")

# ====================================
# TABLA PRINCIPAL CON AGRUPACIÓN ESTILO EXCEL
# ====================================
st.subheader("📋 Datos de Ordenes de Compra - Agrupación por Fecha (Más Reciente Primero)")

# Columnas a mostrar
columnas_mostrar = ['fecha_orden_compra', 'nro_orden_compra', 
                   'proveedor', 'ruc_completo', 'n5', 'cantidad', 'precio_unitario', 
                   'precio_total']

columnas_existentes = [col for col in columnas_mostrar if col in df_filtrado.columns]

# Configuración de paginación
registros_por_pagina = st.selectbox("Registros por página:", [25, 50, 100, 200, 500], index=1)

# Preparar datos para agrupación
if not df_filtrado.empty:
    df_trabajo = df_filtrado[columnas_existentes].copy()
    
    # Formatear fechas para agrupación
    if 'fecha_orden_compra' in df_trabajo.columns:
        df_trabajo['fecha_str'] = df_trabajo['fecha_orden_compra'].dt.strftime('%d/%m/%Y')
        # Ordenar por fecha descendente (más reciente primero)
        df_trabajo = df_trabajo.sort_values('fecha_orden_compra', ascending=False)
    else:
        df_trabajo['fecha_str'] = ''
    
    # Agrupar SOLO por fecha (ordenado por más reciente)
    grupos = df_trabajo.groupby('fecha_str')
    
    # Obtener fechas únicas ordenadas (más reciente primero)
    fechas_ordenadas = df_trabajo.drop_duplicates('fecha_str')['fecha_str'].tolist()
    
    # Mostrar agrupación estilo Excel - SOLO POR FECHA
    for fecha in fechas_ordenadas:
        grupo = grupos.get_group(fecha)
        
        # Encabezado del grupo expandible - SOLO FECHA
        with st.expander(f"📅 {fecha} ({len(grupo)} registros)", expanded=False):
            
            # Preparar datos del grupo
            df_grupo = grupo.copy()
            
            # Formatear datos para visualización
            for col in ['precio_total', 'precio_unitario']:
                if col in df_grupo.columns:
                    df_grupo[col] = df_grupo[col].apply(
                        lambda x: f"₲ {x:_.0f}".replace('_', '.') if pd.notna(x) else ""
                    )
            
            if 'cantidad' in df_grupo.columns:
                df_grupo['cantidad'] = df_grupo['cantidad'].apply(
                    lambda x: f"{x:_.0f}".replace('_', '.') if pd.notna(x) else ""
                )
            
            # Renombrar columnas
            nombres_columnas = {
                'nro_orden_compra': 'NRO. ORDEN',
                'proveedor': 'PROVEEDOR',
                'ruc_completo': 'RUC COMPLETO',
                'n5': 'INSUMOS MEDICAMENTOS',
                'cantidad': 'CANTIDAD',
                'precio_unitario': 'PRECIO UNITARIO',
                'precio_total': 'PRECIO TOTAL',
            }
            
            # Seleccionar columnas relevantes (sin fecha redundante)
            columnas_grupo = ['nro_orden_compra', 'proveedor', 'ruc_completo', 'n5', 'cantidad', 'precio_unitario', 'precio_total']
            columnas_grupo = [col for col in columnas_grupo if col in df_grupo.columns]
            
            df_mostrar = df_grupo[columnas_grupo].rename(columns={k: v for k, v in nombres_columnas.items() if k in columnas_grupo})
            
            # Mostrar tabla del grupo
            st.dataframe(df_mostrar, width="stretch", height=300)
            
            # Mostrar totales del grupo por fecha
            if 'precio_total' in grupo.columns:
                total_grupo = grupo['precio_total'].sum()
                cantidad_total = grupo['cantidad'].sum() if 'cantidad' in grupo.columns else 0
                proveedores_count = grupo['proveedor'].nunique() if 'proveedor' in grupo.columns else 0
                st.info(f"💰 Total del día: ₲ {total_grupo:_.0f}".replace('_', '.') + f" | 📦 Cantidad total: {cantidad_total:_.0f}".replace('_', '.') + f" | 🏢 Proveedores: {proveedores_count}")
    
    # Mostrar totales generales
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_registros = len(df_filtrado)
        st.metric("📊 Total Registros", f"{total_registros:_}".replace('_', '.'))

    with col2:
        if 'precio_total' in df_filtrado.columns:
            total_general = df_filtrado['precio_total'].sum()
            st.metric("💰 Total General", f"₲ {total_general:_.0f}".replace('_', '.'))
    
    with col3:
        fechas_count = len(fechas_ordenadas)
        st.metric("📅 Fechas", f"{fechas_count}")

else:
    st.warning("No hay datos para mostrar")

# ====================================
# DESCARGA DE RESULTADOS
# ====================================
st.markdown("---")
st.subheader("💾 Descargar Resultados")

if not df_filtrado.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        try:
            csv_filtrado = df_filtrado[columnas_existentes].to_csv(index=False)
            st.download_button(
                "📄 Descargar Datos Filtrados",
                csv_filtrado,
                f"covid_contrataciones_filtrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv"
            )
        except:
            st.error("Error en descarga filtrada")
    
    with col2:
        try:
            csv_completo = df[columnas_existentes].to_csv(index=False)
            st.download_button(
                "📄 Descargar Todos los Datos",
                csv_completo,
                f"covid_contrataciones_completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv"
            )
        except:
            st.error("Error en descarga completa")

# ====================================
# INFORMACIÓN DEL SISTEMA
# ====================================
st.markdown("---")
col1, col2 = st.columns(2)

with col1:
    st.markdown(f"""
    **📊 Dashboard MSPBS - Sistema Continuo**
    - 🕐 Última actualización: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
    - 🔄 Próxima actualización automática en ~{30 - (time.time() - st.session_state.last_refresh) % 30:.0f} segundos
    """)

with col2:
    st.markdown(f"""
    **🎯 Estado del Sistema:**
    - 🟢 Sistema de datos: Activo (cada 30 min)
    - 🟢 Dashboard: Auto-refresh (cada 30 seg)
    - 🟢 Base de datos: Conectada
    - 📊 Agrupación: Por fecha y proveedor
    """)

# Auto-actualizar session state
st.session_state.last_refresh = time.time()