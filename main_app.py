import streamlit as st
import sys
import os

# Configuración de la página principal
st.set_page_config(
    page_title="Sistema Integrado de Gestión - MSPBS",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #0e4f3c;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .app-description {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 3rem;
    }
    .status-indicator {
        position: fixed;
        top: 10px;
        right: 10px;
        background-color: #28a745;
        color: white;
        padding: 5px 10px;
        border-radius: 5px;
        font-size: 12px;
        z-index: 1000;
    }
</style>
""", unsafe_allow_html=True)

# Configuración de conexión a PostgreSQL
def get_db_config():
    """Obtiene configuración de BD desde secrets o variables de entorno"""
    try:
        if hasattr(st, 'secrets') and 'db_config' in st.secrets:
            user = st.secrets['db_config']['user']
            # Si el usuario no tiene el formato postgres.INSTANCE_ID, intentar agregarlo
            # El instance_id es la parte antes de .supabase.co en el host
            if '.' not in user and 'host' in st.secrets['db_config']:
                host = st.secrets['db_config']['host']
                # Extraer instance_id del host (ej: db.otblgsembluynkoalivq.supabase.co -> otblgsembluynkoalivq)
                if '.supabase.co' in host:
                    instance_id = host.split('.')[1] if host.startswith('db.') else host.split('.')[0]
                    user = f"postgres.{instance_id}"
            
            return {
                'host': st.secrets['db_config']['host'],
                'port': int(st.secrets['db_config']['port']),
                'dbname': st.secrets['db_config']['dbname'],
                'user': user,
                'password': st.secrets['db_config']['password']
            }
    except Exception:
        pass
    
    # Fallback a variables de entorno o valores por defecto
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'dbname': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'Dggies12345')
    }

@st.cache_resource
def get_db_engine(_host, _port, _dbname, _user, _password):
    """Crear conexión centralizada a PostgreSQL
    
    Los parámetros con _ son para invalidar el cache cuando cambien los secrets
    """
    try:
        from sqlalchemy import create_engine
        
        # Debug: verificar que estamos usando el host correcto
        if _host == 'localhost':
            st.warning(f"⚠️ ADVERTENCIA: Se está usando 'localhost' en lugar del host de Supabase. Verifica los secrets.")
        
        # Supabase requiere SSL y el usuario debe ser postgres.INSTANCE_ID
        # Usar pooler en modo session para evitar problemas de transaction pooling
        # Si el host tiene .pooler., usar session mode, si no usar direct connection
        if '.pooler.' in _host or 'pooler' in _host:
            # Connection pooling - usar session mode
            conn_str = f"postgresql://{_user}:{_password}@{_host}:{_port}/{_dbname}?sslmode=require&pgbouncer=true"
        else:
            # Direct connection
            conn_str = f"postgresql://{_user}:{_password}@{_host}:{_port}/{_dbname}?sslmode=require"
        
        # Forzar IPv4 resolviendo el hostname primero
        import socket
        try:
            # Resolver a IPv4 explícitamente
            ipv4 = socket.gethostbyname(_host)
            # Reemplazar hostname con IP en la cadena de conexión
            conn_str = conn_str.replace(_host, ipv4)
        except:
            pass  # Si falla la resolución, usar el hostname original
        
        engine = create_engine(
            conn_str, 
            connect_args={
                "client_encoding": "utf8",
                "connect_timeout": 10,
                "sslmode": "require"
            },
            pool_pre_ping=True
        )
        return engine
    except Exception as e:
        st.error(f"Error creando engine: {e}")
        return None

def verificar_conexion_db():
    """Verificar estado de la conexión a la base de datos"""
    try:
        config = get_db_config()
        # Pasar los valores como parámetros para que el cache se invalide si cambian
        engine = get_db_engine(
            _host=config['host'],
            _port=config['port'],
            _dbname=config['dbname'],
            _user=config['user'],
            _password=config['password']
        )
        if engine:
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, None
        return False, "No se pudo crear el engine"
    except Exception as e:
        return False, str(e)

def main():
    # Verificar conexión a la base de datos
    db_connected, error_msg = verificar_conexion_db()
    
    # Obtener configuración para mostrar info de debug
    config = get_db_config()
    
    # Indicador de estado
    if db_connected:
        st.markdown('<div class="status-indicator">🟢 BD Conectada</div>', unsafe_allow_html=True)
        st.sidebar.success("✅ PostgreSQL Conectado")
    else:
        st.markdown('<div class="status-indicator">🔴 BD Desconectada</div>', unsafe_allow_html=True)
        st.sidebar.warning("⚠️ Base de datos no disponible (configura en Secrets)")
        
        # Mostrar información de debug en modo expandible
        with st.sidebar.expander("🔍 Información de Debug", expanded=True):
            st.write("**Configuración actual:**")
            st.write(f"Host: `{config['host']}`")
            st.write(f"Puerto: `{config['port']}`")
            st.write(f"Base de datos: `{config['dbname']}`")
            st.write(f"Usuario: `{config['user']}`")
            st.write(f"Password: `{'*' * len(config['password'])}`")
            
            # Verificar si está usando secrets
            st.write("---")
            try:
                if hasattr(st, 'secrets'):
                    if 'db_config' in st.secrets:
                        st.success("✅ Secrets detectados en st.secrets")
                        try:
                            secrets_config = st.secrets['db_config']
                            st.write("**Valores en secrets:**")
                            st.write(f"Host: `{secrets_config.get('host', 'NO ENCONTRADO')}`")
                            st.write(f"Port: `{secrets_config.get('port', 'NO ENCONTRADO')}`")
                            st.write(f"DB Name: `{secrets_config.get('dbname', 'NO ENCONTRADO')}`")
                            st.write(f"User: `{secrets_config.get('user', 'NO ENCONTRADO')}`")
                        except Exception as e:
                            st.error(f"Error leyendo secrets: {e}")
                    else:
                        st.error("❌ No se encontró 'db_config' en st.secrets")
                        st.write("**Secrets disponibles:**")
                        try:
                            st.json(list(st.secrets.keys()))
                        except:
                            st.write("No se pudieron listar los secrets")
                else:
                    st.error("❌ st.secrets no está disponible")
            except Exception as e:
                st.error(f"Error verificando secrets: {e}")
            
            # Mostrar error de conexión si existe
            if error_msg:
                st.write("---")
                st.error(f"**Error de conexión:**")
                st.code(error_msg)
    
    # Título principal
    st.markdown('<h1 class="main-header">🏥 Sistema Integrado de Gestión - MSPBS</h1>', unsafe_allow_html=True)
    st.markdown('<p class="app-description">Plataforma Unificada Para Gestión de Licitaciones, Contratos y Monitoreo</p>', unsafe_allow_html=True)
    
    # Configurar el menú lateral
    with st.sidebar:
        st.image("https://via.placeholder.com/200x100/0e4f3c/ffffff?text=MSPBS", width=200)
        st.markdown("---")
        
        # Lista de aplicaciones disponibles
        apps = {
            "🏠 Inicio": "home",
            "📊 Seguimiento de Oxigeno": "licitaciones",
            "📋 Sistema SICIAP": "siciap", 
            "📈 Tienda Virtual": "dashboard_mspbs",
            "⚙️ Configuración": "config"
        }

        selected_app = st.sidebar.selectbox(
            "Navegación Principal",
            list(apps.keys()),
            index=0
        )
        
        # Información del sistema
        st.markdown("---")
        st.markdown("### 📊 Estado del Sistema")
        from datetime import datetime
        current_time = datetime.now().strftime("%H:%M:%S")
        st.info(f"🕐 Hora actual: {current_time}")
        
        if db_connected:
            st.success("🟢 Todos los sistemas operativos")
        else:
            st.warning("⚠️ Verificar configuración BD")
    
    # Contenido principal basado en la selección
    app_key = apps[selected_app]
    
    try:
        if app_key == "home":
            show_home_page()
        elif app_key == "licitaciones":
            run_licitaciones_app()
        elif app_key == "siciap":
            run_siciap_app()
        elif app_key == "dashboard_mspbs":
            run_dashboard_mspbs()
        elif app_key == "config":
            show_config_page()
    except Exception as e:
        st.error(f"Error cargando módulo '{app_key}': {e}")
        import traceback
        with st.expander("Ver detalles del error"):
            st.code(traceback.format_exc())

def show_home_page():
    """Página de inicio con resumen del sistema"""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        ## 👋 ¡Bienvenido al Sistema Integrado MSPBS!
        
        Esta plataforma unifica tres sistemas críticos del Ministerio de Salud Pública y Bienestar Social.
        
        ### 🎯 Sistemas Disponibles:
        
        - **📊 Seguimiento de Oxigeno**: Gestión Completa de Llamados y Ordenes de compra
        - **📋 Sistema SICIAP**: Dashboard para contratos, stock, pedidos y ejecución
        - **📈 Tienda Virtual**: Monitoreo de Ordenes de Compra - Tienda Virtual DNCP - MSPBS
        
        ### 🚀 Características Principales:
        - ✅ Autenticación y control de roles
        - ✅ Conexión centralizada a PostgreSQL
        - ✅ Dashboards interactivos con Plotly
        - ✅ Actualización automática en tiempo real
        - ✅ Exportación de datos en múltiples formatos
        - ✅ Gestión de proveedores y contratos
        """)
        
        # Métricas del sistema
        st.markdown("### 📊 Estadísticas del Sistema")
        
        try:
            engine = get_db_engine()
            if engine:
                from sqlalchemy import text
                with engine.connect() as conn:
                    # Contar esquemas de licitaciones
                    try:
                        esquemas_query = text("""
                            SELECT count(*) as total_esquemas
                            FROM information_schema.schemata 
                            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
                        """)
                        esquemas_result = conn.execute(esquemas_query).fetchone()
                        total_esquemas = esquemas_result[0] if esquemas_result else 0
                    except:
                        total_esquemas = 0
                    
                    # Contar datos de contrataciones
                    try:
                        contrataciones_query = text("SELECT count(*) FROM contrataciones_datos WHERE entidad = 'Ministerio de Salud Pública y Bienestar Social'")
                        contrataciones_result = conn.execute(contrataciones_query).fetchone()
                        total_contrataciones = contrataciones_result[0] if contrataciones_result else 0
                    except:
                        total_contrataciones = 0
                    
                    # Contar órdenes SICIAP
                    try:
                        siciap_query = text("SELECT count(*) FROM siciap.ordenes")
                        siciap_result = conn.execute(siciap_query).fetchone()
                        total_siciap = siciap_result[0] if siciap_result else 0
                    except:
                        total_siciap = 0
                
                # Mostrar métricas
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    st.metric("Esquemas de Licitaciones", total_esquemas, "📊")
                with col_b:
                    st.metric("Contrataciones", f"{total_contrataciones:,}", "🏥")
                with col_c:
                    st.metric("Órdenes SICIAP", f"{total_siciap:,}", "📋")
                with col_d:
                    st.metric("Sistemas Activos", "3", "🚀")
            else:
                # Métricas por defecto si no hay engine
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    st.metric("Sistemas", "3", "100%")
                with col_b:
                    st.metric("Estado", "Activo", "🟢")
                with col_c:
                    st.metric("BD", "No conectada", "⚠️")
                with col_d:
                    st.metric("Usuarios", "Multi-rol", "👥")
        except Exception as e:
            st.warning(f"Error obteniendo estadísticas: {e}")
            # Métricas por defecto
            col_a, col_b, col_c, col_d = st.columns(4)
            with col_a:
                st.metric("Sistemas", "3", "100%")
            with col_b:
                st.metric("Estado", "Activo", "🟢")
            with col_c:
                st.metric("BD", "Error", "⚠️")
            with col_d:
                st.metric("Usuarios", "Multi-rol", "👥")

def show_config_page():
    """Página de configuración del sistema"""
    st.title("⚙️ Configuración del Sistema")
    st.markdown("---")
    
    # Configuración de base de datos
    st.subheader("🗃️ Configuración de Base de Datos")
    
    config = get_db_config()
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Host", value=config['host'], disabled=True)
        st.text_input("Puerto", value=str(config['port']), disabled=True)
    with col2:
        st.text_input("Base de Datos", value=config['dbname'], disabled=True)
        st.text_input("Usuario", value=config['user'], disabled=True)
    
    # Test de conexión
    if st.button("🔍 Probar Conexión"):
        if verificar_conexion_db():
            st.success("✅ Conexión exitosa a PostgreSQL")
        else:
            st.error("❌ Error de conexión")
    
    st.markdown("---")
    
    # Información del sistema
    st.subheader("📊 Información del Sistema")
    
    import pandas as pd
    info_data = {
        "Componente": [
            "Seguimiento de Oxigeno", 
            "Sistema SICIAP", 
            "Tienda Virtual",
            "Base de Datos",
            "Streamlit",
            "Plotly"
        ],
        "Estado": ["🟢 Activo", "🟢 Activo", "🟢 Activo", "🟢 Conectado", "🟢 Funcionando", "🟢 Funcionando"],
        "Descripción": [
            "Plataforma Unificada Para Gestión de Licitaciones, Contratos y Monitoreo",
            "Dashboard de contratos y ejecución", 
            "Monitoreo tiempo real de Ordenes de Compra - Tienda Virtual",
            "PostgreSQL con múltiples esquemas",
            "Framework web para aplicaciones",
            "Visualizaciones interactivas"
        ]
    }
    
    df_info = pd.DataFrame(info_data)
    st.dataframe(df_info, use_container_width=True)
    
    st.markdown("---")
    
    # Configuración de módulos
    st.subheader("📦 Estado de Módulos")
    
    modulos = [
        {"Módulo": "Licitaciones", "Archivo": "apps/licitaciones_app.py", "Estado": "✅" if os.path.exists("apps/licitaciones_app.py") else "❌"},
        {"Módulo": "SICIAP", "Archivo": "apps/siciap_app.py", "Estado": "✅" if os.path.exists("apps/siciap_app.py") else "❌"},
        {"Módulo": "Tienda Virtual", "Archivo": "apps/dashboard_mspbs.py", "Estado": "✅" if os.path.exists("apps/dashboard_mspbs.py") else "❌"},
    ]
    
    df_modulos = pd.DataFrame(modulos)
    st.dataframe(df_modulos, use_container_width=True)

# ================================================================================
# FUNCIONES WRAPPER PARA INTEGRACIÓN CON MAIN_APP
# ================================================================================

def run_licitaciones_app():
    """Función wrapper para ejecutar el Seguimiento de Oxigeno desde main_app.py"""
    try:
        if os.path.exists("apps/licitaciones_app.py"):
            apps_path = os.path.join(os.getcwd(), "apps")
            if apps_path not in sys.path:
                sys.path.insert(0, apps_path)
            
            import importlib.util
            spec = importlib.util.spec_from_file_location("licitaciones_app", "apps/licitaciones_app.py")
            if spec is None:
                raise ImportError("No se pudo cargar licitaciones_app.py")
            
            licitaciones_module = importlib.util.module_from_spec(spec)
            
            try:
                spec.loader.exec_module(licitaciones_module)
            except SyntaxError as e:
                st.error(f"❌ Error de sintaxis en licitaciones_app.py: {e}")
                st.info("💡 El módulo tiene errores de sintaxis que deben corregirse.")
                import traceback
                with st.expander("Ver detalles del error"):
                    st.code(traceback.format_exc())
                return
            except Exception as init_error:
                st.error(f"❌ Error al inicializar módulo: {init_error}")
                st.info("💡 Verifica que todas las dependencias estén instaladas.")
                return
            
            if hasattr(licitaciones_module, 'main'):
                try:
                    licitaciones_module.main()
                except Exception as main_error:
                    st.error(f"❌ Error ejecutando módulo: {main_error}")
                    import traceback
                    with st.expander("Ver detalles del error"):
                        st.code(traceback.format_exc())
            else:
                st.error("❌ El módulo licitaciones_app no tiene función main()")
        else:
            st.error("❌ Módulo de licitaciones no encontrado")
            st.info("💡 Asegúrate de que el archivo apps/licitaciones_app.py existe")
    except Exception as e:
        st.error(f"❌ Error ejecutando Seguimiento de Oxigeno: {e}")
        import traceback
        with st.expander("Ver detalles del error"):
            st.code(traceback.format_exc())

def run_siciap_app():
    """Función wrapper para ejecutar el sistema SICIAP desde main_app.py"""
    try:
        if os.path.exists("apps/siciap_app.py"):
            apps_path = os.path.join(os.getcwd(), "apps")
            if apps_path not in sys.path:
                sys.path.insert(0, apps_path)
            
            import importlib.util
            spec = importlib.util.spec_from_file_location("siciap_app", "apps/siciap_app.py")
            if spec is None:
                raise ImportError("No se pudo cargar siciap_app.py")
            
            siciap_module = importlib.util.module_from_spec(spec)
            
            try:
                spec.loader.exec_module(siciap_module)
            except SyntaxError as e:
                st.error(f"❌ Error de sintaxis en siciap_app.py: {e}")
                st.info("💡 El módulo tiene errores de sintaxis que deben corregirse.")
                import traceback
                with st.expander("Ver detalles del error"):
                    st.code(traceback.format_exc())
                return
            except Exception as init_error:
                st.error(f"❌ Error al inicializar módulo: {init_error}")
                st.info("💡 Verifica que todas las dependencias estén instaladas (psycopg2-binary).")
                import traceback
                with st.expander("Ver detalles del error"):
                    st.code(traceback.format_exc())
                return
            
            # SICIAP ejecuta directamente el código del módulo
            st.info("✅ Módulo SICIAP cargado correctamente")
        else:
            st.error("❌ Módulo SICIAP no encontrado")
            st.info("💡 Asegúrate de que el archivo apps/siciap_app.py existe")
    except Exception as e:
        st.error(f"❌ Error ejecutando sistema SICIAP: {e}")
        import traceback
        with st.expander("Ver detalles del error"):
            st.code(traceback.format_exc())

def run_dashboard_mspbs():
    """Función wrapper para ejecutar el Tienda Virtual desde main_app.py"""
    try:
        if os.path.exists("apps/dashboard_mspbs.py"):
            apps_path = os.path.join(os.getcwd(), "apps")
            if apps_path not in sys.path:
                sys.path.insert(0, apps_path)
            
            import importlib.util
            spec = importlib.util.spec_from_file_location("dashboard_mspbs", "apps/dashboard_mspbs.py")
            if spec is None:
                raise ImportError("No se pudo cargar dashboard_mspbs.py")
            
            dashboard_module = importlib.util.module_from_spec(spec)
            
            try:
                spec.loader.exec_module(dashboard_module)
            except SyntaxError as e:
                st.error(f"❌ Error de sintaxis en dashboard_mspbs.py: {e}")
                st.info("💡 El módulo tiene errores de sintaxis que deben corregirse.")
                import traceback
                with st.expander("Ver detalles del error"):
                    st.code(traceback.format_exc())
                return
            except Exception as init_error:
                st.error(f"❌ Error al inicializar módulo: {init_error}")
                st.info("💡 Verifica que todas las dependencias estén instaladas.")
                import traceback
                with st.expander("Ver detalles del error"):
                    st.code(traceback.format_exc())
                return
            
            # Si el módulo tiene función main, ejecutarla
            if hasattr(dashboard_module, 'main'):
                try:
                    dashboard_module.main()
                except Exception as main_error:
                    st.error(f"❌ Error ejecutando módulo: {main_error}")
                    import traceback
                    with st.expander("Ver detalles del error"):
                        st.code(traceback.format_exc())
            else:
                st.info("✅ Módulo Tienda Virtual cargado (sin función main)")
        else:
            st.error("❌ Módulo Tienda Virtual no encontrado")
            st.info("💡 Asegúrate de que el archivo apps/dashboard_mspbs.py existe")
    except Exception as e:
        st.error(f"❌ Error ejecutando Tienda Virtual: {e}")
        import traceback
        with st.expander("Ver detalles del error"):
            st.code(traceback.format_exc())

if __name__ == "__main__":
    main()
