# Sistema Integrado de Gestión - MSPBS

Plataforma unificada para la gestión de licitaciones, contratos y monitoreo del Ministerio de Salud Pública y Bienestar Social.

## 📋 Estructura del Proyecto

```
sistema-mspbs-nuevo/
├── main_app.py              # Aplicación principal
├── apps/
│   ├── licitaciones_app.py  # Seguimiento de Oxigeno
│   ├── siciap_app.py        # Sistema SICIAP
│   └── dashboard_mspbs.py   # Tienda Virtual
├── .streamlit/
│   ├── config.toml          # Configuración de Streamlit
│   └── secrets.toml.example # Ejemplo de secrets
├── requirements.txt         # Dependencias Python
└── README.md               # Este archivo
```

## 🚀 Instalación

1. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configurar base de datos:**
   - Copia `.streamlit/secrets.toml.example` a `.streamlit/secrets.toml`
   - Completa con tus credenciales de PostgreSQL

3. **Ejecutar la aplicación:**
   ```bash
   streamlit run main_app.py
   ```

## 📦 Módulos

### 🏠 Main App (main_app.py)
Aplicación principal que integra todos los módulos y proporciona navegación centralizada.

### 📊 Seguimiento de Oxigeno (licitaciones_app.py)
Sistema de gestión de llamados y órdenes de compra de gases medicinales.

### 📋 Sistema SICIAP (siciap_app.py)
Dashboard para contratos, stock, pedidos y ejecución.

### 📈 Tienda Virtual (dashboard_mspbs.py)
Monitoreo en tiempo real de órdenes de compra desde la Tienda Virtual DNCP.

## ⚙️ Configuración

### Variables de Entorno
Puedes configurar la conexión a la base de datos mediante variables de entorno:

- `DB_HOST`: Host de PostgreSQL (default: localhost)
- `DB_PORT`: Puerto de PostgreSQL (default: 5432)
- `DB_NAME`: Nombre de la base de datos (default: postgres)
- `DB_USER`: Usuario de PostgreSQL (default: postgres)
- `DB_PASSWORD`: Contraseña de PostgreSQL

### Streamlit Secrets
Para producción, configura los secrets en `.streamlit/secrets.toml`:

```toml
[db_config]
host = "tu_host"
port = 5432
dbname = "tu_base_de_datos"
user = "tu_usuario"
password = "tu_contraseña"
```

## 🛠️ Desarrollo

Este proyecto utiliza:
- **Streamlit**: Framework web
- **PostgreSQL**: Base de datos
- **SQLAlchemy**: ORM para Python
- **Plotly**: Visualizaciones interactivas
- **Pandas**: Manipulación de datos

## 📝 Notas

- Asegúrate de tener PostgreSQL instalado y corriendo
- Los módulos se cargan dinámicamente desde la carpeta `apps/`
- Cada módulo puede funcionar de forma independiente o integrado en el main_app

## 🚀 Despliegue

Este proyecto puede desplegarse en múltiples plataformas:

- **Streamlit Cloud** (Recomendado - Gratis): [Ver guía completa](DEPLOY.md#-opción-1-streamlit-cloud-recomendado---gratis)
- **Docker**: [Ver guía](DEPLOY.md#-opción-2-docker)
- **Heroku**: [Ver guía](DEPLOY.md#-opción-3-heroku)
- **Servidor Propio**: [Ver guía](DEPLOY.md#-opción-4-servidor-propio-vpscloud)

Para instrucciones detalladas, consulta [DEPLOY.md](DEPLOY.md)

### Despliegue Rápido en Streamlit Cloud

1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Conecta tu repositorio: `Brunogomez94/sistema-mspbs`
3. Configura los secrets con tus credenciales de PostgreSQL
4. ¡Despliega!

## 🔒 Seguridad

- **NO** subas `secrets.toml` a control de versiones
- Usa variables de entorno en producción
- Mantén las credenciales seguras
- El archivo `.gitignore` ya excluye `secrets.toml`