# 🚀 Guía de Despliegue - Sistema MSPBS

Esta guía te ayudará a desplegar el Sistema Integrado de Gestión MSPBS en diferentes plataformas.

## 📋 Requisitos Previos

- Repositorio en GitHub: `https://github.com/Brunogomez94/sistema-mspbs`
- Base de datos PostgreSQL accesible (local o remota)
- Credenciales de acceso a la base de datos

---

## 🌐 Opción 1: Streamlit Cloud (Recomendado - Gratis)

Streamlit Cloud es la forma más fácil y rápida de desplegar aplicaciones Streamlit.

### Paso 1: Preparar el Repositorio

1. Asegúrate de que todos los archivos estén en GitHub
2. Verifica que `packages.txt` existe en la raíz del proyecto

### Paso 2: Conectar con Streamlit Cloud

1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Inicia sesión con tu cuenta de GitHub
3. Haz clic en "New app"
4. Selecciona el repositorio: `Brunogomez94/sistema-mspbs`
5. Selecciona la rama: `main`
6. Archivo principal: `main_app.py`

### Paso 3: Configurar Secrets

En Streamlit Cloud, ve a "Settings" → "Secrets" y agrega:

```toml
[db_config]
host = "tu_host_postgresql"
port = 5432
dbname = "tu_base_de_datos"
user = "tu_usuario"
password = "tu_contraseña"
```

### Paso 4: Desplegar

1. Haz clic en "Deploy"
2. Espera a que se complete el despliegue
3. Tu aplicación estará disponible en: `https://sistema-mspbs.streamlit.app`

---

## 🐳 Opción 2: Docker

### Paso 1: Construir la Imagen

```bash
docker build -t sistema-mspbs .
```

### Paso 2: Ejecutar el Contenedor

```bash
docker run -d \
  -p 8501:8501 \
  -e DB_HOST=tu_host \
  -e DB_PORT=5432 \
  -e DB_NAME=tu_base_de_datos \
  -e DB_USER=tu_usuario \
  -e DB_PASSWORD=tu_contraseña \
  --name sistema-mspbs \
  sistema-mspbs
```

O usando un archivo de secrets:

```bash
docker run -d \
  -p 8501:8501 \
  -v $(pwd)/.streamlit/secrets.toml:/app/.streamlit/secrets.toml \
  --name sistema-mspbs \
  sistema-mspbs
```

### Paso 3: Acceder a la Aplicación

Abre tu navegador en: `http://localhost:8501`

---

## ☁️ Opción 3: Heroku

### Paso 1: Instalar Heroku CLI

Descarga e instala desde [heroku.com/cli](https://devcenter.heroku.com/articles/heroku-cli)

### Paso 2: Crear Procfile

Crea un archivo `Procfile` en la raíz:

```
web: streamlit run main_app.py --server.port=$PORT --server.address=0.0.0.0
```

### Paso 3: Desplegar

```bash
# Login a Heroku
heroku login

# Crear aplicación
heroku create sistema-mspbs

# Configurar variables de entorno
heroku config:set DB_HOST=tu_host
heroku config:set DB_PORT=5432
heroku config:set DB_NAME=tu_base_de_datos
heroku config:set DB_USER=tu_usuario
heroku config:set DB_PASSWORD=tu_contraseña

# Desplegar
git push heroku main
```

---

## 🔧 Opción 4: Servidor Propio (VPS/Cloud)

### Paso 1: Instalar Dependencias

```bash
# Instalar Python y PostgreSQL
sudo apt update
sudo apt install python3 python3-pip postgresql-client

# Clonar repositorio
git clone https://github.com/Brunogomez94/sistema-mspbs.git
cd sistema-mspbs

# Instalar dependencias
pip3 install -r requirements.txt
```

### Paso 2: Configurar Secrets

```bash
# Crear directorio .streamlit si no existe
mkdir -p .streamlit

# Copiar y editar secrets
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
nano .streamlit/secrets.toml
```

### Paso 3: Ejecutar con systemd (Opcional)

Crea `/etc/systemd/system/sistema-mspbs.service`:

```ini
[Unit]
Description=Sistema MSPBS Streamlit App
After=network.target

[Service]
Type=simple
User=tu_usuario
WorkingDirectory=/ruta/a/sistema-mspbs
Environment="PATH=/usr/bin:/usr/local/bin"
ExecStart=/usr/local/bin/streamlit run main_app.py --server.port=8501 --server.address=0.0.0.0
Restart=always

[Install]
WantedBy=multi-user.target
```

Activar servicio:

```bash
sudo systemctl enable sistema-mspbs
sudo systemctl start sistema-mspbs
```

---

## 🔐 Configuración de Base de Datos

### Variables de Entorno

Puedes configurar la conexión usando variables de entorno:

```bash
export DB_HOST=tu_host
export DB_PORT=5432
export DB_NAME=tu_base_de_datos
export DB_USER=tu_usuario
export DB_PASSWORD=tu_contraseña
```

### Streamlit Secrets

Para Streamlit Cloud o local, usa `.streamlit/secrets.toml`:

```toml
[db_config]
host = "tu_host"
port = 5432
dbname = "tu_base_de_datos"
user = "tu_usuario"
password = "tu_contraseña"
```

---

## ✅ Verificación Post-Despliegue

1. **Verificar conexión a BD**: La aplicación mostrará un indicador verde si la conexión es exitosa
2. **Probar módulos**: Navega por los tres módulos desde el menú lateral
3. **Revisar logs**: En Streamlit Cloud, revisa los logs en "Manage app"

---

## 🐛 Solución de Problemas

### Error de conexión a BD
- Verifica que las credenciales en secrets sean correctas
- Asegúrate de que la BD sea accesible desde el servidor de despliegue
- Revisa los firewalls y reglas de seguridad

### Error de módulo no encontrado
- Verifica que todos los archivos en `apps/` estén en el repositorio
- Revisa que `packages.txt` tenga todas las dependencias

### Error de importación
- Verifica que todas las dependencias estén en `requirements.txt`
- Reinstala dependencias: `pip install -r requirements.txt`

---

## 📞 Soporte

Para más ayuda, consulta:
- [Documentación de Streamlit](https://docs.streamlit.io)
- [Streamlit Cloud Docs](https://docs.streamlit.io/streamlit-community-cloud)
- [Issues en GitHub](https://github.com/Brunogomez94/sistema-mspbs/issues)
