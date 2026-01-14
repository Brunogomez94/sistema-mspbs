# 🚀 Inicio Rápido

## Pasos para ejecutar el proyecto

### 1. Instalar dependencias
```bash
cd "c:\Users\User\Desktop\PROYECTOS VARIOS\sistema-mspbs-nuevo"
pip install -r requirements.txt
```

### 2. Configurar base de datos

Copia el archivo de ejemplo y completa tus credenciales:
```bash
copy .streamlit\secrets.toml.example .streamlit\secrets.toml
```

Edita `.streamlit\secrets.toml` y completa:
```toml
[db_config]
host = "localhost"
port = 5432
dbname = "postgres"
user = "postgres"
password = "tu_password"
```

### 3. Ejecutar la aplicación
```bash
streamlit run main_app.py
```

La aplicación se abrirá automáticamente en tu navegador en `http://localhost:8501`

## 📁 Estructura de Archivos

- **main_app.py**: Aplicación principal que integra todos los módulos
- **apps/licitaciones_app.py**: Sistema de Seguimiento de Oxigeno
- **apps/siciap_app.py**: Sistema SICIAP
- **apps/dashboard_mspbs.py**: Dashboard Tienda Virtual

## ⚠️ Notas Importantes

1. Asegúrate de que PostgreSQL esté corriendo
2. Verifica que la base de datos tenga los esquemas necesarios
3. Si hay errores de conexión, revisa `.streamlit/secrets.toml`

## 🔧 Solución de Problemas

### Error de conexión a BD
- Verifica que PostgreSQL esté corriendo
- Revisa las credenciales en `secrets.toml`
- Verifica que el puerto 5432 esté disponible

### Error de módulo no encontrado
- Verifica que todos los archivos en `apps/` existan
- Reinstala dependencias: `pip install -r requirements.txt`

### Error de importación
- Asegúrate de estar en el directorio correcto
- Verifica que todas las dependencias estén instaladas
