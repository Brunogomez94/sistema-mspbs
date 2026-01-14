# 🔐 Configuración de Secrets para Streamlit Cloud

## Pasos para configurar en Streamlit Cloud

1. Ve a tu app en Streamlit Cloud
2. Click en **Settings** (⚙️)
3. Click en **Secrets** en el menú lateral
4. Pega el siguiente contenido:

```toml
[db_config]
host = "db.otblgsembluynkoalivq.supabase.co"
port = 5432
dbname = "postgres"
user = "postgres.otblgsembluynkoalivq"
password = "Dggies12345*/"

[supabase]
url = "https://otblgsembluynkoalivq.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im90Ymxnc2VtYmx1eW5rb2FsaXZxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjgzOTU4MjcsImV4cCI6MjA4Mzk3MTgyN30.GEzj8P6ohKxBuayCC0n5WHypCiTIOcE9qUrXrJ0tI64"
```

5. Click en **Save**

## ✅ Verificación

Después de guardar:
- El sistema intentará conexión directa primero (psycopg2)
- Si falla, usará automáticamente API REST de Supabase
- Verás en el sidebar qué método funcionó

## 📝 Notas

- **db_config**: Para conexión directa a PostgreSQL
- **supabase**: Para API REST (fallback si la conexión directa falla)
- El sistema detecta automáticamente qué método usar
