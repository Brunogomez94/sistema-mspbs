# Configurar Secrets en Streamlit Cloud

## Configuración necesaria para Supabase API REST

Ve a **Streamlit Cloud → Settings → Secrets** y configura:

```toml
[supabase]
url = "https://otblgsembluynkoalivq.supabase.co"
key = "sb_publishable_TOrJ0hD6j3KNfEYQyMGrLg_NPboit6o"
```

**O si prefieres usar la anon key (legacy):**

```toml
[supabase]
url = "https://otblgsembluynkoalivq.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im90Ymxnc2VtYmx1eW5rb2FsaXZxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjgzOTU4MjcsImV4cCI6MjA4Mzk3MTgyN30.GEzj8P6ohKxBuayCC0n5WHypCiTIOcE9qUrXrJ0tI64"
```

## Nota importante

- **Publishable key** es la recomendada (más segura)
- **anon key** es legacy pero también funciona
- El sistema ahora funciona **SOLO con Supabase API REST**
- No necesitas configurar `db_config` a menos que quieras cargar archivos Excel (requiere conexión directa)
