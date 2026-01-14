# Configurar Esquemas Expuestos en Supabase

## 📋 Situación Actual

Tu proyecto usa **múltiples esquemas**:
- **`oxigeno`** - Usado por `licitaciones_app.py`
- **`siciap`** - Usado por `siciap_app.py`
- **`public`** - Usado por `dashboard_mspbs.py` (tabla `contrataciones_datos`)

Por defecto, Supabase API REST **solo expone el esquema `public`**. Para que la API REST pueda acceder a `oxigeno` y `siciap`, necesitas configurar "Exposed schemas".

---

## 🚀 Pasos para Configurar Esquemas Expuestos

### Paso 1: Ir a Configuración de API en Supabase

1. Ve a tu proyecto en Supabase: https://supabase.com/dashboard/project/otblgsembluynkoalivq
2. En el menú lateral, ve a **Settings** → **API**
3. Busca la sección **"Exposed schemas"** o **"API Settings"**

### Paso 2: Agregar Esquemas

En la sección **"Exposed schemas"**, agrega:
- `oxigeno`
- `siciap`
- `public` (ya está por defecto, pero asegúrate de que esté)

**Formato:**
```
oxigeno, siciap, public
```

O si es una lista separada:
- `oxigeno`
- `siciap`
- `public`

### Paso 3: Guardar Cambios

1. Haz clic en **"Save"** o **"Update"**
2. Espera unos segundos para que los cambios se apliquen

### Paso 4: Verificar

Puedes verificar que funciona intentando acceder a una tabla desde la API REST:

```bash
# Ejemplo: Verificar que puedes acceder a oxigeno.proveedores
curl -H "apikey: TU_ANON_KEY" \
     -H "Authorization: Bearer TU_ANON_KEY" \
     "https://otblgsembluynkoalivq.supabase.co/rest/v1/proveedores?select=*&limit=1"
```

---

## 📝 Nota sobre Nombres de Tablas en API REST

Cuando configuras "Exposed schemas", Supabase API REST accede a las tablas de esta forma:

### Antes (solo `public`):
- `public.usuarios` → `usuarios` ✅
- `oxigeno.proveedores` → ❌ No accesible

### Después (con esquemas expuestos):
- `public.usuarios` → `usuarios` ✅
- `oxigeno.proveedores` → `proveedores` ✅ (si `oxigeno` está expuesto)
- `siciap.ordenes` → `ordenes` ✅ (si `siciap` está expuesto)

**⚠️ IMPORTANTE:** Si tienes tablas con el mismo nombre en diferentes esquemas (ej: `public.usuarios` y `oxigeno.usuarios`), la API REST puede tener conflictos. En ese caso, necesitarás usar nombres únicos o acceder de forma diferente.

---

## 🔧 Si No Encuentras "Exposed Schemas"

Si no ves la opción "Exposed schemas" en la interfaz:

1. **Verifica que estés en el plan correcto** - Esta función está disponible en todos los planes
2. **Busca en "Database" → "Settings"** - A veces está en otra sección
3. **Usa SQL directamente** - Puedes configurarlo con SQL:

```sql
-- Verificar esquemas actuales
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast');

-- Los esquemas se exponen automáticamente si están en la lista de "Exposed schemas"
-- en la configuración de la API. No hay comando SQL directo para esto.
```

---

## ✅ Verificación Final

Después de configurar, prueba en tu aplicación:

1. **Login** - Debe funcionar (ya funciona según tu mensaje)
2. **Gestión de Proveedores** - Debe poder leer `oxigeno.proveedores`
3. **Dashboard** - Debe poder leer `public.contrataciones_datos`
4. **SICIAP** - Debe poder leer `siciap.ordenes`, `siciap.ejecucion`, etc.

---

## 🆘 Si Aún No Funciona

Si después de configurar los esquemas expuestos aún no puedes acceder:

1. **Verifica que las tablas existan** en los esquemas correctos
2. **Verifica Row Level Security (RLS)** - Puede estar bloqueando el acceso
3. **Revisa los logs** en Supabase → Logs → API
4. **Prueba con conexión directa** como fallback (ya configurado en tu código)

---

## 📚 Referencias

- [Supabase API Documentation](https://supabase.com/docs/guides/api)
- [Exposed Schemas](https://supabase.com/docs/guides/api/rest/access-control#exposed-schemas)
