# Opciones para Levantar Base de Datos PostgreSQL

## 🎯 Opciones Recomendadas

### 1. **Supabase (Actual - Recomendado)**
✅ **Ventajas:**
- Gratis hasta 500 MB
- API REST incluida
- Panel web fácil de usar
- Backups automáticos
- SSL incluido
- Escalable

❌ **Desventajas:**
- Límite de 500 MB en plan gratuito
- Solo expone esquema `public` por defecto (necesitas configurar "Exposed schemas" para otros esquemas)

**Costo:** Gratis (hasta 500 MB) / $25/mes (8 GB)

---

### 2. **PostgreSQL Local (Tu PC)**
✅ **Ventajas:**
- Control total
- Sin límites de espacio
- Gratis
- Sin dependencia de internet (desarrollo)

❌ **Desventajas:**
- Necesitas mantener el servidor
- No accesible desde Streamlit Cloud (solo local)
- Necesitas configurar SSL para producción
- Backups manuales

**Costo:** Gratis

**Cómo instalar:**
```bash
# Windows (usando Chocolatey)
choco install postgresql

# O descargar desde: https://www.postgresql.org/download/windows/
```

---

### 3. **Docker PostgreSQL**
✅ **Ventajas:**
- Fácil de levantar/bajar
- Aislado del sistema
- Mismo entorno en desarrollo y producción
- Fácil de compartir

❌ **Desventajas:**
- Necesitas Docker instalado
- No accesible desde Streamlit Cloud (solo local)

**Costo:** Gratis

**Cómo usar:**
```bash
docker run --name postgres-mspbs \
  -e POSTGRES_PASSWORD=Dggies12345 \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  -d postgres:15
```

---

### 4. **ElephantSQL (PostgreSQL Cloud)**
✅ **Ventajas:**
- Gratis hasta 20 MB
- Fácil de usar
- Panel web
- SSL incluido

❌ **Desventajas:**
- Muy limitado en plan gratuito (20 MB)
- No tiene API REST (solo conexión directa)
- Puede tener problemas de conexión desde Streamlit Cloud

**Costo:** Gratis (20 MB) / $19/mes (20 GB)

---

### 5. **Neon (PostgreSQL Serverless)**
✅ **Ventajas:**
- Gratis hasta 0.5 GB
- Serverless (paga por uso)
- Muy rápido
- Branching de bases de datos

❌ **Desventajas:**
- No tiene API REST (solo conexión directa)
- Puede tener problemas de conexión desde Streamlit Cloud

**Costo:** Gratis (0.5 GB) / $19/mes (10 GB)

---

### 6. **Railway / Render (PostgreSQL + Hosting)**
✅ **Ventajas:**
- Puedes hostear PostgreSQL + Streamlit en el mismo lugar
- Fácil de desplegar
- SSL incluido

❌ **Desventajas:**
- Más caro
- No tiene API REST (solo conexión directa)

**Costo:** $5-20/mes

---

## 🎯 Recomendación para Tu Proyecto

### **Opción A: Continuar con Supabase (Mejor para Streamlit Cloud)**
1. **Mover tablas de `oxigeno` a `public`** (más fácil para API REST)
2. **O configurar "Exposed schemas" en Supabase** para exponer el esquema `oxigeno`

**Ventajas:**
- Ya está configurado
- API REST funciona bien
- Sin problemas de conexión desde Streamlit Cloud

### **Opción B: PostgreSQL Local + ngrok (Para desarrollo)**
1. Instalar PostgreSQL local
2. Usar ngrok para exponer el puerto 5432
3. Conectar desde Streamlit Cloud

**Ventajas:**
- Control total
- Sin límites

**Desventajas:**
- Tu PC debe estar encendida
- Más complejo

### **Opción C: Docker PostgreSQL + Railway/Render**
1. Usar Docker para PostgreSQL
2. Desplegar en Railway o Render
3. Conectar desde Streamlit Cloud

**Ventajas:**
- Control total
- Escalable

**Desventajas:**
- Más caro
- Más complejo

---

## 📋 Qué Falta Hacer (Estado Actual)

### ✅ Ya hecho:
- Login con Supabase API REST
- Lectura de `usuarios` desde `public`
- Lectura de `proveedores` (intentando diferentes formatos)

### ⚠️ Pendiente:
1. **Mover tablas de `oxigeno` a `public`** o configurar "Exposed schemas"
   - `oxigeno.proveedores` → `public.proveedores`
   - `oxigeno.auditoria` → `public.auditoria`
   - `oxigeno.ordenes_compra` → `public.ordenes_compra`
   - `oxigeno.archivos_cargados` → `public.archivos_cargados`

2. **Adaptar operaciones de escritura (INSERT/UPDATE/DELETE)** para usar API REST
   - Actualmente solo SELECT funciona con API REST
   - INSERT/UPDATE/DELETE necesitan conexión directa o usar API REST con `.insert()`, `.update()`, `.delete()`

3. **Adaptar funciones que usan conexión directa:**
   - `pagina_gestionar_proveedores()` - operaciones de escritura
   - `registrar_actividad()` - auditoría
   - `cargar_archivo_a_postgres()` - carga de Excel

---

## 🚀 Próximos Pasos Recomendados

1. **Decidir estrategia:**
   - ¿Continuar con Supabase y mover tablas a `public`?
   - ¿O configurar "Exposed schemas" en Supabase?

2. **Crear script SQL** para mover tablas de `oxigeno` a `public`

3. **Adaptar código** para usar API REST en operaciones de escritura

4. **Probar** todas las funcionalidades

¿Qué opción prefieres? ¿Continuamos con Supabase o quieres probar otra opción?
