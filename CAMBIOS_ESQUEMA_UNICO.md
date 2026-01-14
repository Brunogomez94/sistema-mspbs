# Cambios para Esquema Único `oxigeno`

## ✅ Cambios Realizados

1. **`cargar_archivo_con_configuracion()`** - Modificado para:
   - Generar `codigo_licitacion` en lugar de `esquema_formateado`
   - Usar esquema `oxigeno` en lugar de crear esquemas dinámicos
   - Agregar columna `codigo_licitacion` a todos los DataFrames

2. **`crear_tabla_llamado()`** - Modificado para:
   - Aceptar parámetro `codigo_licitacion`
   - Agregar columna `codigo_licitacion` a la tabla
   - Crear índice en `codigo_licitacion`
   - Verificar duplicados por `codigo_licitacion` en lugar de por esquema

## ⚠️ Cambios Pendientes

### 1. **`cargar_archivo_a_postgres()`**
- Línea 1597: Cambiar `esquema_formateado` por `codigo_licitacion`
- Línea 1655: Cambiar creación de esquema dinámico por `oxigeno`
- Líneas 1661, 1667, 1673: Actualizar llamadas a funciones de creación de tablas

### 2. **`crear_tabla_ejecucion_general()`**
- Agregar parámetro `codigo_licitacion`
- Agregar columna `codigo_licitacion` a la tabla
- Crear índice en `codigo_licitacion`
- Filtrar por `codigo_licitacion` al verificar duplicados

### 3. **`crear_tabla_orden_compra()`**
- Agregar parámetro `codigo_licitacion`
- Agregar columna `codigo_licitacion` a la tabla
- Crear índice en `codigo_licitacion`
- Filtrar por `codigo_licitacion` al verificar duplicados

### 4. **`obtener_esquemas_postgres()`** → **`obtener_codigos_licitacion()`**
- Cambiar para obtener códigos únicos desde `oxigeno.llamado`
- Query: `SELECT DISTINCT codigo_licitacion FROM oxigeno.llamado ORDER BY codigo_licitacion`

### 5. **`pagina_dashboard()`**
- Cambiar todas las referencias de `esquema` por `codigo_licitacion`
- Filtrar por `codigo_licitacion` en lugar de usar esquemas dinámicos

### 6. **`pagina_cargar_archivo()`**
- Actualizar búsqueda para usar `codigo_licitacion`
- Actualizar verificación de duplicados

### 7. **`pagina_ordenes_compra()`**
- Filtrar por `codigo_licitacion` en lugar de esquema

### 8. **`eliminar_esquema_postgres()`** → **`eliminar_licitacion()`**
- Cambiar para eliminar por `codigo_licitacion`
- Eliminar registros de todas las tablas con ese `codigo_licitacion`

### 9. **Actualizar `archivos_cargados`**
- Cambiar columna `esquema` por `codigo_licitacion` (o mantener ambas para compatibilidad)

## 📝 Notas

- **Formato de `codigo_licitacion`**: `{MODALIDAD}_{NUMERO}/{AÑO}` (ej: `LPN_100/2023`)
- **Índices**: Agregar índices en `codigo_licitacion` para mejor rendimiento
- **Migración**: Si hay datos existentes en esquemas dinámicos, necesitarás un script de migración
