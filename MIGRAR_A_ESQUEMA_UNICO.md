# Migración a Esquema Único `oxigeno`

## 🎯 Objetivo

Cambiar de esquemas dinámicos (`lpn_100/2023`, `lpn_101/2023`, etc.) a un **esquema único `oxigeno`** con una columna `codigo_licitacion` para identificar cada licitación.

## 📋 Cambios Necesarios

### 1. **Modificar Funciones de Carga**
- `cargar_archivo_con_configuracion()` - Usar `oxigeno` en lugar de esquema dinámico
- `crear_tabla_llamado()` - Agregar columna `codigo_licitacion`
- `crear_tabla_ejecucion_general()` - Agregar columna `codigo_licitacion`
- `crear_tabla_orden_compra()` - Agregar columna `codigo_licitacion`

### 2. **Agregar Columna `codigo_licitacion`**
Todas las tablas necesitan esta columna:
- `oxigeno.llamado` → `codigo_licitacion VARCHAR(100)`
- `oxigeno.ejecucion_general` → `codigo_licitacion VARCHAR(100)`
- `oxigeno.orden_de_compra` → `codigo_licitacion VARCHAR(100)`

### 3. **Modificar Funciones de Visualización**
- `obtener_esquemas_postgres()` → `obtener_codigos_licitacion()` - Obtener códigos únicos desde `oxigeno.llamado`
- `pagina_dashboard()` - Filtrar por `codigo_licitacion` en lugar de esquema
- `pagina_cargar_archivo()` - Usar `codigo_licitacion` en lugar de crear esquema
- `pagina_ordenes_compra()` - Filtrar por `codigo_licitacion`
- Todas las funciones que usan `esquema` → cambiar a filtrar por `codigo_licitacion`

### 4. **Actualizar Eliminación**
- `eliminar_esquema_postgres()` → `eliminar_licitacion()` - Eliminar por `codigo_licitacion` en lugar de esquema

## 🔧 Formato de `codigo_licitacion`

Formato sugerido: `{MODALIDAD}_{NUMERO}/{AÑO}`

Ejemplos:
- `LPN_100/2023`
- `CD_50/2024`
- `LP_25/2023`

## ✅ Ventajas

1. **Un solo esquema** - Más fácil de gestionar
2. **Mejor rendimiento** - No hay que crear/esquemas dinámicos
3. **Compatible con API REST** - Solo necesitas exponer `oxigeno`
4. **Fácil de consultar** - Un solo `SELECT` con `WHERE codigo_licitacion = ...`
5. **Sin límites** - No hay restricción de número de esquemas

## ⚠️ Consideraciones

- **Migración de datos existentes**: Si ya tienes datos en esquemas dinámicos, necesitarás un script de migración
- **Índices**: Agregar índice en `codigo_licitacion` para mejor rendimiento
- **Compatibilidad**: Asegurar que todas las funciones usen el nuevo formato
