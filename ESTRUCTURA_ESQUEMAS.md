# Estructura de Esquemas del Proyecto

## 📊 Esquemas Utilizados

### 1. **Esquema `oxigeno`** (licitaciones_app.py)
**Módulo:** Sistema de Gestión de Licitaciones

**Tablas:**
- `oxigeno.usuarios` - Usuarios del sistema
- `oxigeno.proveedores` - Proveedores registrados
- `oxigeno.ordenes_compra` - Órdenes de compra
- `oxigeno.items_orden_compra` - Items de órdenes de compra
- `oxigeno.archivos_cargados` - Registro de archivos cargados
- `oxigeno.auditoria` - Registro de actividades del sistema
- `oxigeno.usuario_servicio` - Servicios asignados a usuarios
- `oxigeno.actas_recepcion` - Actas de recepción

**Esquemas dinámicos (por licitación):**
- `lpn_100/2023` - Ejemplo de esquema de licitación
- `lpn_101/2023` - Otro ejemplo
- (Cada licitación tiene su propio esquema)

---

### 2. **Esquema `siciap`** (siciap_app.py)
**Módulo:** Sistema SICIAP

**Tablas:**
- `siciap.ordenes` - Órdenes
- `siciap.ejecucion` - Ejecución
- `siciap.stock_critico` - Stock crítico
- `siciap.pedidos` - Pedidos

---

### 3. **Esquema `public`** (dashboard_mspbs.py)
**Módulo:** Dashboard MSPBS

**Tablas:**
- `public.contrataciones_datos` - Datos de contrataciones

---

## 🔧 Configuración Necesaria en Supabase

### Esquemas que DEBEN estar expuestos en API REST:
1. ✅ `public` - Ya expuesto por defecto
2. ⚠️ `oxigeno` - **NECESITA configurarse** (ver `CONFIGURAR_ESQUEMAS_SUPABASE.md`)
3. ⚠️ `siciap` - **NECESITA configurarse** (ver `CONFIGURAR_ESQUEMAS_SUPABASE.md`)

### Esquemas dinámicos (licitaciones):
- Los esquemas de licitaciones (ej: `lpn_100/2023`) **NO necesitan** estar expuestos
- Se acceden mediante conexión directa cuando se cargan archivos Excel
- No se usan con API REST

---

## 📝 Notas Importantes

1. **API REST vs Conexión Directa:**
   - **API REST:** Usado para operaciones simples (SELECT, INSERT, UPDATE, DELETE básicos)
   - **Conexión Directa:** Usado para operaciones complejas (crear esquemas, cargar Excel masivo)

2. **Nombres de Tablas en API REST:**
   - Cuando un esquema está expuesto, las tablas se acceden **sin el prefijo del esquema**
   - Ejemplo: `oxigeno.proveedores` → `proveedores` (en API REST)
   - El código ya maneja esto automáticamente

3. **Row Level Security (RLS):**
   - Asegúrate de que RLS esté configurado correctamente
   - O desactívalo para desarrollo (no recomendado para producción)

---

## 🚀 Próximos Pasos

1. ✅ Configurar "Exposed schemas" en Supabase (ver `CONFIGURAR_ESQUEMAS_SUPABASE.md`)
2. ✅ Verificar que todas las tablas existan en sus esquemas correctos
3. ✅ Probar acceso desde cada módulo
4. ⚠️ Adaptar operaciones de escritura para usar API REST (pendiente)
