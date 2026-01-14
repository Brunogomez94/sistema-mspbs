-- ============================================================================
-- SCRIPT PARA MOVER TABLAS DE 'oxigeno' A 'public' EN SUPABASE
-- Esto permite que Supabase API REST acceda a las tablas sin configuración adicional
-- Ejecuta este script completo en Supabase → SQL Editor
-- ============================================================================

-- IMPORTANTE: Este script mueve las tablas, NO las copia
-- Si ya tienes datos, se moverán junto con las tablas

-- ============================================================================
-- 1. MOVER TABLA DE PROVEEDORES
-- ============================================================================
-- Primero, crear la tabla en public si no existe
CREATE TABLE IF NOT EXISTS public.proveedores (
    id SERIAL PRIMARY KEY,
    ruc VARCHAR(50) UNIQUE NOT NULL,
    razon_social VARCHAR(200) NOT NULL,
    direccion TEXT,
    correo_electronico VARCHAR(100),
    telefono VARCHAR(50),
    contacto_nombre VARCHAR(100),
    observaciones TEXT,
    activo BOOLEAN DEFAULT TRUE,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Copiar datos de oxigeno.proveedores a public.proveedores (si existen)
INSERT INTO public.proveedores (id, ruc, razon_social, direccion, correo_electronico, telefono, contacto_nombre, observaciones, activo, fecha_registro, fecha_actualizacion)
SELECT id, ruc, razon_social, direccion, correo_electronico, telefono, contacto_nombre, observaciones, activo, fecha_registro, fecha_actualizacion
FROM oxigeno.proveedores
ON CONFLICT (id) DO NOTHING;

-- Eliminar tabla antigua (solo si la copia fue exitosa)
-- DROP TABLE IF EXISTS oxigeno.proveedores CASCADE;

-- ============================================================================
-- 2. MOVER TABLA DE AUDITORÍA
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.auditoria (
    id SERIAL PRIMARY KEY,
    usuario_id INTEGER NOT NULL,
    usuario_nombre VARCHAR(100) NOT NULL,
    accion VARCHAR(100) NOT NULL,
    modulo VARCHAR(50) NOT NULL,
    descripcion TEXT NOT NULL,
    detalles JSONB,
    ip_address VARCHAR(45),
    user_agent TEXT,
    fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    esquema_afectado VARCHAR(100),
    registro_afectado_id INTEGER,
    valores_anteriores JSONB,
    valores_nuevos JSONB,
    FOREIGN KEY (usuario_id) REFERENCES public.usuarios(id)
);

-- Copiar datos
INSERT INTO public.auditoria (id, usuario_id, usuario_nombre, accion, modulo, descripcion, detalles, ip_address, user_agent, fecha_hora, esquema_afectado, registro_afectado_id, valores_anteriores, valores_nuevos)
SELECT id, usuario_id, usuario_nombre, accion, modulo, descripcion, detalles, ip_address, user_agent, fecha_hora, esquema_afectado, registro_afectado_id, valores_anteriores, valores_nuevos
FROM oxigeno.auditoria
ON CONFLICT (id) DO NOTHING;

-- DROP TABLE IF EXISTS oxigeno.auditoria CASCADE;

-- ============================================================================
-- 3. MOVER TABLA DE ÓRDENES DE COMPRA
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.ordenes_compra (
    id SERIAL PRIMARY KEY,
    numero_orden VARCHAR(50) UNIQUE NOT NULL,
    fecha_emision TIMESTAMP NOT NULL,
    esquema VARCHAR(100) NOT NULL,
    servicio_beneficiario VARCHAR(200),
    simese VARCHAR(50),
    usuario_id INTEGER NOT NULL,
    estado VARCHAR(50) NOT NULL DEFAULT 'Emitida',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (usuario_id) REFERENCES public.usuarios(id)
);

-- Copiar datos
INSERT INTO public.ordenes_compra (id, numero_orden, fecha_emision, esquema, servicio_beneficiario, simese, usuario_id, estado, fecha_creacion)
SELECT id, numero_orden, fecha_emision, esquema, servicio_beneficiario, simese, usuario_id, estado, fecha_creacion
FROM oxigeno.ordenes_compra
ON CONFLICT (id) DO NOTHING;

-- DROP TABLE IF EXISTS oxigeno.ordenes_compra CASCADE;

-- ============================================================================
-- 4. MOVER TABLA DE ITEMS DE ÓRDENES DE COMPRA
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.items_orden_compra (
    id SERIAL PRIMARY KEY,
    orden_compra_id INTEGER NOT NULL,
    item_descripcion TEXT NOT NULL,
    cantidad DECIMAL(10,2) NOT NULL,
    unidad_medida VARCHAR(50),
    precio_unitario DECIMAL(10,2),
    subtotal DECIMAL(10,2),
    FOREIGN KEY (orden_compra_id) REFERENCES public.ordenes_compra(id) ON DELETE CASCADE
);

-- Copiar datos
INSERT INTO public.items_orden_compra (id, orden_compra_id, item_descripcion, cantidad, unidad_medida, precio_unitario, subtotal)
SELECT id, orden_compra_id, item_descripcion, cantidad, unidad_medida, precio_unitario, subtotal
FROM oxigeno.items_orden_compra
ON CONFLICT (id) DO NOTHING;

-- DROP TABLE IF EXISTS oxigeno.items_orden_compra CASCADE;

-- ============================================================================
-- 5. MOVER TABLA DE ARCHIVOS CARGADOS
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.archivos_cargados (
    id SERIAL PRIMARY KEY,
    nombre_archivo VARCHAR(255) NOT NULL,
    esquema VARCHAR(100) NOT NULL,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_id INTEGER NOT NULL,
    ubicacion_fisica VARCHAR(500),
    estado VARCHAR(50) DEFAULT 'Activo',
    FOREIGN KEY (usuario_id) REFERENCES public.usuarios(id)
);

-- Copiar datos
INSERT INTO public.archivos_cargados (id, nombre_archivo, esquema, fecha_carga, usuario_id, ubicacion_fisica, estado)
SELECT id, nombre_archivo, esquema, fecha_carga, usuario_id, ubicacion_fisica, estado
FROM oxigeno.archivos_cargados
ON CONFLICT (id) DO NOTHING;

-- DROP TABLE IF EXISTS oxigeno.archivos_cargados CASCADE;

-- ============================================================================
-- 6. MOVER TABLA DE USUARIO SERVICIO
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.usuario_servicio (
    id SERIAL PRIMARY KEY,
    usuario_id INTEGER NOT NULL,
    servicio_nombre VARCHAR(200) NOT NULL,
    fecha_asignacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    activo BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (usuario_id) REFERENCES public.usuarios(id)
);

-- Copiar datos
INSERT INTO public.usuario_servicio (id, usuario_id, servicio_nombre, fecha_asignacion, activo)
SELECT id, usuario_id, servicio_nombre, fecha_asignacion, activo
FROM oxigeno.usuario_servicio
ON CONFLICT (id) DO NOTHING;

-- DROP TABLE IF EXISTS oxigeno.usuario_servicio CASCADE;

-- ============================================================================
-- VERIFICACIÓN
-- ============================================================================
-- Verificar que las tablas estén en public
SELECT 
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema = 'public' 
  AND table_name IN ('proveedores', 'auditoria', 'ordenes_compra', 'items_orden_compra', 'archivos_cargados', 'usuario_servicio')
ORDER BY table_name;

-- Verificar que no queden tablas en oxigeno (opcional)
SELECT 
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema = 'oxigeno'
ORDER BY table_name;
