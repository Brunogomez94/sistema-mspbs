-- ============================================================================
-- SCRIPT PARA CREAR TODAS LAS TABLAS NECESARIAS EN SUPABASE
-- Ejecuta este script completo en Supabase → SQL Editor
-- ============================================================================

-- ============================================================================
-- 1. TABLA DE USUARIOS (en public para API REST)
-- ============================================================================
CREATE TABLE IF NOT EXISTS public.usuarios (
    id INTEGER NOT NULL,
    cedula VARCHAR(20) NOT NULL,
    username VARCHAR(50) NOT NULL,
    password VARCHAR(200) NOT NULL,
    nombre_completo VARCHAR(100) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'user',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ultimo_cambio_password TIMESTAMP,
    CONSTRAINT usuarios_pkey PRIMARY KEY (id),
    CONSTRAINT usuarios_cedula_unique UNIQUE (cedula),
    CONSTRAINT usuarios_username_unique UNIQUE (username)
);

-- Crear secuencia para id
CREATE SEQUENCE IF NOT EXISTS usuarios_id_seq;
ALTER TABLE public.usuarios ALTER COLUMN id SET DEFAULT nextval('usuarios_id_seq');
ALTER SEQUENCE usuarios_id_seq OWNED BY public.usuarios.id;

-- Insertar usuario admin por defecto
INSERT INTO public.usuarios (cedula, username, password, nombre_completo, role)
SELECT '123456', 'admin', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918', 'Administrador', 'admin'
WHERE NOT EXISTS (SELECT 1 FROM public.usuarios WHERE username = 'admin');

-- ============================================================================
-- 2. CREAR ESQUEMA OXIGENO
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS oxigeno;

-- ============================================================================
-- 3. TABLA DE ARCHIVOS CARGADOS
-- ============================================================================
CREATE TABLE IF NOT EXISTS oxigeno.archivos_cargados (
    id SERIAL PRIMARY KEY,
    nombre_archivo VARCHAR(255) NOT NULL,
    esquema VARCHAR(100) NOT NULL,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_id INTEGER NOT NULL,
    ubicacion_fisica VARCHAR(500),
    estado VARCHAR(50) DEFAULT 'Activo',
    FOREIGN KEY (usuario_id) REFERENCES public.usuarios(id)
);

-- Índices para archivos_cargados
CREATE INDEX IF NOT EXISTS idx_archivos_cargados_usuario ON oxigeno.archivos_cargados(usuario_id);
CREATE INDEX IF NOT EXISTS idx_archivos_cargados_esquema ON oxigeno.archivos_cargados(esquema);

-- ============================================================================
-- 4. TABLA DE ÓRDENES DE COMPRA
-- ============================================================================
CREATE TABLE IF NOT EXISTS oxigeno.ordenes_compra (
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

-- ============================================================================
-- 5. TABLA DE ITEMS DE ÓRDENES DE COMPRA
-- ============================================================================
CREATE TABLE IF NOT EXISTS oxigeno.items_orden_compra (
    id SERIAL PRIMARY KEY,
    orden_compra_id INTEGER NOT NULL,
    lote VARCHAR(50),
    item VARCHAR(50),
    codigo_insumo VARCHAR(100),
    codigo_servicio VARCHAR(100),
    descripcion TEXT,
    cantidad NUMERIC(15, 2) NOT NULL,
    unidad_medida VARCHAR(50),
    precio_unitario NUMERIC(15, 2),
    monto_total NUMERIC(15, 2),
    observaciones TEXT,
    FOREIGN KEY (orden_compra_id) REFERENCES oxigeno.ordenes_compra(id) ON DELETE CASCADE
);

-- Índices para items_orden_compra
CREATE INDEX IF NOT EXISTS idx_items_orden_compra_orden ON oxigeno.items_orden_compra(orden_compra_id);

-- ============================================================================
-- 6. TABLA DE PROVEEDORES
-- ============================================================================
CREATE TABLE IF NOT EXISTS oxigeno.proveedores (
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

-- ============================================================================
-- 7. TABLA DE AUDITORÍA
-- ============================================================================
CREATE TABLE IF NOT EXISTS oxigeno.auditoria (
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

-- Índices para auditoría
CREATE INDEX IF NOT EXISTS idx_auditoria_usuario ON oxigeno.auditoria(usuario_id);
CREATE INDEX IF NOT EXISTS idx_auditoria_fecha ON oxigeno.auditoria(fecha_hora DESC);
CREATE INDEX IF NOT EXISTS idx_auditoria_modulo ON oxigeno.auditoria(modulo);
CREATE INDEX IF NOT EXISTS idx_auditoria_accion ON oxigeno.auditoria(accion);

-- ============================================================================
-- 8. TABLA DE USUARIO-SERVICIO (para asignar servicios a usuarios)
-- ============================================================================
CREATE TABLE IF NOT EXISTS oxigeno.usuario_servicio (
    id SERIAL PRIMARY KEY,
    usuario_id INTEGER NOT NULL,
    servicio VARCHAR(200) NOT NULL,
    puede_crear_oc BOOLEAN DEFAULT TRUE,
    puede_crear_acta BOOLEAN DEFAULT TRUE,
    fecha_asignacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (usuario_id) REFERENCES public.usuarios(id) ON DELETE CASCADE,
    UNIQUE(usuario_id, servicio)
);

-- ============================================================================
-- VERIFICACIÓN
-- ============================================================================
-- Verificar que las tablas se crearon correctamente
SELECT 
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema IN ('public', 'oxigeno')
    AND table_name IN ('usuarios', 'archivos_cargados', 'ordenes_compra', 'items_orden_compra', 
                        'proveedores', 'auditoria', 'usuario_servicio')
ORDER BY table_schema, table_name;

-- Verificar usuario admin
SELECT id, cedula, username, nombre_completo, role 
FROM public.usuarios 
WHERE username = 'admin';
