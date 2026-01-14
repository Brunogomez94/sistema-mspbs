-- Script para crear la tabla de usuarios en Supabase
-- Ejecuta este script en Supabase → SQL Editor

-- Crear esquema si no existe
CREATE SCHEMA IF NOT EXISTS oxigeno;

-- Crear tabla de usuarios
CREATE TABLE IF NOT EXISTS oxigeno.usuarios (
    id SERIAL PRIMARY KEY,
    cedula VARCHAR(20) UNIQUE NOT NULL,
    username VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(200) NOT NULL,
    nombre_completo VARCHAR(100) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'user',
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ultimo_cambio_password TIMESTAMP
);

-- Crear usuario admin por defecto
-- Contraseña: admin (hash SHA256)
INSERT INTO oxigeno.usuarios (cedula, username, password, nombre_completo, role)
VALUES (
    '123456', 
    'admin', 
    '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918', 
    'Administrador', 
    'admin'
)
ON CONFLICT (username) DO NOTHING;

-- IMPORTANTE: Para que la API REST de Supabase pueda acceder a esta tabla,
-- necesitas configurar el esquema 'oxigeno' en Supabase Dashboard:
-- 1. Ve a Settings → API
-- 2. En "Exposed schemas", agrega 'oxigeno'
-- 3. O mueve la tabla al esquema 'public' si prefieres

-- Alternativa: Mover tabla a public (más fácil para API REST)
-- ALTER TABLE oxigeno.usuarios SET SCHEMA public;
