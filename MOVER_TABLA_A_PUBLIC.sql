-- Script para mover la tabla usuarios al esquema public
-- Esto permite que la API REST de Supabase acceda a la tabla sin configuración adicional
-- Ejecuta este script en Supabase → SQL Editor

-- Mover la tabla al esquema public
ALTER TABLE oxigeno.usuarios SET SCHEMA public;

-- Verificar que se movió correctamente
SELECT table_schema, table_name 
FROM information_schema.tables 
WHERE table_name = 'usuarios';

-- Verificar usuarios
SELECT id, cedula, username, nombre_completo, role 
FROM public.usuarios;
