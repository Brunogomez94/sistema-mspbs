-- Script para verificar que el usuario admin existe
-- Ejecuta este script en Supabase → SQL Editor

-- Verificar usuarios existentes
SELECT id, cedula, username, nombre_completo, role, fecha_creacion 
FROM oxigeno.usuarios;

-- Si no hay usuarios, crear el admin manualmente:
INSERT INTO oxigeno.usuarios (cedula, username, password, nombre_completo, role)
VALUES (
    '123456', 
    'admin', 
    '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918', 
    'Administrador', 
    'admin'
);
