-- ============================================================================
-- SCRIPT PARA VERIFICAR QUE TODAS LAS TABLAS ESTÉN CREADAS CORRECTAMENTE
-- Ejecuta este script en Supabase → SQL Editor
-- ============================================================================

-- Verificar tabla de usuarios en public
SELECT 
    'public.usuarios' as tabla,
    COUNT(*) as registros,
    CASE WHEN COUNT(*) > 0 THEN '✅ Existe' ELSE '❌ No existe' END as estado
FROM information_schema.tables 
WHERE table_schema = 'public' AND table_name = 'usuarios';

-- Verificar usuario admin
SELECT 
    'Usuario admin' as verificación,
    id, cedula, username, nombre_completo, role 
FROM public.usuarios 
WHERE username = 'admin';

-- Verificar esquema oxigeno
SELECT 
    'Esquema oxigeno' as verificación,
    CASE WHEN COUNT(*) > 0 THEN '✅ Existe' ELSE '❌ No existe' END as estado
FROM information_schema.schemata 
WHERE schema_name = 'oxigeno';

-- Verificar todas las tablas en oxigeno
SELECT 
    table_name as tabla,
    CASE WHEN table_name IN (
        'archivos_cargados', 
        'ordenes_compra', 
        'items_orden_compra', 
        'proveedores', 
        'auditoria', 
        'usuario_servicio'
    ) THEN '✅ Requerida' ELSE '⚠️ Opcional' END as estado
FROM information_schema.tables
WHERE table_schema = 'oxigeno'
ORDER BY table_name;

-- Verificar secuencia de usuarios
SELECT 
    'Secuencia usuarios_id_seq' as verificación,
    CASE WHEN COUNT(*) > 0 THEN '✅ Existe' ELSE '❌ No existe' END as estado
FROM information_schema.sequences 
WHERE sequence_schema = 'public' AND sequence_name = 'usuarios_id_seq';
