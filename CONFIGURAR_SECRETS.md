# 🔐 Guía Paso a Paso: Configurar Secrets en Streamlit Cloud

Esta guía te ayudará a configurar la conexión a PostgreSQL en Streamlit Cloud.

## 📋 Requisitos Previos

Antes de comenzar, necesitas tener:
- ✅ Tu aplicación desplegada en Streamlit Cloud
- ✅ Credenciales de acceso a tu base de datos PostgreSQL:
  - Host (dirección del servidor)
  - Puerto (generalmente 5432)
  - Nombre de la base de datos
  - Usuario
  - Contraseña

---

## 🚀 Pasos para Configurar Secrets

### Paso 1: Acceder a Streamlit Cloud

1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Inicia sesión con tu cuenta de GitHub
3. Encuentra tu aplicación en la lista: `sistema-mspbs` o `sistema-compl-siciap`

### Paso 2: Abrir la Configuración de Secrets

1. Haz clic en el nombre de tu aplicación
2. En la parte superior, verás un menú con pestañas
3. Haz clic en **"⚙️ Settings"** (Configuración)
4. En el menú lateral izquierdo, busca y haz clic en **"Secrets"**

### Paso 3: Agregar los Secrets

En el editor de secrets que aparece, pega el siguiente formato y completa con tus datos:

```toml
[db_config]
host = "tu_host_aqui"
port = 5432
dbname = "tu_base_de_datos"
user = "tu_usuario"
password = "tu_contraseña"
```

### Paso 4: Completar con tus Datos Reales

Reemplaza cada valor con tus credenciales reales:

#### Ejemplo 1: Base de Datos Local/Remota
```toml
[db_config]
host = "db.example.com"
port = 5432
dbname = "postgres"
user = "miusuario"
password = "mi_contraseña_segura"
```

#### Ejemplo 2: Base de Datos en la Nube (AWS RDS, Azure, etc.)
```toml
[db_config]
host = "mydb.xxxxx.us-east-1.rds.amazonaws.com"
port = 5432
dbname = "mspbs_db"
user = "admin"
password = "MiPassword123!"
```

#### Ejemplo 3: Base de Datos Local con IP Pública
```toml
[db_config]
host = "123.456.789.0"
port = 5432
dbname = "postgres"
user = "postgres"
password = "Dggies12345"
```

### Paso 5: Guardar los Secrets

1. Haz clic en el botón **"Save"** (Guardar) en la parte inferior
2. Verás un mensaje de confirmación: "Secrets saved successfully"

### Paso 6: Reiniciar la Aplicación

1. Ve a la pestaña **"⚡ Manage app"** (Gestionar app)
2. Haz clic en el botón **"Reboot app"** (Reiniciar app)
3. Espera unos segundos mientras se reinicia

### Paso 7: Verificar la Conexión

1. Abre tu aplicación en el navegador
2. En la barra lateral izquierda, deberías ver:
   - ✅ **"PostgreSQL Conectado"** (en verde)
   - 🟢 **"BD Conectada"** en el indicador superior derecho

Si ves estos mensajes, ¡la configuración fue exitosa! 🎉

---

## ⚠️ Solución de Problemas

### Problema: Sigue apareciendo "Base de datos no disponible"

**Solución 1: Verificar formato del secret**
- Asegúrate de que el formato sea exactamente como se muestra arriba
- No uses comillas dobles dentro de comillas dobles
- Verifica que no haya espacios extra antes de `[db_config]`

**Solución 2: Verificar credenciales**
- Confirma que el host sea accesible desde internet
- Verifica que el usuario y contraseña sean correctos
- Asegúrate de que el puerto 5432 esté abierto

**Solución 3: Verificar firewall**
- Si tu BD está en un servidor propio, asegúrate de que:
  - El puerto 5432 esté abierto en el firewall
  - Streamlit Cloud pueda acceder a tu IP (puede requerir whitelist)

**Solución 4: Reiniciar aplicación**
- Ve a "Manage app" → "Reboot app"
- Espera 30-60 segundos

### Problema: Error de conexión timeout

**Causa:** El host no es accesible desde internet o el firewall está bloqueando

**Solución:**
- Si es una BD local, necesitas exponerla a internet (usar túnel, VPN, o IP pública)
- Considera usar un servicio de BD en la nube (AWS RDS, Azure Database, etc.)

### Problema: Error de autenticación

**Causa:** Usuario o contraseña incorrectos

**Solución:**
- Verifica las credenciales en tu servidor PostgreSQL
- Asegúrate de que el usuario tenga permisos de conexión

---

## 🔒 Seguridad

### ✅ Buenas Prácticas:

1. **Nunca compartas tus secrets públicamente**
2. **Usa contraseñas fuertes** para tu base de datos
3. **No subas `secrets.toml` a GitHub** (ya está en `.gitignore`)
4. **Rota las contraseñas periódicamente**

### ⚠️ Importante:

- Los secrets en Streamlit Cloud están encriptados
- Solo tú y las personas con acceso a tu cuenta pueden verlos
- Los secrets se aplican automáticamente a todas las instancias de tu app

---

## 📝 Ejemplo Completo

Aquí tienes un ejemplo completo de cómo debería verse tu configuración de secrets:

```toml
[db_config]
host = "postgres.example.com"
port = 5432
dbname = "mspbs_production"
user = "mspbs_user"
password = "P@ssw0rd_S3gur0_2024"
```

**Nota:** Este es solo un ejemplo. Usa tus credenciales reales.

---

## 🆘 ¿Necesitas Más Ayuda?

Si después de seguir estos pasos aún tienes problemas:

1. Revisa los logs de la aplicación en Streamlit Cloud
2. Verifica que tu base de datos PostgreSQL esté corriendo
3. Prueba conectarte a la BD desde otra herramienta (pgAdmin, DBeaver) para confirmar las credenciales

---

## ✅ Checklist Final

Antes de considerar que está todo configurado, verifica:

- [ ] Secrets guardados correctamente en Streamlit Cloud
- [ ] Aplicación reiniciada después de guardar secrets
- [ ] Indicador verde "BD Conectada" visible en la app
- [ ] Mensaje "PostgreSQL Conectado" en la barra lateral
- [ ] Los módulos cargan sin errores de conexión

¡Listo! Tu aplicación debería estar funcionando correctamente. 🚀
