# Script de Consulta MongoDB Atlas - Créditos en Mora

Este script permite consultar documentos de la colección `loan` en MongoDB Atlas y validar el status de los usuarios asociados.

## Funcionalidades

1. **Consulta de préstamos**: Busca documentos en la colección `loan` que cumplan con los siguientes criterios:
   - `financial_entity_id` en la lista especificada
   - `status` igual a "paid"
   - `amortization` con al menos un elemento que tenga `days_in_arrear` mayor a 0

2. **Actualización de amortization**: 
   - Para cada préstamo encontrado, actualiza todos los elementos de `amortization` que tengan `days_in_arrear` mayor a 0
   - Establece `days_in_arrear` igual a 0 en MongoDB

3. **Validación y actualización de usuarios**: 
   - Para cada préstamo encontrado, busca el usuario correspondiente en la colección `user`
   - Si el usuario tiene status "arrear", busca todos sus préstamos
   - Actualiza el status del usuario a "active" si:
     - Solo tiene un préstamo, O
     - Tiene múltiples préstamos pero ninguno está en arrear

4. **Exportación de datos**: Guarda los resultados en archivos JSON con timestamp.

5. **Notificaciones por correo**: Envía un resumen de la ejecución por correo usando Resend.

## Requisitos

- Python 3.7+
- Conexión a MongoDB Atlas
- Cuenta de Resend para notificaciones por correo
- Dependencias instaladas (ver `requirements.txt`)

## Instalación

1. Clona o descarga este repositorio
2. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```
3. Configura las variables de entorno creando un archivo `.env` con:
   ```
   # Configuración de MongoDB
   MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
   DATABASE_NAME=middleware
   
   # IDs de entidades financieras
   STOP_ID=your_stop_id_here
   YOYO_ID=your_yoyo_id_here
   
   # Configuración de email con Resend
   RESEND_API_KEY=re_your_api_key_here
   EMAIL_FROM=LeanCore Checker <noreply@yourdomain.com>
   EMAIL_TO=admin@yourdomain.com
   ```

## Uso

1. Ejecuta el script:
   ```bash
   python main.py
   ```

2. El script ejecutará automáticamente:
   - Consulta a la colección `loan`
   - Guardado de resultados en archivo JSON
   - Actualización de amortization (days_in_arrear → 0)
   - Validación de usuarios
   - Guardado de resultados de validación
   - Envío de notificación por correo con resumen

## Archivos generados

- `loan_documents_YYYYMMDD_HHMMSS.json`: Documentos de préstamos encontrados
- `amortization_updates_YYYYMMDD_HHMMSS.json`: Registro de actualizaciones de amortization (solo si se realizaron actualizaciones)
- `user_validation_YYYYMMDD_HHMMSS.json`: Resultados de validación de usuarios
- `user_updates_YYYYMMDD_HHMMSS.json`: Registro de actualizaciones de status de usuarios (solo si se realizaron actualizaciones)

## Notificaciones por correo

El script envía automáticamente un resumen de la ejecución por correo que incluye:
- Número de documentos procesados
- Cantidad de actualizaciones realizadas
- Lista de archivos generados
- Fecha y hora de ejecución
- Estado de la ejecución

**Nota**: Si las variables de email no están configuradas, el script continuará ejecutándose normalmente pero no enviará notificaciones por correo.

## Estructura de la consulta

La consulta implementada es equivalente a:

```javascript
db.loan.find({
  financial_entity_id: {$in: ["f0f6b280-b6f8-4b2c-ba3c-0ead0c15590e", "713bf1d6-9e5d-4b48-b211-7bc5ec736130"]},
  status: "paid",
  amortization: {
    $elemMatch: {
      days_in_arrear: { $gt: 0 }
    }
  }
})
```

## Notas importantes

- El script se conecta a la base de datos `middleware`
- Los ObjectId se convierten automáticamente a string para la serialización JSON
- Se incluye manejo de errores y validaciones
- La conexión se cierra automáticamente al finalizar
- **⚠️ IMPORTANTE**: El script actualiza directamente la base de datos MongoDB. Asegúrate de tener una copia de seguridad antes de ejecutarlo en producción. 