import os
import json
import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import resend

load_dotenv()

# Configuración de la URI de MongoDB (puedes cambiar esto por una variable de entorno o input)
MONGODB_URI = os.getenv('MONGODB_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')
COLLECTION_NAME = 'loan'

STOP_ID = os.getenv('STOP_ID')
YOYO_ID = os.getenv('YOYO_ID')

# Configuración de email
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")

if not STOP_ID or not YOYO_ID: 
    raise Exception("Configura los IDs en las variables de entorno")

if not RESEND_API_KEY or not EMAIL_FROM or not EMAIL_TO:
    print("⚠️  Variables de email no configuradas. Las notificaciones por correo estarán deshabilitadas.")

# IDs de entidades financieras
FINANCIAL_ENTITY_IDS = [
    STOP_ID,
    YOYO_ID
]

# Conexión a MongoDB
def get_mongo_collection():
    client = MongoClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    return db[COLLECTION_NAME]

# Consulta de documentos
query = {
    "financial_entity_id": {"$in": FINANCIAL_ENTITY_IDS},
    "amortization": {
        "$elemMatch": {
            "days_in_arrear": {"$gt": 0},
            "pending_payment": 0
        }
    }
}

# Directorio y archivo de backup
output_dir = "backups"
os.makedirs(output_dir, exist_ok=True)

def send_email_notification(execution_summary):
    """Envía una notificación por correo con el resumen de la ejecución"""
    try:
        # Verificar que las variables de email estén configuradas
        if not RESEND_API_KEY or not EMAIL_FROM or not EMAIL_TO:
            print("⚠️  Variables de email no configuradas. Saltando notificación por correo.")
            return False

        # Configurar la API key de Resend
        resend.api_key = RESEND_API_KEY
        
        print(f"🔑 API Key configurada: {RESEND_API_KEY[:10]}...")

        # Crear el contenido del email
        subject = f"📊 Mora con Saldo Cero - Resumen de Ejecución - {execution_summary['timestamp']}"
        
        # Crear el cuerpo del email en HTML
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Resumen de Ejecución</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
                .summary {{ background-color: #e9ecef; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
                .section {{ margin-bottom: 15px; }}
                .metric {{ display: flex; justify-content: space-between; margin: 5px 0; }}
                .metric-label {{ font-weight: bold; }}
                .metric-value {{ color: #007bff; }}
                .success {{ color: #28a745; }}
                .warning {{ color: #ffc107; }}
                .error {{ color: #dc3545; }}
                .files {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; }}
                .footer {{ margin-top: 20px; font-size: 12px; color: #6c757d; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚀 Mora con Saldo Cero - Corrección</h2>
                <p>Resumen de ejecución del {execution_summary['timestamp']}</p>
            </div>

            <div class="summary">
                <h3>📊 Resumen General</h3>
                <div class="metric">
                    <span class="metric-label">Documentos encontrados:</span>
                    <span class="metric-value">{execution_summary['documents_found']}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Cuotas actualizadas:</span>
                    <span class="metric-value success">{execution_summary['amortizations_updated']}</span>
                </div>
            </div>

            <div class="section">
                <h3>📁 Archivos Generados</h3>
                <div class="files">
                    <ul>
                        <li>{execution_summary['backup_file']}</li>
                    </ul>
                </div>
            </div>

            <div class="section">
                <h3>⏱️ Información de Ejecución</h3>
                <div class="metric">
                    <span class="metric-label">Fecha de ejecución:</span>
                    <span class="metric-value">{execution_summary['execution_date']}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Estado:</span>
                    <span class="metric-value success">✅ Completado exitosamente</span>
                </div>
            </div>

            <div class="footer">
                <p>Este es un mensaje automático generado por el script de corrección de mora con saldo cero.</p>
                <p>Para más información, revisa el archivo JSON generado en el directorio de backups.</p>
            </div>
        </body>
        </html>
        """

        # Crear el contenido de texto plano
        text_content = f"""
Mora con Saldo Cero - Resumen de Ejecución
==================================================

Fecha de ejecución: {execution_summary['execution_date']}

📊 RESUMEN GENERAL:
• Documentos encontrados: {execution_summary['documents_found']}
• Cuotas actualizadas: {execution_summary['amortizations_updated']}

📁 ARCHIVOS GENERADOS:
• {execution_summary['backup_file']}

Estado: ✅ Completado exitosamente

---
Este es un mensaje automático generado por el script de corrección de mora con saldo cero.
Para más información, revisa el archivo JSON generado en el directorio de backups.
        """

        # Enviar el email usando la sintaxis correcta de Resend v0.8.0
        print("📧 Preparando email...")
        print(f"   FROM: {EMAIL_FROM}")
        print(f"   TO: {EMAIL_TO}")
        print(f"   SUBJECT: {subject}")
        
        params = {
            "from": EMAIL_FROM,
            "to": [EMAIL_TO] if isinstance(EMAIL_TO, str) else EMAIL_TO,
            "subject": subject,
            "html": html_content,
        }
        
        print("📤 Enviando email...")
        response = resend.Emails.send(params)
        
        print(f"📬 Respuesta de Resend: {response}")
        
        if response and isinstance(response, dict) and 'id' in response:
            print(f"✅ Notificación por correo enviada exitosamente. ID: {response['id']}")
            return True
        else:
            print(f"❌ Error al enviar notificación por correo. Respuesta: {response}")
            return False

    except Exception as e:
        print(f"❌ Error al enviar notificación por correo: {e}")
        print(f"   Tipo de error: {type(e).__name__}")
        import traceback
        print("📋 Traceback completo:")
        traceback.print_exc()
        return False

def main():
    print("🚀 Iniciando corrección de mora con saldo cero")
    print("=" * 60)
    
    collection = get_mongo_collection()
    
    # Obtener documentos que cumplen la condición
    docs = list(collection.find(query))
    print(f"📊 Documentos encontrados: {len(docs)}")

    # Backup de los documentos
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f"{output_dir}/loan_saldo_cero_documents_{timestamp}.json"
    
    # Convertir ObjectId a string para serializar
    for doc in docs:
        doc["_id"] = str(doc["_id"])
    
    with open(backup_filename, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    print(f"📄 Backup guardado en {backup_filename}")

    # Actualizar los documentos
    total_amortizations_updated = 0
    
    for doc in docs:
        loan_id = doc["_id"]
        amortizations = doc.get("amortization", [])
        updates = []
        
        for idx, amort in enumerate(amortizations):
            try:
                if amort["days_in_arrear"] > 0 and amort["pending_payment"] == 0:
                    updates.append(idx)
            except KeyError:
                print(f"[KeyError] Crédito con id {doc['_id']}, amortización: {amort.get('_id')}")
            except TypeError:
                print(f"[TypeError] Crédito con id {doc['_id']}, amortización: {amort.get('id')}")

        if updates:
            for idx in updates:
                update_action = {"$set": {f"amortization.{idx}.days_in_arrear": 0}}
                result = collection.update_one({"_id": doc["_id"]}, update_action)
                print(f"Loan {loan_id}: Amortization index {idx} actualizado (matched: {result.matched_count}, modified: {result.modified_count})")
                if result.modified_count > 0:
                    total_amortizations_updated += 1
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN FINAL:")
    print(f"   • Documentos encontrados: {len(docs)}")
    print(f"   • Cuotas actualizadas: {total_amortizations_updated}")
    print(f"   • Archivo de backup: {backup_filename}")
    print("=" * 60)
    
    # Enviar notificación por correo
    print("\n📧 Enviando notificación por correo...")
    execution_summary = {
        'timestamp': timestamp,
        'execution_date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'documents_found': len(docs),
        'amortizations_updated': total_amortizations_updated,
        'backup_file': backup_filename
    }
    
    email_sent = send_email_notification(execution_summary)
    if email_sent:
        print("✅ Notificación por correo enviada exitosamente")
    else:
        print("⚠️  No se pudo enviar la notificación por correo")
    
    print("\n✅ Script completado")

if __name__ == "__main__":
    main()
