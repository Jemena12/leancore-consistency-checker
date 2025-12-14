"""
Script para identificar transacciones de pago no aplicadas.
Configurado para obtener pagos de los últimos 2 días.
Optimizado para excluir préstamos con status "paid" en la consulta a la BD.
"""
import csv
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import resend

load_dotenv()

# Configuración de email
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")

if not RESEND_API_KEY or not EMAIL_FROM or not EMAIL_TO:
    print("⚠️  Variables de email no configuradas. Las notificaciones por correo estarán deshabilitadas.")


def connect_to_mongodb():
    """
    Connect to MongoDB using environment variables MONGODB_URI and DATABASE_NAME.
    """
    mongodb_uri = os.getenv("MONGODB_URI")
    database_name = os.getenv("DATABASE_NAME")

    if not mongodb_uri:
        raise ValueError("MONGODB_URI environment variable is not set")
    if not database_name:
        raise ValueError("DATABASE_NAME environment variable is not set")

    try:
        client = MongoClient(mongodb_uri)
        db = client[database_name]
        db.command("ping")
        print(f"Successfully connected to MongoDB database: {database_name}")
        return db
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        raise


def get_unapplied_transactions(db, date_range="recent", limit=None):
    """
    Obtiene los transaction id de la colección payment que no están aplicados en loan.amortization.payment_info.
    
    Args:
        db: Conexión a la base de datos MongoDB
        date_range: Rango de fechas a consultar ("recent", "august", "september")
        limit: Número máximo de pagos a procesar (None = sin límite)
    """
    if date_range == "august":
        # Obtener todos los pagos de agosto 2025
        august_start = "2025-08-01"
        august_end = "2025-09-01"  # Incluir todo agosto usando $lt
        
        # IDs de entidades financieras
        STOP_ID = os.getenv("STOP_ID")
        YOYO_ID = os.getenv("YOYO_ID")
        
        query = {
            "date": {
                "$gte": august_start,
                "$lt": august_end
            },
            "financial_entity_id": {
                "$in": [STOP_ID, YOYO_ID]
            }
        }
        
        if limit:
            payments = list(db.payment.find(query).limit(limit))
            print(f"Payments fetched for August 2025 (YOYO & STOP only) - LIMITED TO {limit}: {len(payments)}")
        else:
            payments = list(db.payment.find(query))
            print(f"Payments fetched for August 2025 (YOYO & STOP only): {len(payments)}")
        
    elif date_range == "september":
        # Obtener todos los pagos de septiembre 2025
        september_start = "2025-09-01"
        september_end = "2025-10-01"  # Incluir todo septiembre usando $lt
        
        # IDs de entidades financieras
        STOP_ID = os.getenv("STOP_ID")
        YOYO_ID = os.getenv("YOYO_ID")
        
        query = {
            "date": {
                "$gte": september_start,
                "$lt": september_end
            },
            "financial_entity_id": {
                "$in": [STOP_ID, YOYO_ID]
            }
        }

        if date_range == "october":
            october_start= "2025-10-01"
            october_end = "2025-11-01"  # Incluir todo octubre usando $lt

            query = {
                "date": {
                    "$gte": october_start,
                    "$lt": october_end
                },
            }
        
        if limit:
            payments = list(db.payment.find(query).limit(limit))
            print(f"Payments fetched for September 2025 (YOYO & STOP only) - LIMITED TO {limit}: {len(payments)}")
        else:
            payments = list(db.payment.find(query))
            print(f"Payments fetched for September 2025 (YOYO & STOP only): {len(payments)}")
        
    elif date_range == "october":
        october_start = "2025-10-01"
        october_end = "2025-11-01"  # Incluir todo octubre usando $lt

        query = {
            "date": {
                "$gte": october_start,
                "$lt": october_end
            },
        }
        
        if limit:
            payments = list(db.payment.find(query).limit(limit))
            print(f"Payments fetched for October 2025 (YOYO & STOP only) - LIMITED TO {limit}: {len(payments)}")
        else:
            payments = list(db.payment.find(query))
            print(f"Payments fetched for October 2025 (YOYO & STOP only): {len(payments)}")
        
    else:  # recent (últimos 2 días)
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=2)
        yesterday = yesterday.isoformat()
        payments = list(db.payment.find({"date": {"$gte": yesterday}}))
        print(f"Payments fetched since {yesterday}: {len(payments)}")

    count = 0

    unapplied_payments = []
    inconsistent_loans = set()  # Para almacenar IDs únicos de préstamos con inconsistencias
    for payment in payments:
        print(f"Processing payment {count + 1}/{len(payments)}")
        count += 1
        payment_transactions = payment.get("transactions", [])

        loan = db.loan.find_one({
            "_id": payment.get("loan_id"),
            "status": {"$ne": "paid"}  # Excluir préstamos con status "paid"
        })
        if not loan:
            # Si no se encuentra el préstamo o tiene status "paid", omitir
            loan_exists = db.loan.find_one({"_id": payment.get("loan_id")})
            if loan_exists and loan_exists.get("status") == "paid":
                print(f"⏭️  Omitiendo préstamo con status 'paid': {payment.get('loan_id')}")
                continue
            else:
                print(f"⚠️  Préstamo no encontrado: {payment.get('loan_id')}")
                inconsistent_loans.add(str(payment.get("loan_id")))
                continue
            
        loan_amortization = loan.get("amortization", [])
        if not loan_amortization:
            print(f"⚠️  Préstamo {payment.get('loan_id')} no tiene tabla de amortización")
            inconsistent_loans.add(str(payment.get("loan_id")))
            continue

        # Obtener el payment_id para referencia
        payment_id = str(payment.get("_id"))
        
        # Recopilar todos los términos (cuotas) a los que aplica este pago
        terms_in_payment = set()
        for transaction in payment_transactions:
            transaction_details = transaction.get("details", {})
            term = transaction_details.get("term")

            # Validar que el término sea válido
            if not term or term < 1 or term > len(loan_amortization):
                print(f"⚠️  Término inválido {term} para préstamo {payment.get('loan_id')} (amortización tiene {len(loan_amortization)} períodos)")
                inconsistent_loans.add(str(payment.get("loan_id")))
                continue

            terms_in_payment.add(term)
        
        # Verificar que cada cuota mencionada en el pago tenga al menos un ID en payment_info
        for term in terms_in_payment:
            payment_period = loan_amortization[term - 1]
            payment_info = payment_period.get("payment_info", [])

            # Verificar si payment_info está vacío (no hay pagos aplicados)
            if not payment_info or len(payment_info) == 0:
                # La cuota NO tiene ningún pago aplicado
                transaction_ids = [t.get("id") for t in payment_transactions if t.get("details", {}).get("term") == term]
                
                print(
                    {
                        "payment_id": payment_id,
                        "loan_id": str(payment.get("loan_id")),
                        "transaction_ids": transaction_ids,
                        "term": term,
                        "issue": "payment_info is empty",
                        "payment_info": payment_info
                    }
                )
                unapplied_payments.append(
                    {
                        "payment_id": payment_id,
                        "loan_id": str(payment.get("loan_id")),
                        "transaction_ids": ",".join(transaction_ids),
                        "term": term,
                        "issue": "payment_info_empty"
                    }
                )
                # Agregar el loan_id a la lista de inconsistencias
                inconsistent_loans.add(str(payment.get("loan_id")))

    # Convertir a lista y ordenar para evitar duplicados
    unique_inconsistent_loans = sorted(list(inconsistent_loans))
    return unapplied_payments, unique_inconsistent_loans, len(payments)


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
        subject = f"📊 Pagos No Aplicados - Resumen de Ejecución - {execution_summary['timestamp']}"
        
        # Crear el cuerpo del email en HTML
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Resumen de Ejecución - Pagos No Aplicados</title>
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
                <h2>🔍 Pagos No Aplicados - Análisis</h2>
                <p>Resumen de ejecución del {execution_summary['timestamp']}</p>
            </div>

            <div class="summary">
                <h3>📊 Resumen General</h3>
                <div class="metric">
                    <span class="metric-label">Pagos procesados:</span>
                    <span class="metric-value">{execution_summary['payments_processed']}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Transacciones no aplicadas:</span>
                    <span class="metric-value warning">{execution_summary['unapplied_transactions']}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Préstamos con inconsistencias:</span>
                    <span class="metric-value error">{execution_summary['inconsistent_loans']}</span>
                </div>
            </div>

            <div class="section">
                <h3>📁 Archivos Generados</h3>
                <div class="files">
                    <ul>
                        <li>{execution_summary['csv_file']}</li>
                        <li>{execution_summary['txt_file']}</li>
                    </ul>
                </div>
            </div>

            <div class="section">
                <h3>⏱️ Información de Ejecución</h3>
                <div class="metric">
                    <span class="metric-label">Rango de fechas:</span>
                    <span class="metric-value">{execution_summary['date_range']}</span>
                </div>
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
                <p>Este es un mensaje automático generado por el script de análisis de pagos no aplicados.</p>
                <p>Para más información, revisa los archivos CSV y TXT generados.</p>
            </div>
        </body>
        </html>
        """

        # Crear el contenido de texto plano
        text_content = f"""
Pagos No Aplicados - Resumen de Ejecución
==================================================

Fecha de ejecución: {execution_summary['execution_date']}
Rango de fechas: {execution_summary['date_range']}

📊 RESUMEN GENERAL:
• Pagos procesados: {execution_summary['payments_processed']}
• Transacciones no aplicadas: {execution_summary['unapplied_transactions']}
• Préstamos con inconsistencias: {execution_summary['inconsistent_loans']}

📁 ARCHIVOS GENERADOS:
• {execution_summary['csv_file']}
• {execution_summary['txt_file']}

Estado: ✅ Completado exitosamente

---
Este es un mensaje automático generado por el script de análisis de pagos no aplicados.
Para más información, revisa los archivos CSV y TXT generados.
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


if __name__ == "__main__":
    import sys
    
    # Determinar el rango de fechas desde argumentos de línea de comandos
    date_range = "recent"  # Por defecto: últimos 2 días
    limit = None  # Por defecto: sin límite
    
    if len(sys.argv) > 1:
        date_range = sys.argv[1].lower()
        if date_range not in ["recent", "august", "september", "october"]:
            print("❌ Rango de fechas inválido. Usa: recent, august, september, o october")
            sys.exit(1)
    
    # Segundo argumento opcional: límite de pagos
    if len(sys.argv) > 2:
        try:
            limit = int(sys.argv[2])
            print(f"🧪 MODO TEST: Limitando a {limit} pagos")
        except ValueError:
            print("❌ El límite debe ser un número entero")
            sys.exit(1)
    
    print(f"🔍 Procesando pagos: {date_range}")
    print("=" * 60)
    
    db = connect_to_mongodb()
    unapplied, inconsistent_loan_ids, total_payments_processed = get_unapplied_transactions(db, date_range, limit)
    
    print("\n📊 Resumen:")
    print(f"   • Pagos procesados: {total_payments_processed}")
    print(f"   • Unapplied transactions: {len(unapplied)}")
    print(f"   • Inconsistent loans: {len(inconsistent_loan_ids)}")

    # Determinar nombres de archivos según el rango de fechas
    test_suffix = f"_test_{limit}" if limit else ""
    
    if date_range == "august":
        csv_file = f"unapplied_transactions_august_2025{test_suffix}.csv"
        inconsistent_file = f"inconsistent_loans_august_2025{test_suffix}.txt"
        description = f"en agosto 2025{' (TEST con ' + str(limit) + ' pagos)' if limit else ''}"
    elif date_range == "september":
        csv_file = f"unapplied_transactions_september_2025{test_suffix}.csv"
        inconsistent_file = f"inconsistent_loans_september_2025{test_suffix}.txt"
        description = f"en septiembre 2025{' (TEST con ' + str(limit) + ' pagos)' if limit else ''}"
    else:
        csv_file = f"unapplied_transactions_recent{test_suffix}.csv"
        inconsistent_file = f"inconsistent_loans_recent{test_suffix}.txt"
        description = f"en los últimos 2 días{' (TEST con ' + str(limit) + ' pagos)' if limit else ''}"

    # Exportar transacciones no aplicadas a CSV
    if unapplied:
        with open(csv_file, mode="w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=unapplied[0].keys())
            writer.writeheader()
            writer.writerows(unapplied)
        print(f"\n📄 Exported unapplied transactions to {csv_file}")
    else:
        print(f"\n✅ No unapplied transactions to export.")

    # Exportar IDs de créditos con inconsistencias
    if inconsistent_loan_ids:
        # Eliminar duplicados y ordenar
        unique_loan_ids = sorted(list(set(inconsistent_loan_ids)))
        
        with open(inconsistent_file, mode="w") as f:
            f.write(f"IDs de créditos con inconsistencias encontradas {description}:\n")
            f.write("=" * 60 + "\n\n")
            for loan_id in unique_loan_ids:
                f.write(f"{loan_id}\n")
            f.write("\n")  # Línea vacía al final
        
        print(f"📄 Exported {len(unique_loan_ids)} unique inconsistent loan IDs to {inconsistent_file}")
        if len(inconsistent_loan_ids) != len(unique_loan_ids):
            print(f"   • Duplicados eliminados: {len(inconsistent_loan_ids) - len(unique_loan_ids)}")
    else:
        print("✅ No inconsistent loans found.")
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN FINAL:")
    print(f"   • Pagos procesados: {total_payments_processed}")
    print(f"   • Transacciones no aplicadas: {len(unapplied)}")
    print(f"   • Préstamos con inconsistencias: {len(inconsistent_loan_ids)}")
    print(f"   • Archivo CSV: {csv_file}")
    print(f"   • Archivo TXT: {inconsistent_file}")
    print("=" * 60)
    
    # Enviar notificación por correo
    print("\n📧 Enviando notificación por correo...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    execution_summary = {
        'timestamp': timestamp,
        'execution_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'date_range': date_range,
        'payments_processed': total_payments_processed,  # Dato real de pagos procesados
        'unapplied_transactions': len(unapplied),
        'inconsistent_loans': len(inconsistent_loan_ids),
        'csv_file': csv_file,
        'txt_file': inconsistent_file
    }
    
    email_sent = send_email_notification(execution_summary)
    if email_sent:
        print("✅ Notificación por correo enviada exitosamente")
    else:
        print("⚠️  No se pudo enviar la notificación por correo")
    
    print("\n✅ Proceso completado")
