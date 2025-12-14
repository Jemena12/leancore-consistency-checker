import os
import json
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import resend

load_dotenv()

# Configuración de la URI de MongoDB (puedes cambiar esto por una variable de entorno o input)
MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = "loan"

STOP_ID = os.getenv("STOP_ID")
YOYO_ID = os.getenv("YOYO_ID")

# Configuración de email
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")

if not STOP_ID or not YOYO_ID:
    raise Exception("Configura los IDs en las variables de entorno")

if not RESEND_API_KEY or not EMAIL_FROM or not EMAIL_TO:
    print("⚠️  Variables de email no configuradas. Las notificaciones por correo estarán deshabilitadas.")

# Directorio y archivo de backup
output_dir = "backups"
os.makedirs(output_dir, exist_ok=True)

int_keys = [
    "principal",
    "total_amount",
    "principal_payment_amount",
    "interest_amount",
    "taxes",
    "days_in_arrear",
    "pending_payment",
    "arrear_interest_amount",
    "pending_principal_payment_amount",
    "pending_interest_amount",
    "pending_interest_taxes_amount",
    "pending_arrear_interest_amount",
    "pending_guarantee_amount",
    "pending_guarantee_taxes_amount",
    "pending_other_expenses_amount",
    "period_days",
    "interest_taxes_amount",
    "guarantee_amount",
    "guarantee_taxes_amount",
    "other_expenses_amount",
    "arrear_interest_paid",
    "arrear_interest_taxes_amount",
    "pending_arrear_interest_taxes_amount",
]


def connect_to_mongodb(uri):
    """Conecta a MongoDB Atlas usando la URI proporcionada"""
    try:
        client = MongoClient(uri)
        # Verificar la conexión
        client.admin.command("ping")
        print("✅ Conexión exitosa a MongoDB Atlas")
        return client
    except Exception as e:
        print(f"❌ Error al conectar a MongoDB: {e}")
        return None


def get_loan_documents(db):
    """Obtiene los documentos de la colección loan según los criterios especificados"""
    try:
        # Consulta equivalente a la del mongo shell
        query = {
            "financial_entity_id": {"$in": [STOP_ID, YOYO_ID]},
            "status": "paid",
            "amortization": {"$elemMatch": {"days_in_arrear": {"$gt": 0}}},
        }

        loan_collection = db.loan
        results = list(loan_collection.find(query))

        return results

    except Exception as e:
        print(f"❌ Error al consultar la colección loan: {e}")
        return []


def save_to_json(data, filename):
    """Guarda los datos en un archivo JSON"""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return True

    except Exception as e:
        print(f"❌ Error al guardar el archivo JSON: {e}")
        return False


def update_amortization_arrears(db, loan_documents):
    """Actualiza los elementos de amortization que tengan days_in_arrear mayor a cero"""
    try:
        loan_collection = db.loan
        updated_loans = []

        print(f"\n🔄 Actualizando amortization para {len(loan_documents)} préstamos...")

        for i, loan_doc in enumerate(loan_documents, 1):
            loan_id = loan_doc.get("_id")
            print(f"🔍 Préstamo {i}: ID={loan_id}")

            amortization = loan_doc.get("amortization", [])

            if not amortization:
                print(f"⚠️  Préstamo {i}: No tiene amortization")
                continue

            # Contar elementos con days_in_arrear > 0
            arrear_elements = []
            update_array = []
            for j, element in enumerate(amortization):
                print("element", element)
                days_in_arrear = element.get("days_in_arrear", 0)
                days_in_arrear = int(days_in_arrear)
                print(f"🔍 Préstamo {i}: Elemento {j}: days_in_arrear={days_in_arrear}")
                print("type(days_in_arrear)", type(days_in_arrear))
                if days_in_arrear > 0:
                    arrear_elements.append(
                        {"index": j, "days_in_arrear": days_in_arrear}
                    )
                    updated_element = element.copy()
                    updated_element["days_in_arrear"] = 0
                    update_array.append(updated_element)
                else:
                    update_array.append(element)

                type_check = all([isinstance(element[key], int) for key in int_keys])
                if not type_check:
                    print(
                        f"Crédito con id {loan_id} tiene campos flotantes en la tabla de amortización {element.get('id')}"
                    )

            if not arrear_elements:
                print(f"ℹ️  Préstamo {i}: No tiene elementos con days_in_arrear > 0")
                continue

            print(
                f"📋 Préstamo {i}: Encontrados {len(arrear_elements)} elementos con days_in_arrear > 0"
            )

            # Actualizar en MongoDB
            try:
                update_result = loan_collection.update_one(
                    {"_id": loan_id}, {"$set": {"amortization": update_array}}
                )

                if update_result.modified_count > 0:
                    print(
                        f"✅ Préstamo {i}: Actualizados {len(arrear_elements)} elementos de amortization"
                    )
                    updated_loans.append(
                        {
                            "loan_id": str(loan_id),
                            "elements_updated": len(arrear_elements),
                            "arrear_elements": arrear_elements,
                        }
                    )
                else:
                    print(f"⚠️  Préstamo {i}: No se pudo actualizar")

            except Exception as update_error:
                print(f"❌ Error al actualizar préstamo {i}: {update_error}")

        # Resumen de actualizaciones
        if updated_loans:
            print(f"\n📊 RESUMEN DE ACTUALIZACIONES DE AMORTIZATION:")
            print(f"   • Préstamos actualizados: {len(updated_loans)}")
            total_elements = sum(loan["elements_updated"] for loan in updated_loans)
            print(f"   • Elementos de amortization actualizados: {total_elements}")
        else:
            print(f"\n📊 No se realizaron actualizaciones de amortization")

        return updated_loans

    except Exception as e:
        print(f"❌ Error al actualizar amortization: {e}")
        return []


def validate_user_status(db, loan_documents):
    """Valida el status de los usuarios asociados a los préstamos y actualiza según criterios"""
    try:
        user_collection = db.user
        loan_collection = db.loan
        validation_results = []
        updated_users = []

        print(f"\n🔍 Validando status de {len(loan_documents)} usuarios...")

        # Crear un set de user_ids únicos para evitar procesar el mismo usuario múltiples veces
        unique_user_ids = set()
        for loan_doc in loan_documents:
            user_id = loan_doc.get("user_id")
            if user_id:
                unique_user_ids.add(user_id)

        print(f"📊 Procesando {len(unique_user_ids)} usuarios únicos...")

        for user_id in unique_user_ids:
            # Buscar el usuario por _id
            user_query = {"_id": user_id}
            user_doc = user_collection.find_one(user_query)

            if not user_doc:
                print(f"❌ Usuario ID={user_id} - No encontrado en la colección user")
                validation_results.append(
                    {
                        "user_id": str(user_id),
                        "user_status": "No encontrado",
                        "user_found": False,
                        "loans_found": 0,
                        "status_updated": False,
                    }
                )
                continue

            user_status = user_doc.get("status", "No especificado")
            print(f"\n👤 Procesando usuario: ID={user_id}, Status actual={user_status}")

            # Si el usuario tiene status "arrear", buscar todos sus préstamos
            if user_status == "arrear":
                print(f"🔍 Usuario en arrear - buscando todos sus préstamos...")

                # Buscar todos los préstamos del usuario
                user_loans_query = {"user_id": user_id}
                user_loans = list(loan_collection.find(user_loans_query))

                print(f"📋 Encontrados {len(user_loans)} préstamos para el usuario")

                # Contar préstamos con status "arrear"
                arrear_loans = [
                    loan for loan in user_loans if loan.get("status") == "arrear"
                ]
                other_loans = [
                    loan for loan in user_loans if loan.get("status") != "arrear"
                ]

                print(f"   • Préstamos en arrear: {len(arrear_loans)}")
                print(f"   • Otros préstamos: {len(other_loans)}")

                should_update = False
                update_reason = ""

                # Lógica de actualización
                if len(user_loans) == 1:
                    # Solo tiene un préstamo
                    should_update = True
                    update_reason = "Usuario tiene solo un préstamo"
                    print(
                        f"✅ Usuario tiene solo un préstamo - marcado para actualización"
                    )
                elif len(arrear_loans) == 0:
                    # No tiene préstamos en arrear
                    should_update = True
                    update_reason = "Usuario no tiene préstamos en arrear"
                    print(
                        f"✅ Usuario no tiene préstamos en arrear - marcado para actualización"
                    )
                else:
                    update_reason = (
                        "Usuario tiene múltiples préstamos y algunos están en arrear"
                    )
                    print(
                        f"⚠️  Usuario tiene {len(arrear_loans)} préstamos en arrear - no se actualiza"
                    )

                # Actualizar status si corresponde
                if should_update:
                    try:
                        update_result = user_collection.update_one(
                            {"_id": user_id}, {"$set": {"status": "active"}}
                        )

                        if update_result.modified_count > 0:
                            print(f"🔄 Status actualizado de 'arrear' a 'active'")
                            updated_users.append(
                                {
                                    "user_id": str(user_id),
                                    "old_status": "arrear",
                                    "new_status": "active",
                                    "reason": update_reason,
                                }
                            )
                        else:
                            print(f"⚠️  No se pudo actualizar el status")

                    except Exception as update_error:
                        print(f"❌ Error al actualizar status: {update_error}")

                validation_results.append(
                    {
                        "user_id": str(user_id),
                        "user_status": user_status,
                        "user_found": True,
                        "loans_found": len(user_loans),
                        "arrear_loans": len(arrear_loans),
                        "other_loans": len(other_loans),
                        "status_updated": should_update,
                        "update_reason": update_reason,
                    }
                )

            else:
                # Usuario no está en arrear, solo registrar
                print(f"ℹ️  Usuario no está en arrear (status: {user_status})")
                validation_results.append(
                    {
                        "user_id": str(user_id),
                        "user_status": user_status,
                        "user_found": True,
                        "loans_found": 0,
                        "status_updated": False,
                    }
                )

        # Resumen de actualizaciones
        if updated_users:
            print(f"\n📊 RESUMEN DE ACTUALIZACIONES:")
            print(f"   • Usuarios actualizados: {len(updated_users)}")
            for user in updated_users:
                print(
                    f"   • {user['user_id']}: {user['old_status']} → {user['new_status']} ({user['reason']})"
                )
        else:
            print(f"\n📊 No se realizaron actualizaciones de status")

        return validation_results, updated_users

    except Exception as e:
        print(f"❌ Error al validar usuarios: {e}")
        return [], []


# ============================================================================
# NUEVA FUNCIONALIDAD: VALIDACIÓN DE CONSISTENCIA DE PAYMENT_INFO
# ============================================================================
# Esta función valida que los IDs en payment_info de la tabla de amortización
# existan realmente en la colección payment. Si encuentra IDs que no existen,
# los remueve del array payment_info para mantener la consistencia de datos.
# 
# Problema resuelto: Créditos con payment_info con IDs de transacciones que
# no existen en la colección payment, causando inconsistencias en el sistema.
# 
# TEMPORALMENTE DESACTIVADA - DESCOMENTAR CUANDO SE NECESITE USAR
# ============================================================================
# def validate_payment_info_consistency(db, loan_documents):
#     """Valida que los IDs en payment_info existan en la colección payment y limpia los que no existen"""
#     try:
#         payment_collection = db.payment
#         loan_collection = db.loan
#         validation_results = []
#         updated_loans = []

#         print(f"\n🔍 Validando consistencia de payment_info para {len(loan_documents)} préstamos...")

#         for i, loan_doc in enumerate(loan_documents, 1):
#             loan_id = loan_doc.get("_id")
#             print(f"🔍 Préstamo {i}: ID={loan_id}")

#             amortization = loan_doc.get("amortization", [])

#             if not amortization:
#                 print(f"⚠️  Préstamo {i}: No tiene amortization")
#                 continue

#             # Recopilar todos los IDs de payment_info
#             all_payment_info_ids = []
#             for j, element in enumerate(amortization):
#                 payment_info = element.get("payment_info", [])
#                 if payment_info:
#                     all_payment_info_ids.extend(payment_info)

#             if not all_payment_info_ids:
#                 print(f"ℹ️  Préstamo {i}: No tiene payment_info en amortization")
#                 continue

#             print(f"📋 Préstamo {i}: Encontrados {len(all_payment_info_ids)} IDs en payment_info")

#             # Verificar qué IDs existen en la colección payment
#             existing_payment_ids = set()
#             missing_payment_ids = []

#             for payment_id in all_payment_info_ids:
#                 # Buscar en la colección payment usando el ID de transacción
#                 payment_query = {
#                     "transactions.id": payment_id,
#                     "loan_id": loan_id
#                 }
#                 payment_doc = payment_collection.find_one(payment_query)

#                 if payment_doc:
#                     existing_payment_ids.add(payment_id)
#                     print(f"   ✅ ID {payment_id}: Encontrado en payment")
#                 else:
#                     missing_payment_ids.append(payment_id)
#                     print(f"   ❌ ID {payment_id}: NO encontrado en payment")

#             if not missing_payment_ids:
#                 print(f"✅ Préstamo {i}: Todos los payment_info son válidos")
#                 validation_results.append({
#                     "loan_id": str(loan_id),
#                     "total_payment_info": len(all_payment_info_ids),
#                     "valid_payment_info": len(existing_payment_ids),
#                     "invalid_payment_info": len(missing_payment_ids),
#                     "missing_ids": missing_payment_ids,
#                     "updated": False
#                 })
#                 continue

#             print(f"⚠️  Préstamo {i}: {len(missing_payment_ids)} IDs inválidos encontrados")

#             # Crear nueva amortización con payment_info limpiado
#             updated_amortization = []
#             elements_updated = 0

#             for j, element in enumerate(amortization):
#                 updated_element = element.copy()
#                 payment_info = element.get("payment_info", [])

#                 if payment_info:
#                     # Filtrar solo los IDs que existen en payment
#                     valid_payment_info = [pid for pid in payment_info if pid in existing_payment_ids]
                    
#                     if len(valid_payment_info) != len(payment_info):
#                         print(f"   🔄 Elemento {j}: Limpiando payment_info de {len(payment_info)} a {len(valid_payment_info)} IDs")
#                         updated_element["payment_info"] = valid_payment_info
#                         elements_updated += 1

#                 updated_amortization.append(updated_element)

#             # Actualizar en MongoDB si hay cambios
#             if elements_updated > 0:
#                 try:
#                     update_result = loan_collection.update_one(
#                         {"_id": loan_id}, 
#                         {"$set": {"amortization": updated_amortization}}
#                     )

#                     if update_result.modified_count > 0:
#                         print(f"✅ Préstamo {i}: Actualizada amortización con payment_info limpiado")
#                         updated_loans.append({
#                             "loan_id": str(loan_id),
#                             "elements_updated": elements_updated,
#                             "total_payment_info": len(all_payment_info_ids),
#                             "valid_payment_info": len(existing_payment_ids),
#                             "invalid_payment_info": len(missing_payment_ids),
#                             "missing_ids": missing_payment_ids
#                         })
#                     else:
#                         print(f"⚠️  Préstamo {i}: No se pudo actualizar")

#                 except Exception as update_error:
#                     print(f"❌ Error al actualizar préstamo {i}: {update_error}")

#             validation_results.append({
#                 "loan_id": str(loan_id),
#                 "total_payment_info": len(all_payment_info_ids),
#                 "valid_payment_info": len(existing_payment_ids),
#                 "invalid_payment_info": len(missing_payment_ids),
#                 "missing_ids": missing_payment_ids,
#                 "updated": elements_updated > 0
#             })

#         # Resumen de actualizaciones
#         if updated_loans:
#             print(f"\n📊 RESUMEN DE ACTUALIZACIONES DE PAYMENT_INFO:")
#             print(f"   • Préstamos actualizados: {len(updated_loans)}")
#             total_elements = sum(loan["elements_updated"] for loan in updated_loans)
#             total_invalid = sum(loan["invalid_payment_info"] for loan in updated_loans)
#             print(f"   • Elementos de amortization actualizados: {total_elements}")
#             print(f"   • IDs de payment_info inválidos limpiados: {total_invalid}")
#         else:
#             print(f"\n📊 No se realizaron actualizaciones de payment_info")

#         return validation_results, updated_loans

#     except Exception as e:
#         print(f"❌ Error al validar payment_info: {e}")
#         return [], []


# ============================================================================
# FIN DE LA NUEVA FUNCIONALIDAD: VALIDACIÓN DE CONSISTENCIA DE PAYMENT_INFO
# ============================================================================


def convert_utc_minus_5_to_utc(date_string):
    """Convierte una fecha de UTC-5 a UTC en formato ISO con Z"""
    try:
        # Parsear la fecha original
        original_date = datetime.fromisoformat(date_string.replace("Z", "+00:00"))

        # Si la fecha ya está en UTC (termina en Z), no necesita conversión
        if date_string.endswith("Z"):
            return date_string

        # Si la fecha está en UTC-5, convertir a UTC
        if "-05:00" in date_string:
            # Verificar si la hora es 00:00:00, entonces llevarla al final del día
            if (
                original_date.hour == 0
                and original_date.minute == 0
                and original_date.second == 0
            ):
                print(
                    f"     🕛 Fecha detectada a medianoche, llevando al final del día"
                )
                # Llevar al final del día (23:59:59) manteniendo la timezone
                original_date = original_date.replace(
                    hour=23, minute=59, second=59, microsecond=999000
                )
                print(f"     🕚 Nueva fecha en UTC-5: {original_date.isoformat()}")

            # La fecha ya está parseada correctamente, ahora convertir a UTC
            utc_date = original_date.astimezone(timezone.utc)
            # Formatear en el formato deseado: YYYY-MM-DDTHH:MM:SS.sssZ
            return utc_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # Para otros casos, asumir que necesita conversión desde UTC-5
        # Crear timezone UTC-5
        utc_minus_5 = timezone(timedelta(hours=-5))

        # Si no tiene timezone info, asumir que está en UTC-5
        if original_date.tzinfo is None:
            original_date = original_date.replace(tzinfo=utc_minus_5)

            # Verificar si la hora es 00:00:00, entonces llevarla al final del día
            if (
                original_date.hour == 0
                and original_date.minute == 0
                and original_date.second == 0
            ):
                print(
                    f"     🕛 Fecha detectada a medianoche, llevando al final del día"
                )
                original_date = original_date.replace(
                    hour=23, minute=59, second=59, microsecond=999000
                )
                print(f"     🕚 Nueva fecha en UTC-5: {original_date.isoformat()}")

        # Convertir a UTC
        utc_date = original_date.astimezone(timezone.utc)

        # Formatear en el formato deseado: YYYY-MM-DDTHH:MM:SS.sssZ
        return utc_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    except Exception as e:
        print(f"Error convirtiendo fecha {date_string}: {e}")
        return date_string  # Retornar la fecha original si hay error


def send_email_notification(execution_summary):
    """Envía una notificación por correo con el resumen de la ejecución"""
    try:
        # Verificar que las variables de email estén configuradas
        if not RESEND_API_KEY or not EMAIL_FROM or not EMAIL_TO:
            print("⚠️  Variables de email no configuradas. Saltando notificación por correo.")
            return False

        # Configurar la API key de Resend (debe estar antes de cualquier llamada)
        resend.api_key = RESEND_API_KEY
        
        # Verificar que la API key esté configurada correctamente
        print(f"🔑 API Key configurada: {RESEND_API_KEY[:10]}...")  # Solo mostrar primeros caracteres

        # Crear el contenido del email
        subject = f"📊 Resumen de Ejecución - LeanCore Consistency Checker - {execution_summary['timestamp']}"
        
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
                <h2>🚀 LeanCore Consistency Checker</h2>
                <p>Resumen de ejecución del {execution_summary['timestamp']}</p>
            </div>

            <div class="summary">
                <h3>📊 Resumen General</h3>
                <div class="metric">
                    <span class="metric-label">Documentos de loan encontrados:</span>
                    <span class="metric-value">{execution_summary['loan_documents_count']}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Préstamos con amortization actualizada:</span>
                    <span class="metric-value success">{execution_summary['amortization_updates_count']}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Usuarios validados:</span>
                    <span class="metric-value">{execution_summary['users_validated_count']}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Usuarios actualizados:</span>
                    <span class="metric-value success">{execution_summary['users_updated_count']}</span>
                </div>
            </div>

            <div class="section">
                <h3>📁 Archivos Generados</h3>
                <div class="files">
                    <ul>
        """
        
        for file in execution_summary['files_generated']:
            html_content += f"<li>{file}</li>"
        
        html_content += """
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
                <p>Este es un mensaje automático generado por el LeanCore Consistency Checker.</p>
                <p>Para más información, revisa los archivos JSON generados en el directorio de backups.</p>
            </div>
        </body>
        </html>
        """

        # Crear el contenido de texto plano
        text_content = f"""
LeanCore Consistency Checker - Resumen de Ejecución
==================================================

Fecha de ejecución: {execution_summary['execution_date']}

📊 RESUMEN GENERAL:
• Documentos de loan encontrados: {execution_summary['loan_documents_count']}
• Préstamos con amortization actualizada: {execution_summary['amortization_updates_count']}
• Usuarios validados: {execution_summary['users_validated_count']}
• Usuarios actualizados: {execution_summary['users_updated_count']}

📁 ARCHIVOS GENERADOS:
{chr(10).join(f"• {file}" for file in execution_summary['files_generated'])}

Estado: ✅ Completado exitosamente

---
Este es un mensaje automático generado por el LeanCore Consistency Checker.
Para más información, revisa los archivos JSON generados en el directorio de backups.
        """

        # Enviar el email usando la sintaxis correcta de Resend v0.8.0
        print("📧 Preparando email...")
        print(f"   FROM: {EMAIL_FROM}")
        print(f"   TO: {EMAIL_TO}")
        print(f"   SUBJECT: {subject}")
        
        # La sintaxis correcta para Resend v0.8.0
        params = {
            "from": EMAIL_FROM,
            "to": [EMAIL_TO] if isinstance(EMAIL_TO, str) else EMAIL_TO,
            "subject": subject,
            "html": html_content,
        }
        
        print("📤 Enviando email...")
        response = resend.Emails.send(params)
        
        print(f"📬 Respuesta de Resend: {response}")
        
        # La respuesta de Resend es un dict con 'id' si fue exitoso
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


def get_todays_payments_regex_approach(db):
    """Alternativa usando regex para fechas en formato string"""
    try:
        # Obtener la fecha de hoy en UTC-5
        utc_minus_5 = timezone(timedelta(hours=-5))
        today_utc_minus_5 = datetime.now(utc_minus_5).date()
        today_str = today_utc_minus_5.strftime("%Y-%m-%d")

        print(f"📅 Buscando pagos para: {today_str}")

        # Consulta usando regex para coincidir con la fecha
        # Este regex coincide con fechas que empiecen con YYYY-MM-DD
        query = {"payment_date": {"$regex": f"^{today_str}T.*-05:00$"}}

        loan_collection = db.loan
        results = list(loan_collection.find(query))

        print(
            f"✅ Encontrados {len(results)} créditos con pago programado para hoy (UTC-5)"
        )

        # Guardar resultados en archivo JSON
        print("\n📋 Guardando resultados en archivo JSON...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/payment_loan_documents_{timestamp}.json"

        if save_to_json(results, filename):
            print(f"📄 Archivo creado: {filename}")

        # Convertir fechas de UTC-5 a UTC
        for result in results:
            original_date = result.get("payment_date")
            original_limit_date = result.get("limit_payment_date")
            print(
                f"   • Crédito ID: {result.get('_id')}, Payment Date Original: {original_date}, Limit Payment Date Original: {original_limit_date}"
            )

            if original_date:
                try:
                    # Convertir la fecha de UTC-5 a UTC
                    utc_payment_date = convert_utc_minus_5_to_utc(original_date)
                    # Actualizar payment_date en la base de datos
                    loan_collection.update_one(
                        {"_id": result.get("_id")},
                        {"$set": {"payment_date": utc_payment_date}},
                    )
                except Exception as e:
                    print(f"     ❌ Error convirtiendo fecha: {e}")

            if original_limit_date:
                try:
                    # Convertir la fecha de límite de pago de UTC-5 a UTC
                    utc_limit_date = convert_utc_minus_5_to_utc(original_limit_date)
                    # Actualizar limit_payment_date en la base de datos
                    loan_collection.update_one(
                        {"_id": result.get("_id")},
                        {"$set": {"limit_payment_date": utc_limit_date}},
                    )
                except Exception as e:
                    print(f"     ❌ Error convirtiendo fecha: {e}")

        return results

    except Exception as e:
        print(f"❌ Error al consultar pagos de hoy: {e}")
        return []


def main():
    """Función principal del script"""
    print("🚀 Iniciando script de consulta MongoDB Atlas")
    print("=" * 50)

    # Solicitar la URI de MongoDB Atlas
    uri = MONGODB_URI

    # Conectar a MongoDB
    client = connect_to_mongodb(uri)
    if not client:
        return

    try:
        # Seleccionar la base de datos middleware
        db = client[DATABASE_NAME]
        print(f"📂 Conectado a la base de datos: middleware")

        get_todays_payments_regex_approach(db)

        # Paso 1: Obtener documentos de la colección loan
        print("\n📋 Paso 1: Consultando colección loan...")
        loan_documents = get_loan_documents(db)

        if not loan_documents:
            print("⚠️  No se encontraron documentos que cumplan los criterios")
            return

        # Paso 2: Guardar resultados en archivo JSON
        print("\n📋 Paso 2: Guardando resultados en archivo JSON...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/loan_documents_{timestamp}.json"

        if save_to_json(loan_documents, filename):
            print(f"📄 Archivo creado: {filename}")

        # Paso 3: Actualizar amortization
        print("\n📋 Paso 3: Actualizando amortization...")
        amortization_updates = update_amortization_arrears(db, loan_documents)

        # Paso 4: Validar status de usuarios
        print("\n📋 Paso 4: Validando status de usuarios...")
        validation_results, updated_users = validate_user_status(db, loan_documents)

        # Guardar resultados de validación
        validation_filename = f"{output_dir}/user_validation_{timestamp}.json"
        if save_to_json(validation_results, validation_filename):
            print(f"📄 Resultados de validación guardados en: {validation_filename}")

        # Guardar resultados de actualizaciones de usuarios
        if updated_users:
            user_updates_filename = f"{output_dir}/user_updates_{timestamp}.json"
            if save_to_json(updated_users, user_updates_filename):
                print(
                    f"📄 Resultados de actualizaciones de usuarios guardados en: {user_updates_filename}"
                )

        # Paso 5: Validar consistencia de payment_info (TEMPORALMENTE DESACTIVADO)
        # print("\n📋 Paso 5: Validando consistencia de payment_info...")
        # payment_info_validation_results, payment_info_updates = validate_payment_info_consistency(db, loan_documents)

        # Guardar resultados de validación de payment_info
        # payment_info_validation_filename = f"{output_dir}/payment_info_validation_{timestamp}.json"
        # if save_to_json(payment_info_validation_results, payment_info_validation_filename):
        #     print(f"📄 Resultados de validación de payment_info guardados en: {payment_info_validation_filename}")

        # Guardar resultados de actualizaciones de payment_info
        # if payment_info_updates:
        #     payment_info_updates_filename = f"{output_dir}/payment_info_updates_{timestamp}.json"
        #     if save_to_json(payment_info_updates, payment_info_updates_filename):
        #         print(
        #             f"📄 Resultados de actualizaciones de payment_info guardados en: {payment_info_updates_filename}"
        #         )

        # Guardar resultados de actualizaciones de amortization
        if amortization_updates:
            amortization_updates_filename = f"amortization_updates_{timestamp}.json"
            if save_to_json(amortization_updates, amortization_updates_filename):
                print(
                    f"📄 Resultados de actualizaciones de amortization guardados en: {amortization_updates_filename}"
                )

        # Resumen final
        print("\n" + "=" * 50)
        print("📊 RESUMEN FINAL:")
        print(f"   • Documentos de loan encontrados: {len(loan_documents)}")
        print(
            f"   • Préstamos con amortization actualizada: {len(amortization_updates)}"
        )
        print(f"   • Usuarios validados: {len(validation_results)}")
        print(f"   • Usuarios actualizados: {len(updated_users)}")
        # print(f"   • Préstamos con payment_info validados: {len(payment_info_validation_results)}")
        # print(f"   • Préstamos con payment_info actualizado: {len(payment_info_updates)}")

        files_generated = [filename, validation_filename]  # , payment_info_validation_filename]
        if updated_users:
            files_generated.append(user_updates_filename)
        if amortization_updates:
            files_generated.append(amortization_updates_filename)
        # if payment_info_updates:
        #     files_generated.append(payment_info_updates_filename)

        print(f"   • Archivos generados: {', '.join(files_generated)}")
        print("=" * 50)

        # Enviar notificación por correo
        print("\n📧 Enviando notificación por correo...")
        execution_summary = {
            'timestamp': timestamp,
            'execution_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'loan_documents_count': len(loan_documents),
            'amortization_updates_count': len(amortization_updates),
            'users_validated_count': len(validation_results),
            'users_updated_count': len(updated_users),
            'files_generated': files_generated
        }
        
        email_sent = send_email_notification(execution_summary)
        if email_sent:
            print("✅ Notificación por correo enviada exitosamente")
        else:
            print("⚠️  No se pudo enviar la notificación por correo")

    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")

    finally:
        # Cerrar conexión
        client.close()
        print("🔌 Conexión cerrada")


if __name__ == "__main__":
    main()
