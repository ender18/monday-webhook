import asyncio
import logging
from manychat import *
from monday import *
from config import *

logger = logging.getLogger(__name__)

def extract_data(event: dict):
    return {
        "name": event.get("pulseName", ""),
        "phone": event.get("columnValues", {}).get("phonetelkfeip", {}).get("phone", ""),
        "email": event.get("columnValues", {}).get("email_mkq1kgh3", {}).get("email", ""),
        "group_name": event.get("groupName", ""),
        "board_id": event.get("boardId"),
        "pulse_id": event.get("pulseId")
    }

async def handle_create_pulse(event: dict):
    data = extract_data(event)
    
    if data["name"] and data["phone"] and data["pulse_id"]:
        flow_ns = "content20260207011126_045289"
        asyncio.create_task(
            create_subscriber_and_send_flow(
                data["name"], 
                data["phone"],
                data["pulse_id"],
                flow_ns,
                MANYCHAT_API_KEY,
                MANYCHAT_BASE_URL,
                MANYCHAT_FIELD_ID,
                MONDAY_TOKEN,
                MONDAY_API_URL,
                MONDAY_BOARD_ID,
                MONDAY_COLUMN_ID
            )
        )

async def handle_update_column_value(event: dict):
    """
    Maneja el webhook de Monday cuando se actualiza una columna
    """
    pulse_id = event.get("pulseId")
    column_id = event.get("columnId")
    value = event.get("value", {}).get("label", {}).get("text", "")
    
    # Caso 1: Create - Crear suscriptor en ManyChat
    if column_id == "color_mm0mavpp" and value == "Create" and pulse_id:
        logger.info(f"Procesando Create para pulse {pulse_id}")
        
        # 1. Obtener datos del item en Monday (nombre y teléfono)
        item_data = await get_item_data_from_monday(
            pulse_id,
            MONDAY_TOKEN,
            MONDAY_API_URL,
            MONDAY_PHONE_COLUMN_ID
        )
        
        if not item_data:
            logger.error(f"No se pudo obtener datos del item {pulse_id}")
            return
        
        name = item_data["name"]
        phone = item_data["phone"]
        
        logger.info(f"Creando suscriptor - Name: {name}, Phone: {phone}")
        
        # 2. Crear suscriptor en ManyChat
        try:
            subscriber_id = await create_subscriber(
                name=name,
                phone=phone,
                pulse_id=pulse_id,
                api_key=MANYCHAT_API_KEY,
                base_url=MANYCHAT_BASE_URL,
                field_id=MANYCHAT_FIELD_ID
            )
            
            logger.info(f"Suscriptor creado en ManyChat: {subscriber_id}")
            
            # 3. Actualizar columna en Monday con el subscriber_id
            await update_monday_column(
                pulse_id=pulse_id,
                board_id=MONDAY_BOARD_ID,
                column_id=MONDAY_SUBSCRIBER_COLUMN_ID,
                subscriber_id=str(subscriber_id),
                monday_token=MONDAY_TOKEN,
                monday_api_url=MONDAY_API_URL
            )
            
            logger.info(f"✅ Proceso completado: Monday {pulse_id} <-> ManyChat {subscriber_id}")
            
        except Exception as e:
            logger.error(f"Error en el proceso de creación: {e}")
    
    # Caso 2: Validated - Enviar flow (lo agregarás después)
    elif column_id == "color_mm0mavpp" and value == "Sync" and pulse_id:
        logger.info(f"Procesando Sync para pulse {pulse_id}")

        # 1. Obtener subscriber_id de Monday
        subscriber_id = await get_subscriber_id_from_monday(
            pulse_id,
            MONDAY_TOKEN,
            MONDAY_API_URL,
            MONDAY_COLUMN_ID
        )

        # 2. Eliminar todos los tags actuales
        tags_eliminadas = await remove_all_tags(
            subscriber_id=subscriber_id,
            api_key=MANYCHAT_API_KEY,
            base_url=MANYCHAT_BASE_URL
        )
        logger.info(f"Tags eliminados: {tags_eliminadas}")

        # 3. Obtener tags de Monday
        tags = await get_tags_from_monday(
            pulse_id=pulse_id,
            monday_token=MONDAY_TOKEN,
            monday_api_url=MONDAY_API_URL,
            tags_column_id="dropdown_mm0m5pmf"  # Tu columna de tags
        )
        logger.info(f"Tags obtenidos de Monday: {tags}")

        # 4. Agregar los nuevos tags
        if tags:
            tags_agregados = await add_multiple_tags(
                subscriber_id=subscriber_id,
                tag_names=tags,
                api_key=MANYCHAT_API_KEY,
                base_url=MANYCHAT_BASE_URL
            )
            logger.info(f"✅ Sync completado: {tags_eliminadas} eliminados, {tags_agregados} agregados")



    # Más casos se pueden agregar aquí...

async def handle_move_pulse_into_group(event: dict):
    pulse_id = event.get("pulseId")
    dest_group_id = event.get("destGroupId")
    dest_group_title = event.get("destGroup", {}).get("title", "")
    
    if dest_group_id == "group_mm0dfs55" and dest_group_title == "Onboarding" and pulse_id:
        subscriber_id = await get_subscriber_id_from_monday(
            pulse_id,
            MONDAY_TOKEN,
            MONDAY_API_URL,
            MONDAY_COLUMN_ID
        )
        
        if subscriber_id:
            flow_ns = "content20260209061400_261451"
            asyncio.create_task(
                send_flow_to_subscriber(
                    subscriber_id,
                    flow_ns,
                    MANYCHAT_API_KEY,
                    MANYCHAT_BASE_URL
                )
            )

async def handle_update_pulse(event: dict):
    pass

async def handle_delete_pulse(event: dict):
    pass

EVENT_HANDLERS = {
    "create_pulse": handle_create_pulse,
    "update_column_value": handle_update_column_value,
    "move_pulse_into_group": handle_move_pulse_into_group,
    "update_pulse": handle_update_pulse,
    "delete_pulse": handle_delete_pulse,
}

async def process_event(event: dict):
    event_type = event.get("type")
    handler = EVENT_HANDLERS.get(event_type)
    
    if handler:
        await handler(event)
    else:
        logger.warning(f"Unknown event type: {event_type}")