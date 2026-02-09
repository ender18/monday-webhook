import asyncio
import logging
from manychat import create_subscriber_and_send_flow, send_flow_to_subscriber
from monday import get_subscriber_id_from_monday
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
    pulse_id = event.get("pulseId")
    column_id = event.get("columnId")
    value = event.get("value", {}).get("label", {}).get("text", "")
    
    if column_id == "status" and value == "Validated" and pulse_id:
        subscriber_id = await get_subscriber_id_from_monday(
            pulse_id,
            MONDAY_TOKEN,
            MONDAY_API_URL,
            MONDAY_COLUMN_ID
        )
        
        if subscriber_id:
            flow_ns = "content20260209055543_080582"
            asyncio.create_task(
                send_flow_to_subscriber(
                    subscriber_id,
                    flow_ns,
                    MANYCHAT_API_KEY,
                    MANYCHAT_BASE_URL
                )
            )

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