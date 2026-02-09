import httpx
import logging

logger = logging.getLogger(__name__)

async def create_subscriber_and_send_flow(name: str, phone: str, pulse_id: int, flow_ns: str, api_key: str, base_url: str, field_id: int, monday_token: str, monday_api_url: str, board_id: int, column_id: str):
    subscriber_id = None
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{base_url}/subscriber/createSubscriber",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "first_name": name,
                    "phone": phone,
                    "whatsapp_phone": phone,
                    "has_opt_in_sms": True,
                    "has_opt_in_email": True,
                    "consent_phrase": "string"
                }
            )
            subscriber_id = response.json()["data"]["id"]
            
            await client.post(
                f"{base_url}/subscriber/setCustomField",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "subscriber_id": subscriber_id,
                    "field_id": field_id,
                    "field_value": pulse_id
                }
            )
            
            mutation = f'''mutation {{
                change_column_value(
                    item_id: {pulse_id},
                    board_id: {board_id},
                    column_id: "{column_id}",
                    value: "{subscriber_id}"
                ) {{ id }}
            }}'''
            
            await client.post(
                monday_api_url,
                headers={
                    "Authorization": monday_token,
                    "Content-Type": "application/json"
                },
                json={"query": mutation}
            )
            
            await client.post(
                f"{base_url}/sending/sendFlow",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"subscriber_id": int(subscriber_id), "flow_ns": flow_ns}
            )
            
            logger.info(f"Sync completed: Monday {pulse_id} <-> ManyChat {subscriber_id}")
            
    except Exception as e:
        logger.error(f"Error: {e}")

async def send_flow_to_subscriber(subscriber_id: int, flow_ns: str, api_key: str, base_url: str):
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(
                f"{base_url}/sending/sendFlow",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"subscriber_id": subscriber_id, "flow_ns": flow_ns}
            )
            logger.info(f"Flow {flow_ns} sent to subscriber {subscriber_id}")
            
    except Exception as e:
        logger.error(f"Error sending flow: {e}")