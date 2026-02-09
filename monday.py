import httpx
import logging

logger = logging.getLogger(__name__)

async def get_subscriber_id_from_monday(pulse_id: int, monday_token: str, monday_api_url: str, column_id: str):
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            query = f'''query {{
                items(ids: [{pulse_id}]) {{
                    column_values(ids: ["{column_id}"]) {{
                        id
                        value
                        text
                    }}
                }}
            }}'''
            
            response = await client.post(
                monday_api_url,
                headers={
                    "Authorization": monday_token,
                    "Content-Type": "application/json"
                },
                json={"query": query}
            )
            
            data = response.json()
            value = data["data"]["items"][0]["column_values"][0]["value"]
            subscriber_id = int(value.strip('"'))
            
            logger.info(f"Retrieved subscriber_id {subscriber_id} for pulse {pulse_id}")
            return subscriber_id
            
    except Exception as e:
        logger.error(f"Error getting subscriber from Monday: {e}")
        return None