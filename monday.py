import httpx
import logging

logger = logging.getLogger(__name__)


async def update_monday_column(pulse_id: int, board_id: int, column_id: str, subscriber_id: str, monday_token: str, monday_api_url: str):
    """
    Actualiza una columna en Monday con el subscriber_id
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
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
            
            logger.info(f"Monday updated: item {pulse_id}, column {column_id}")
            
    except Exception as e:
        logger.error(f"Error updating Monday: {e}")
        raise


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


async def get_item_data_from_monday(pulse_id: int, monday_token: str, monday_api_url: str, phone_column_id: str):
    """
    Obtiene el nombre y teléfono de un item en Monday
    
    Returns:
        dict con 'name' y 'phone'
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            query = f'''query {{ 
                items(ids: [{pulse_id}]) {{ 
                    id 
                    name 
                    column_values {{ 
                        id 
                        text 
                        value 
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
            item = data["data"]["items"][0]
            
            name = item["name"]
            
            # Buscar la columna del teléfono en todas las column_values
            phone = None
            for col in item["column_values"]:
                if col["id"] == phone_column_id:
                    phone = col["text"]
                    break

            callsign = None
            for col in item["column_values"]:
                if col["id"] == "numeric_mm0m3ake":
                    callsign = col["text"]
                    break
            
            name = callsign + " - " + name

            logger.info(f"Retrieved data from Monday - Name: {name}, Phone: {phone}")
            
            return {
                "name": name,
                "phone": phone
            }
            
    except Exception as e:
        logger.error(f"Error getting item data from Monday: {e}")
        return None


async def get_tags_from_monday(pulse_id: int, monday_token: str, monday_api_url: str, tags_column_id: str):
    """
    Obtiene los tags de la columna dropdown en Monday
    
    Args:
        pulse_id: ID del item
        monday_token: Token de Monday
        monday_api_url: URL de la API
        tags_column_id: ID de la columna dropdown (ej: "dropdown_mm0m5pmf")
    
    Returns:
        list: Lista de nombres de tags ["Tiene 1 servicio", "Juan y Ender"]
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            query = f'''query {{ 
                items(ids: [{pulse_id}]) {{ 
                    id 
                    name 
                    column_values {{ 
                        id 
                        text 
                        value 
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
            item = data["data"]["items"][0]
            
            # Buscar la columna de tags
            tags_text = None
            for col in item["column_values"]:
                if col["id"] == tags_column_id:
                    tags_text = col["text"]
                    break
            
            if not tags_text:
                logger.info(f"No tags found for pulse {pulse_id}")
                return []
            
            # Dividir por coma y limpiar espacios
            tags = [tag.strip() for tag in tags_text.split(",")]
            
            logger.info(f"Retrieved {len(tags)} tags from Monday: {tags}")
            return tags
            
    except Exception as e:
        logger.error(f"Error getting tags from Monday: {e}")
        return []