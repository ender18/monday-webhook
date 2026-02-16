import httpx
import logging

logger = logging.getLogger(__name__)


async def create_subscriber(name: str, phone: str, pulse_id: int, api_key: str, base_url: str, field_id: int):
    """
    Crea un suscriptor en ManyChat y guarda el pulse_id en campo custom
    
    Returns:
        subscriber_id: ID del suscriptor creado
    """
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
            
            logger.info(f"Subscriber created: {subscriber_id}")
            return subscriber_id
            
    except Exception as e:
        logger.error(f"Error creating subscriber: {e}")
        raise


async def send_flow_to_subscriber(subscriber_id: int, flow_ns: str, api_key: str, base_url: str):
    """
    Envía un flow a un suscriptor
    """
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
        raise

async def get_subscriber_info(subscriber_id: int, api_key: str, base_url: str):
    """
    Obtiene la información completa de un suscriptor incluyendo tags
    
    Returns:
        dict con toda la info del suscriptor
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{base_url}/subscriber/getInfo",
                headers={"Authorization": f"Bearer {api_key}"},
                params={"subscriber_id": subscriber_id}
            )
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Info obtenida para subscriber {subscriber_id}")
            return result.get("data", {})
            
    except Exception as e:
        logger.error(f"Error getting subscriber info: {e}")
        raise


async def remove_tag(subscriber_id: int, tag_id: int, api_key: str, base_url: str):
    """
    Elimina una etiqueta específica de un suscriptor
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(
                f"{base_url}/subscriber/removeTag",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"subscriber_id": subscriber_id, "tag_id": tag_id}
            )
            logger.info(f"Tag {tag_id} eliminado del subscriber {subscriber_id}")
            
    except Exception as e:
        logger.error(f"Error removing tag: {e}")
        raise


async def remove_all_tags(subscriber_id: int, api_key: str, base_url: str):
    """
    Elimina todas las etiquetas de un suscriptor
    
    Returns:
        int: Cantidad de etiquetas eliminadas
    """
    try:
        # 1. Obtener info del suscriptor para ver sus tags
        subscriber_info = await get_subscriber_info(subscriber_id, api_key, base_url)
        
        tags = subscriber_info.get("tags", [])
        
        if not tags:
            logger.info(f"Subscriber {subscriber_id} no tiene etiquetas")
            return 0
        
        logger.info(f"Eliminando {len(tags)} etiquetas del subscriber {subscriber_id}")
        
        # 2. Eliminar cada tag
        for tag in tags:
            tag_id = tag.get("id")
            tag_name = tag.get("name")
            
            await remove_tag(subscriber_id, tag_id, api_key, base_url)
            logger.info(f"  ✓ Eliminada: {tag_name} (ID: {tag_id})")
        
        logger.info(f"✅ Todas las etiquetas eliminadas del subscriber {subscriber_id}")
        return len(tags)
        
    except Exception as e:
        logger.error(f"Error removing all tags: {e}")
        raise

async def add_tag_by_name(subscriber_id: int, tag_name: str, api_key: str, base_url: str):
    """
    Agrega una etiqueta a un suscriptor usando el nombre del tag
    (Crea el tag si no existe)
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(
                f"{base_url}/subscriber/addTagByName",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"subscriber_id": subscriber_id, "tag_name": tag_name}
            )
            logger.info(f"Tag '{tag_name}' agregado al subscriber {subscriber_id}")
            
    except Exception as e:
        logger.error(f"Error adding tag by name: {e}")
        raise


async def add_multiple_tags(subscriber_id: int, tag_names: list, api_key: str, base_url: str):
    """
    Agrega múltiples etiquetas a un suscriptor
    
    Args:
        subscriber_id: ID del suscriptor
        tag_names: Lista de nombres de tags ["Tag1", "Tag2", ...]
        api_key: API key de ManyChat
        base_url: URL base de la API
    
    Returns:
        int: Cantidad de tags agregados
    """
    try:
        if not tag_names:
            logger.info(f"No hay tags para agregar al subscriber {subscriber_id}")
            return 0
        
        logger.info(f"Agregando {len(tag_names)} tags al subscriber {subscriber_id}")
        
        for tag_name in tag_names:
            await add_tag_by_name(subscriber_id, tag_name, api_key, base_url)
            logger.info(f"  ✓ Agregado: {tag_name}")
        
        logger.info(f"✅ Todos los tags agregados al subscriber {subscriber_id}")
        return len(tag_names)
        
    except Exception as e:
        logger.error(f"Error adding multiple tags: {e}")
        raise