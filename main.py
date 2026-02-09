from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
import json
from handlers import process_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.post("/")
async def monday_webhook(request: Request):
    body = await request.json()
    
    print("\n" + "="*80)
    print("📥 WEBHOOK RECIBIDO:")
    print(json.dumps(body, indent=2, ensure_ascii=False))
    print("="*80 + "\n")
    
    await process_event(body.get("event", {}))
    return JSONResponse(content={"status": "received"}, status_code=200)