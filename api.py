# api.py
import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv

# Import your custom modules
from ChatGPTIntegration import ChatGPTAdapter
from BigQueryIntergration import BigQueryClient
from CoreDatamodels import ParsedRequest, Appointment
from AppointmentManagementLogic import AppointmentManager

# Initialize logging
logger = logging.getLogger(__name__)
load_dotenv()

app = FastAPI(title="Calendar RAG System", version="1.0.0")

# Configure logging on startup
@app.on_event("startup")
async def startup_event():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger.info("Application startup completed")

# Request/Response models
class ChatRequest(BaseModel):
    text: str
    user_id: str

class ChatResponse(BaseModel):
    text: str
    structured_data: Dict[str, Any]
    status_code: int = 200

# Root endpoint
@app.get("/", tags=["Health Check"])
async def root():
    return {
        "message": "Calendar RAG System Operational",
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc"
        }
    }

# Main chat endpoint
@app.post("/api/chat", response_model=ChatResponse)
async def handle_chat(request: ChatRequest):
    try:
        logger.info(f"Processing request from user {request.user_id}")
        
        # Initialize components
        openai_key = os.getenv("OPENAI_API_KEY")
        if not openai_key:
            raise HTTPException(status_code=500, detail="OpenAI API key not configured")

        gpt_adapter = ChatGPTAdapter(openai_key)
        bq_client = BigQueryClient()
        manager = AppointmentManager(bq_client)

        # Step 1: Parse natural language request
        parsed_data = gpt_adapter.parse_request(request.text,request.user_id)
        
        # Handle parsing errors
        if isinstance(parsed_data, dict) and "error" in parsed_data:
            logger.error(f"Parsing failed: {parsed_data.get('details', 'Unknown error')}")
            return JSONResponse(
                status_code=400,
                content={
                    "text": "Could not understand request",
                    "structured_data": parsed_data,
                    "status_code": 400
                }
            )

        # Step 2: Validate parsed data
        validated_request = parsed_data
        # try:
        #     validated_request = parsed_data
        # except Exception as e:
        #     logger.error(f"Unexpected error: {str(e)}")
        #     return JSONResponse(
        #         status_code=400,
        #         content={
        #             "text": "Invalid request parameters",
        #             "structured_data": {"errors": e.errors()},
        #             "status_code": 400
        #         }
        #     )

        # Step 3: Process appointment
        result = None
        intent = validated_request.intent
        
        if intent == 'create_appointment':
            result = manager.create_appointment(validated_request)
        elif intent == 'cancel_appointment':
            result = manager.cancel_appointment(validated_request)
        elif intent == 'reschedule_appointment':
            result = manager.reschedule_appointment(validated_request)
        elif intent == 'get_availability':
            result = manager.get_availability(validated_request)
        else:
            result = {"error": "Unknown intent"}
            

        # Step 4: Generate response
        return {
            "text": gpt_adapter.generate_response(result),
            "structured_data": result,
            "status_code": 200
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "text": "An unexpected error occurred",
                "structured_data": {"error": str(e)},
                "status_code": 500
            }
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_config=None  # Use default logging config
    )