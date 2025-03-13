from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from BigQueryIntergration import BigQueryClient
from AppointmentManagementLogic import AppointmentManager
from ChatGPTIntegration import ChatGPTAdapter
import os

app = FastAPI()

class ChatRequest(BaseModel):
    text: str
    user_id: str

@app.post("/api/chat")
async def handle_chat(request: ChatRequest):
    try:
        # Step 1: Parse natural language
        gpt_adapter = ChatGPTAdapter(os.getenv("OPENAI_API_KEY"))
        parsed_request = gpt_adapter.parse_request(request.text)
        
        # Step 2: Process request
        bq_client = BigQueryClient()
        manager = AppointmentManager(bq_client)
        
        match parsed_request.get('intent'):
            case 'create_appointment':
                result = manager.create_appointment(parsed_request)
            case 'cancel_appointment':
                result = manager.cancel_appointment(parsed_request)
            # Add other cases
            
        # Step 3: Generate natural language response
        return {
            "text": gpt_adapter.generate_response(result),
            "structured_data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))