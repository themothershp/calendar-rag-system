# ChatGPTAdapter.py
import json
from openai import OpenAI
from pydantic import ValidationError
from CoreDatamodels import ParsedRequest  # We'll create this next
from datetime import datetime
import logging

# Initialize logger first
logger = logging.getLogger(__name__)


class ChatGPTAdapter:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)
    
    def parse_request(self, natural_language: str,user_id : str) -> dict:
        """Converts natural language to structured data"""
        
        
        prompt = f"""
                User ID: {user_id}
                Current time: {datetime.now().isoformat()}
                Convert this request to JSON:
                {natural_language}

                Important Rules:
                - Use CURRENT YEAR ({datetime.now().year})
                - For future dates without time: assume 9 AM
                - Never suggest past dates
                - Appointment IDs look like: APT-1234567890-WORKER123

                Response Structure:
                {{
                "intent": "create_appointment|cancel_appointment|reschedule_appointment|get_availability",
                "user_id": "USERXXX",
                "worker_name": "Only for create/reschedule/get_availability",
                "datetime": "ISO 8601 (required for create/reschedule)",
                "duration": "Minutes (only for create/reschedule, default 30)",
                "appointment_id": "Required for cancel/reschedule if mentioned"
                }}

                Examples:
                1. Cancel by ID:
                {{"intent": "cancel_appointment", "user_id": "USER048", "appointment_id": "APT-1740812400-WORKER123"}}

                2. Cancel by details:
                {{"intent": "cancel_appointment", "user_id": "USER048", "worker_name": "Tyler", "datetime": "2025-03-04T16:00:00"}}

                3. Create new:
                {{"intent": "create_appointment", "user_id": "USER046", "worker_name": "John", "datetime": "2025-03-22T15:00:00", "duration": 30}}

                4. Reschedule:
                {{"intent": "reschedule_appointment", "appointment_id": "APT-1740812400-WORKER123", "user_id": "USER046", "datetime": "2025-03-23T11:00:00"}}

                Required Fields by Intent:
                - create_appointment: user_id, worker_name, datetime
                - cancel_appointment: user_id + (appointment_id OR worker_name+datetime)
                - reschedule_appointment: user_id + (appointment_id OR worker_name) + datetime
                - get_availability: user_id, worker_name

                Respond ONLY with valid JSON. Never include comments or explanations.
                """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",  # Use gpt-4 if available
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            raw_json = response.choices[0].message.content
            print("ChatGPT Raw Output:", raw_json)
            parsed_data = json.loads(raw_json)
            validated = ParsedRequest(**parsed_data)
            return validated
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {str(e)}")
            return {"error": "Invalid response format", "details": str(e)}
            
        except ValidationError as e:
            logger.error(f"Validation failed: {e.errors()}")
            return {"error": "Validation failed", "details": e.errors()}
            
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {"error": "Processing failed", "details": str(e)}
    
    
    def generate_response(self, structured_data: dict) -> str:
        """Convert structured data into natural language response"""
        try:
            prompt = f"""
                Convert this appointment data into a friendly user message:
                {json.dumps(structured_data, indent=2)}

                Rules:
                - Use simple, conversational language
                - Highlight key details: worker name, date/time, status
                - For conflicts, suggest alternatives clearly
                - Never expose internal IDs or technical terms
                
            """
            
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Response generation failed: {str(e)}")
            return "Your appointment has been processed. Check details below."
            