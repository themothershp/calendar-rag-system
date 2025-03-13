from pydantic import BaseModel, Field, field_validator, model_validator

from typing import Optional
import random
from datetime import datetime




class User(BaseModel):
    user_id: str = Field(..., pattern=r'^USER\d{3}$')
    name: str
    email: str = Field(..., pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$')
    phone: str = Field(
        ..., 
        min_length=10, 
        max_length=20,
        pattern=r'^(\+?\d{1,4}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$'
    )
    timezone: str = 'UTC'

class WorkingHours(BaseModel):
    start: str = Field(pattern=r'^\d{2}:\d{2}$')
    end: str = Field(pattern=r'^\d{2}:\d{2}$')

class Worker(BaseModel):
    worker_id: str = Field(..., pattern=r'^WORKER\d{3}$')
    name: str
    role: str
    working_hours: WorkingHours  # Use nested model
    timezone: str = 'UTC'

    # @field_validator('working_hours')
    # @classmethod
    # def validate_hours(cls, v):
    #     try:
    #         start = datetime.strptime(v['start'], '%H:%M').time()
    #         end = datetime.strptime(v['end'], '%H:%M').time()
    #         if start >= end:
    #             raise ValueError("End time must be after start time")
    #     except (KeyError, ValueError) as e:
    #         raise ValueError("Invalid time format, use HH:MM") from e
    #     return v

class Appointment(BaseModel):
    appointment_id: str = Field(
        default_factory=lambda: f"APT-{datetime.now().timestamp()}-{random.randint(1000,9999)}"
    )
    user_id: str
    worker_id: str
    start_time: datetime
    end_time: datetime
    status: str = Field(default='scheduled', pattern='^(scheduled|cancelled|rescheduled)$')
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('end_time')
    @classmethod
    def validate_duration(cls, v, info):
        if 'start_time' in info.data and v <= info.data['start_time']:
            raise ValueError("End time must be after start time")
        if (v - info.data['start_time']).total_seconds() > 3600 * 4:  # Max 4 hours
            raise ValueError("Appointment too long")
        return v


class ParsedRequest(BaseModel):
    intent: str = Field(pattern="^(create|cancel|reschedule)_appointment$|^get_availability$")
    user_id: str = Field(..., pattern=r"^USER\d{3}$")
    worker_name: Optional[str] = None
    datetime: Optional[datetime] = None
    duration: Optional[int] = Field(None, ge=15, le=240)
    appointment_id: Optional[str] = None

    class Config:
        extra = "ignore"  # Ignore unexpected fields
    
    @model_validator(mode='before')
    def validate_fields_based_on_intent(cls, values):
        intent = values.get('intent')
        
        if intent == 'create_appointment':
            if not values.get('worker_name'):
                raise ValueError("worker_name is required for creating appointments")
            if not values.get('datetime'):
                raise ValueError("datetime is required for creating appointments")
                
        elif intent in ('cancel_appointment'):
            if not values.get('appointment_id') and not values.get('worker_name'):
                raise ValueError("Either appointment_id or worker_name+datetime required")
        elif intent == 'reschedule_appointment':
            # Require EITHER appointment_id OR worker_name+datetime
            if not values.get('appointment_id') and not values.get('worker_name'):
                raise ValueError("Need either appointment_id or worker_name+datetime")
                
        return values
        
    @model_validator(mode='before')
    def check_error(cls, values):
        if "error" in values:
            raise ValueError("Error in parsed data")
        return values

    @field_validator('datetime')
    def validate_future_date(cls, v):
        if v < datetime.now():
            raise ValueError("Date cannot be in the past")
        return v