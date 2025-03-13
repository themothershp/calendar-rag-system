from typing import List, Dict  # Add this import at the top
from CoreDatamodels import User, Worker, WorkingHours, Appointment
from faker import Faker
import random
from datetime import datetime, timedelta
import pytz

class DataGenerator:
    def __init__(self):
        self.fake = Faker()
    
    def generate_users(self, count=50) -> List[User]:
        return [User(
            user_id=f"USER{str(i).zfill(3)}",
            name=self.fake.name(),
            email=self.fake.email(),
            phone=self._generate_valid_phone(),
            timezone=random.choice(pytz.all_timezones)
        ) for i in range(1, count+1)]

    # In sampleDataGeneration.py
    def _generate_valid_phone(self):
        formats = [
            '###-###-####',        # Standard US format
            '(###) ###-####',      # Parenthesis format
            '+## ### ### ####',    # International format
            '1-###-###-####',      # US with country code
            '###.###.####',         # Dot separator
            '##########'            # Plain numbers
        ]
        
        phone = self.fake.numerify(text=random.choice(formats))
        
        # Ensure max length constraint
        if len(phone) > 20:
            phone = phone[:20]
        
        return phone
    
    
    # Updated parameter and return type annotations
    def generate_appointments(self, users: List[User], workers: List[Worker], count=200) -> List[dict]:
        appointments = []
        for _ in range(count):
            user = random.choice(users)
            worker = random.choice(workers)
            
            # Get working hours from Worker model
            start_hour = int(worker.working_hours.start.split(':')[0])
            end_hour = int(worker.working_hours.end.split(':')[0])

            # Generate random appointment time
            start_time = datetime.now().replace(
                hour=random.randint(start_hour, end_hour - 1),
                minute=random.choice([0, 15, 30, 45]),
                second=0,
                microsecond=0
            ) + timedelta(days=random.randint(1, 30))
            
            end_time = start_time + timedelta(minutes=30)

            # Create Appointment instance with all required fields
            appointment = Appointment(
                user_id=user.user_id,
                worker_id=worker.worker_id,
                start_time=start_time,
                end_time=end_time,
                status="scheduled"
            )
            
            appointment_data = appointment.model_dump(mode='json')
            appointments.append(appointment_data)
        return appointments
    
    def generate_workers(self, count=10) -> List[Worker]:
        roles = ['Doctor', 'Consultant', 'Technician']
        return [Worker(
            worker_id=f"WORKER{str(i).zfill(3)}",
            name=self.fake.name(),
            role=random.choice(roles),
            working_hours=WorkingHours(  # Properly initialize nested model
                start=f"{h:02d}:00",
                end=f"{(h+8):02d}:00"  # Fixed formatting
            ),
            timezone=random.choice(pytz.all_timezones)
        ) for i, h in enumerate(random.choices(
            population=range(6, 12),  # Proper syntax for random.choices
            k=count
        ))]

    def _generate_working_hours(self):
        start_hour = random.choice([7, 8, 9, 10])
        return {
            'start': f"{start_hour:02d}:00",
            'end': f"{start_hour+8:02d}:00"  # Ensure 8-hour workday
        }
# dg = DataGenerator()
# for _ in range(10):
#     phone = dg._generate_valid_phone()
#     print(f"{phone} ({len(phone)})")
# import re

# pattern = r'^(\+?\d{1,4}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$'
# test_number = '887-170-2638'

# print(re.match(pattern, test_number))  # Should return a match

