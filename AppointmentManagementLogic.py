from BigQueryIntergration import bigquery
from CoreDatamodels import Appointment,ParsedRequest
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pydantic import BaseModel
from google.cloud import bigquery
import pytz,json

logger = logging.getLogger(__name__)

class AppointmentManager:
    def __init__(self, bq_client):
        self.bq_client = bq_client
        self.default_duration = 30  # minutes

    def create_appointment(self, request: ParsedRequest) -> Dict:
        """Main appointment creation flow"""
        try:
            # Step 1: Validate worker exists
            logger.info(f"Looking up worker: {request.worker_name}")
            worker = self._get_worker_details(request.worker_name)
            logger.debug(f"Worker details: {json.dumps(worker, default=str)}")
            
            if not worker:
                raise ValueError(f"Worker '{request.worker_name}' not found. Valid workers: {self._list_all_worker_names()}")

            # Step 2: Convert to UTC and validate
            start_time = self._convert_to_utc(request.datetime, worker['timezone'])
            end_time = start_time + timedelta(minutes=request.duration or self.default_duration)
            
            if start_time < datetime.now(pytz.utc):
                raise ValueError("Cannot create appointments in the past")

            # Step 3: Check availability
            if not self.check_availability(worker['worker_id'], start_time, end_time):
                alternatives = self.suggest_alternatives(worker['worker_id'], start_time)
                return {
                    "status": "conflict",
                    "message": "Requested time unavailable",
                    "alternatives": alternatives
                }

            # Step 4: Create appointment
            appointment_id = f"APT-{int(start_time.timestamp())}-{worker['worker_id']}"
            appointment_data = {
                "appointment_id": appointment_id,
                "user_id": request.user_id,
                "worker_id": worker['worker_id'],
                "start_time": start_time.replace(tzinfo=None).isoformat(),  # Remove timezone info
                "end_time": end_time.replace(tzinfo=None).isoformat(),      # For DATETIME compatibility
                "status": "scheduled",
                "created_at": datetime.now(pytz.utc).replace(tzinfo=None).isoformat()
            }

            # Insert into BigQuery
            # errors = self.bq_client.insert_rows_json(
            #     'calendar_system.appointments',
            #     [appointment_data]
            # )
            errors = self.bq_client.insert_data('appointments', [appointment_data])
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                raise RuntimeError("Failed to create appointment")

            return appointment_data

        except Exception as e:
            logger.error(f"Appointment creation failed: {str(e)}")
            raise

    # AppointmentManagementLogic.py (in check_availability)
    def check_availability(self, worker_id: str, start: datetime, end: datetime, exclude_id: str = None) -> bool:
        """Check availability while optionally excluding an appointment"""
        query = """
            SELECT COUNT(*) AS conflicts
            FROM `calendar_system.appointments`
            WHERE worker_id = @worker_id
            AND status NOT IN ('cancelled', 'rescheduled')
            AND (
                (start_time BETWEEN @start AND @end) OR
                (end_time BETWEEN @start AND @end) OR
                (start_time <= @start AND end_time >= @end)
            )
            {}
        """.format("AND appointment_id != @exclude_id" if exclude_id else "")
        
        params = [
            bigquery.ScalarQueryParameter("worker_id", "STRING", worker_id),
            bigquery.ScalarQueryParameter("start", "DATETIME", start.replace(tzinfo=None)),
            bigquery.ScalarQueryParameter("end", "DATETIME", end.replace(tzinfo=None))
        ]
        
        if exclude_id:
            params.append(
                bigquery.ScalarQueryParameter("exclude_id", "STRING", exclude_id)
            )
        
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        
        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            result = next(query_job.result())
            return result.conflicts == 0
        except Exception as e:
            logger.error(f"Availability check failed: {str(e)}")
            raise

    def suggest_alternatives(self, worker_id: str, original_time: datetime, max_slots=3) -> List[str]:
        """Find next available time slots"""
        worker = self._get_worker_by_id(worker_id)
        if not worker:
            return []

        alternatives = []
        current_time = original_time
        interval = timedelta(minutes=30)
        max_attempts = 10  # Prevent infinite loops

        while len(alternatives) < max_slots and max_attempts > 0:
            current_time += interval
            end_time = current_time + timedelta(minutes=self.default_duration)
            
            if self._is_within_working_hours(current_time, worker):
                if self.check_availability(worker_id, current_time, end_time):
                    alternatives.append(current_time.astimezone(
                        pytz.timezone(worker['timezone'])
                    ).isoformat())
                    
            max_attempts -= 1

        return alternatives
    
    def reschedule_appointment(self, request: ParsedRequest) -> Dict:
        """Reschedule an existing appointment"""
        try:
            if not request.appointment_id:
                raise ValueError("Appointment ID required for rescheduling")

            # Get existing appointment
            existing = self._get_appointment(request.appointment_id, request.user_id)
            if not existing:
                raise ValueError("Appointment not found or access denied")

            # Validate new time
            worker = self._get_worker_by_id(existing['worker_id'])
            new_start = self._convert_to_utc(request.datetime, worker['timezone'])
            new_end = new_start + timedelta(minutes=request.duration)

            # Check availability (excluding current appointment)
            if not self.check_availability(
                worker['worker_id'], new_start, new_end, exclude_id=request.appointment_id
            ):
                alternatives = self.suggest_alternatives(worker['worker_id'], new_start)
                return {
                    "status": "conflict",
                    "message": "New time unavailable",
                    "alternatives": alternatives
                }

            # Update appointment
            update_query = f"""
                UPDATE `calendar_system.appointments`
                SET start_time = '{new_start.isoformat()}',
                    end_time = '{new_end.isoformat()}',
                    status = 'rescheduled'
                WHERE appointment_id = '{request.appointment_id}'
                AND user_id = '{request.user_id}'
            """
            
            query_job = self.bq_client.query(update_query)
            query_job.result()
            
            return {
                "status": "success",
                "appointment_id": request.appointment_id,
                "new_time": new_start.isoformat()
            }

        except Exception as e:
            logger.error(f"Rescheduling failed: {str(e)}")
            raise
            

    def cancel_appointment(self, request: ParsedRequest) -> Dict:
        """Cancel an appointment by ID or worker/time details"""
        try:
            # Try to find appointment by ID first
            print("@@@@ Request",request)
            if request.appointment_id:
                existing = self._get_appointment(request.appointment_id, request.user_id)
            else:
                # Fall back to worker/time search
                existing = self._find_appointment_by_details(
                    user_id=request.user_id,
                    worker_name=request.worker_name,
                    dt=request.datetime
                )

            if not existing:
                raise ValueError("Appointment not found or access denied")

            # Parameterized query for security
            cancel_query = """
                UPDATE `calendar_system.appointments`
                SET status = 'cancelled'
                WHERE appointment_id = @appointment_id
                AND user_id = @user_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("appointment_id", "STRING", existing['appointment_id']),
                    bigquery.ScalarQueryParameter("user_id", "STRING", request.user_id)
                ]
            )

            query_job = self.bq_client.query(cancel_query, job_config=job_config)
            query_job.result()  # Wait for completion

            return {
                "status": "success",
                "appointment_id": existing['appointment_id'],
                "cancelled_at": datetime.now(pytz.utc).isoformat()
            }

        except Exception as e:
            logger.error(f"Appointment cancellation failed: {str(e)}")
            raise

    def _find_appointment_by_details(self, user_id: str, worker_name: str, dt: datetime) -> Optional[Dict]:
        """Find appointment by user, worker, and LOCAL time"""
        try:
            # Get worker's timezone first
            worker = self._get_worker_details(worker_name)
            if not worker:
                return None
                
            # Convert LOCAL time to UTC
            utc_time = self._convert_to_utc(dt, worker['timezone'])
            
            # Search window: ±2 hours in UTC
            start_window = utc_time - timedelta(hours=2)
            end_window = utc_time + timedelta(hours=2)
            
            query = """
                SELECT a.* 
                FROM `calendar_system.appointments` a
                JOIN `calendar_system.workers` w ON a.worker_id = w.worker_id
                WHERE a.user_id = @user_id
                AND LOWER(w.name) = LOWER(@worker_name)
                AND a.start_time BETWEEN @start_time AND @end_time
                AND a.status != 'cancelled'
                LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                    bigquery.ScalarQueryParameter("worker_name", "STRING", worker_name.strip()),
                    bigquery.ScalarQueryParameter("start_time", "DATETIME", start_window),
                    bigquery.ScalarQueryParameter("end_time", "DATETIME", end_window)
                ]
            )
            
            result = next(self.bq_client.query(query, job_config=job_config).result(), None)
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Appointment lookup failed: {str(e)}")
            return None

    # Example: get_user_appointments()
    def get_user_appointments(self, user_id: str) -> List[Dict]:
        try:
            query = f"""
                SELECT * 
                FROM `calendar_system.appointments`
                WHERE user_id = '{user_id}'
                AND status != 'cancelled'
                ORDER BY start_time DESC
            """
            query_job = self.bq_client.query(query)  # Get job object
            results = query_job.result()             # Process results here
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Failed to fetch appointments: {str(e)}")
            return []

    def _get_appointment(self, appointment_id: str, user_id: str) -> Optional[Dict]:
        """Internal method to retrieve an appointment"""
        try:
            query = f"""
                SELECT * 
                FROM `calendar_system.appointments`
                WHERE appointment_id = '{appointment_id}'
                AND user_id = '{user_id}'
                LIMIT 1
            """
            result = next(self.bq_client.query(query).result(), None)
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Appointment lookup failed: {str(e)}")
            return None

    def _get_worker_by_id(self, worker_id: str) -> Optional[Dict]:
        """Get worker details by ID"""
        try:
            query = f"""
                SELECT * 
                FROM `calendar_system.workers`
                WHERE worker_id = '{worker_id}'
                LIMIT 1
            """
            result = next(self.bq_client.query(query).result(), None)
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Worker lookup failed: {str(e)}")
            return None

    def _get_worker_details(self, worker_name: str) -> Optional[Dict]:
        """Get worker details from BigQuery"""
        worker_name = worker_name.strip()
        
        query = """
            SELECT worker_id, name, working_hours, timezone
            FROM `calendar_system.workers`
            WHERE LOWER(name) = LOWER(@worker_name)
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("worker_name", "STRING", worker_name)
            ]
        )
        
        try:
            # Execute query and fetch results
            query_job = self.bq_client.query(query, job_config=job_config)
            result = next(query_job.result(), None)
            
            logger.info(f"Worker lookup: {worker_name} → Found: {bool(result)}")
            return dict(result) if result else None  # Convert Row to dict
            
        except Exception as e:
            logger.error(f"Worker lookup failed: {str(e)}")
            return None

    def _convert_to_utc(self, naive_time: datetime, source_tz: str) -> datetime:
        """Convert naive datetime to UTC"""
        try:
            tz = pytz.timezone(source_tz)
            localized = tz.localize(naive_time)
            return localized.astimezone(pytz.utc)
        except Exception as e:
            logger.error(f"Time conversion failed: {str(e)}")
            raise ValueError("Invalid datetime format") from e

    def _is_within_working_hours(self, utc_time: datetime, worker: Dict) -> bool:
        """Check if UTC time falls within worker's local working hours"""
        try:
            tz = pytz.timezone(worker['timezone'])
            local_time = utc_time.astimezone(tz)
            
            start_hour, start_minute = map(int, worker['working_hours']['start'].split(':'))
            end_hour, end_minute = map(int, worker['working_hours']['end'].split(':'))
            
            start = local_time.replace(hour=start_hour, minute=start_minute, second=0)
            end = local_time.replace(hour=end_hour, minute=end_minute, second=0)
            
            return start <= local_time <= end
        except Exception as e:
            logger.error(f"Working hours check failed: {str(e)}")
            return False
    def _list_all_worker_names(self) -> List[str]:
        """Debug method to list all workers"""
        query = "SELECT name FROM `calendar_system.workers`"
        results = self.bq_client.query(query)
        return [row['name'] for row in results]

