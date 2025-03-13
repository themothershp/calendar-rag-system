FROM python:3.12.5-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .

ENV OPENAI_API_KEY="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"


EXPOSE 8000

CMD ["python", "api.py"]
