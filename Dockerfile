FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p downloads reports/prompts reports/runtime reports/exports data logs

EXPOSE 5000

CMD ["python", "scripts/run_local_web.py", "--host", "0.0.0.0"]
