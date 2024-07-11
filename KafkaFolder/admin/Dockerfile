# trunk-ignore-all(checkov/CKV_DOCKER_3)
FROM python:3.11-alpine

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "script.py"]