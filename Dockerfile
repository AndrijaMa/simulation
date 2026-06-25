FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py telemetry_dashboard.html ./
ENV TELEMETRY_DIR=/data
EXPOSE 8080
CMD ["python", "server.py"]
