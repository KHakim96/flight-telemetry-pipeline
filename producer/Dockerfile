FROM python:3.10-slim
WORKDIR /app
RUN pip install --no-cache-dir requests kafka-python
COPY main.py .
CMD ["python", "-u", "main.py"]
