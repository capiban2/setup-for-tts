FROM python:3.13-alpine

COPY main.py requirements.txt /

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "main.py"]
