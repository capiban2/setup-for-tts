FROM python:3.13-alpine

COPY requirements.txt main.py /

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python","-u", "main.py"]
