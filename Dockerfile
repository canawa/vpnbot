FROM python:3.12
LABEL authors="den"

ENTRYPOINT ["top", "-b"]

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN pip install --upgrade pip
ENTRYPOINT ["python3", "main.py"]