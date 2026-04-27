FROM python:3.12
LABEL authors="den"

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt
RUN pip install --upgrade pip
RUN apt-get update && apt-get install -y locales \
    && sed -i '/ru_RU.UTF-8/s/^# //g' /etc/locale.gen \
    && locale-gen \

ENV LANG=ru_RU.UTF-8
ENV LC_ALL=ru_RU.UTF-8

ENTRYPOINT ["python3", "main.py"]