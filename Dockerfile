FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY ./requirements.txt requirements.txt

RUN pip install -r requirements.txt 

COPY ./log_cfg.py log_cfg.py
COPY ./app /app/app