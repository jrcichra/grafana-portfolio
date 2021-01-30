FROM python:3-alpine3.12
WORKDIR /app
RUN apk add g++
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY main.py ./ 
CMD python -u /app/main.py