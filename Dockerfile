FROM python:alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

ENV TRADIER_AUTH_TOKEN=${TRADIER_AUTH_TOKEN}

CMD [ "python", "-u", "./option_control.py" ]