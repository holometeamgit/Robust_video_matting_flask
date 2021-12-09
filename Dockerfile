# For more information, please refer to https://aka.ms/vscode-docker-python
FROM ubuntu:18.04

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:jonathonf/ffmpeg-4
RUN apt-get update && apt-get upgrade -y

RUN apt-get install -y \
bzip2 \
libx11-6 \
python \
python-dev \
python3-pip \
libjpeg8-dev \
zlib1g-dev \
ffmpeg \
pkg-config \
libavformat-dev \
libavcodec-dev \
libavdevice-dev \
libavutil-dev \
libswscale-dev \
libswresample-dev \
libavfilter-dev \
software-properties-common

EXPOSE 5000

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

RUN pip3 install --upgrade setuptools

# Install pip requirements
COPY requirements_inference.txt .
RUN pip3 install -r requirements_inference.txt

WORKDIR /app
COPY . /app

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "flask_app:app"]
