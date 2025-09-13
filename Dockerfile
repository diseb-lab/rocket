FROM python:3.12-alpine

# Set environment variables
ENV LANGUAGE=C.UTF-8
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /rocket

# Install build tools and dependencies
RUN apk update && apk add --no-cache \
    openssl \
    py3-pip \
    && python3 -m ensurepip \
    && pip3 install --upgrade pip setuptools wheel

# Copy and install Python dependencies
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .
CMD ["python3", "-m", "rocket_controller", "ByzzQLStrategy"]
