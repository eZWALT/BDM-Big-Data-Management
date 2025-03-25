# This specific individual docker image is used to build the dockerized streamlit frontend
FROM python:3.12-slim

# Setup the application directory structure and copy relevant files
WORKDIR /app 
COPY resources resources
COPY src/frontend/requirements.txt . 

# Install dependencies
RUN pip3 install --break-system-packages -r requirements.txt

# Copy streamlit application files and expose port 9999
COPY src/frontend .  
EXPOSE 9999

CMD ["streamlit", "run", "home.py", "--server.port=9999", "--server.address=0.0.0.0"]


# Slightly smaller Alpine image workaround (Not worth the stress for a reduction of 100 MB)
# FROM alpine:3.19.1
# RUN apk update && apk add \
#     curl \
#     python3 \
#     py3-pip \
#     py3-pyarrow \
#     && rm -rf /var/lib/apt/lists/*
# Setup the application directory structure and copy relevant files
# WORKDIR /app 
# COPY resources resources
# COPY src/frontend/requirements.txt . 
# RUN pip3 install --break-system-packages -r requirements.txt
# CMD ["streamlit", "run", "Inicio.py", "--server.port=9999", "--server.address=0.0.0.0"]
