# Use the official Python image as a base
FROM python:3.12

# Create a group and user
RUN groupadd -r restricted && \
    useradd -r -g restricted restricted \
    && pip install --upgrade pip \
    && apt-get update

# Set the working directory
# WORKDIR /home/restricted/
WORKDIR /var/app

# Change ownership of the working directory
# RUN chown -R restricted:restricted /home/restricted

# # Switch to the new user
# USER restricted

# Set the default command for the container
CMD ["python"]
