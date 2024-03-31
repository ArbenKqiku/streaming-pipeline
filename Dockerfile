# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements_docker.txt

# Make the script executable
RUN chmod +x producer_company_house.py

# Run the script with the provided configuration file as an argument
CMD ["./producer_company_house.py", "getting_started.ini"]