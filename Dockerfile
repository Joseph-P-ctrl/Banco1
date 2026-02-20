# Set the base image
FROM python:3

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . .

# Expose the port that the Flask application will be listening on
EXPOSE 5000

# Set the environment variables (if needed)
# ENV ENV_VAR_NAME value

# Define the command to start the Flask application
#CMD ["python", "app.py", "--host=0.0.0.0"]
CMD ["gunicorn"  , "--bind", "0.0.0.0:5000", "app:app"]
