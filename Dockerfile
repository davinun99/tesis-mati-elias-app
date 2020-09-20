# base image to be used
FROM python:3.8
# The environment variable ensures that the python output is set 
#straight to the terminal with out buffering it first
ENV PYTHONUNBUFFERED 1

# Set the working directory
WORKDIR /code

# add the requirements file to the working dir
COPY requirements.txt /code/

#install the requirements (install before adding rest of code to #avoid rerunning this at every code change-built in layers)
RUN pip3 install -r requirements.txt

# Copy the current directory contents into the container at /code/
COPY . /code/

#port from the container to expose to host
EXPOSE 80

#Tell image what to do when it starts as a container
ENTRYPOINT ["sh", "/code/start.sh"]
