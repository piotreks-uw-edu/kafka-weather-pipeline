docker run -it --rm --name kafka \
    -p 127.0.0.1:5000:5000 \
    --network my-network \
    --volume $(pwd -W)/../../:/var/app \
    --env-file $(pwd -W)/enviroment/kafka.env \
    --env-file $(pwd -W)/enviroment/open_weather.env \
    python:3.12-restricted \
    bash -c "pip install -r requirements.txt; exec /bin/bash"