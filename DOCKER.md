```bash
docker build . -t <name>:latest
docker run -v ./config:/rocket/config -v /var/run/docker.sock:/var/run/docker.sock -v ./logs:/rocket/logs -e HOST_INTERCEPTOR_PATH="/absolute/host/path/to/rocket_interceptor" --net=host <name>:latest
```
