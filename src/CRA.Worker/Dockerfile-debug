FROM microsoft/windowsservercore

# deploy the Hello World app
COPY ./bin/Debug /root/CRA.Worker/bin/Debug

# entrypoint for CRA worker
ENTRYPOINT ["/root/CRA.Worker/bin/Debug/CRA.Worker.exe"]
