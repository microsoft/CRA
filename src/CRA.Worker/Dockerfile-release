FROM microsoft/windowsservercore

# deploy the Hello World app
COPY ./bin/Release /root/CRA.Worker/bin/Release

# entrypoint for CRA worker
ENTRYPOINT ["/root/CRA.Worker/bin/Release/CRA.Worker.exe"]
